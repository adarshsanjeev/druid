/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.schema;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Builds a view of used segment metadata currently present on the Coordinator
 * in the {@link SegmentsMetadataManager} object, by polling the REST API
 * {@code /druid/coordinator/v1/metadata/segments}.
 */
@ManageLifecycle
public class MetadataSegmentView
{
  private static final EmittingLogger log = new EmittingLogger(MetadataSegmentView.class);

  private final CoordinatorClient coordinatorClient;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;

  private final boolean isCacheEnabled;
  /**
   * Use {@link ImmutableSortedSet} so that the order of segments is deterministic and
   * sys.segments queries return the segments in sorted order based on segmentId.
   *
   * Volatile since this reference is reassigned in {@code poll()} and then read in {@code getPublishedSegments()}
   * from other threads.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedSet<SegmentStatusInCluster> publishedSegments = null;
  /**
   * Caches the replication factor for segment IDs. In case of coordinator restarts or leadership re-elections, the coordinator API returns `null` replication factor until load rules are evaluated.
   * The cache can be used during these periods to continue serving the previously fetched values.
   */
  private final Cache<SegmentId, Integer> segmentIdToReplicationFactor;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodInMS;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CountDownLatch cachePopulated = new CountDownLatch(1);

  @Inject
  public MetadataSegmentView(
      final CoordinatorClient coordinatorClient,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final BrokerSegmentMetadataCacheConfig config
  )
  {
    Preconditions.checkNotNull(config, "BrokerSegmentMetadataCacheConfig");
    this.coordinatorClient = coordinatorClient;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.isCacheEnabled = config.isMetadataSegmentCacheEnable();
    this.pollPeriodInMS = config.getMetadataSegmentPollPeriod();
    this.scheduledExec = Execs.scheduledSingleThreaded("MetadataSegmentView-Cache--%d");
    this.segmentIdToReplicationFactor = CacheBuilder.newBuilder()
                                                    .expireAfterAccess(10, TimeUnit.MINUTES)
                                                    .build();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      if (isCacheEnabled) {
        scheduledExec.schedule(new PollTask(), pollPeriodInMS, TimeUnit.MILLISECONDS);
      }
      lifecycleLock.started();
      log.info("MetadataSegmentView is started.");
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }
    log.info("MetadataSegmentView is stopping.");
    if (isCacheEnabled) {
      scheduledExec.shutdown();
    }
    log.info("MetadataSegmentView is stopped.");
  }

  private void poll()
  {
    log.info("Polling segments from coordinator");
    final CloseableIterator<SegmentStatusInCluster> metadataSegments = getMetadataSegments(
        coordinatorClient,
        segmentWatcherConfig.getWatchedDataSources()
    );

    final ImmutableSortedSet.Builder<SegmentStatusInCluster> builder = ImmutableSortedSet.naturalOrder();
    while (metadataSegments.hasNext()) {
      final SegmentStatusInCluster segment = metadataSegments.next();
      final DataSegment interned = DataSegmentInterner.intern(segment.getDataSegment());
      Integer replicationFactor = segment.getReplicationFactor();
      if (replicationFactor == null) {
        replicationFactor = segmentIdToReplicationFactor.getIfPresent(segment.getDataSegment().getId());
      } else {
        segmentIdToReplicationFactor.put(segment.getDataSegment().getId(), segment.getReplicationFactor());
      }
      final SegmentStatusInCluster segmentStatusInCluster = new SegmentStatusInCluster(
          interned,
          segment.isOvershadowed(),
          replicationFactor,
          segment.getNumRows(),
          segment.isRealtime()
      );
      builder.add(segmentStatusInCluster);
    }
    publishedSegments = builder.build();
    cachePopulated.countDown();
  }

  Iterator<SegmentStatusInCluster> getSegments()
  {
    if (isCacheEnabled) {
      Uninterruptibles.awaitUninterruptibly(cachePopulated);
      return publishedSegments.iterator();
    } else {
      return getMetadataSegments(
          coordinatorClient,
          segmentWatcherConfig.getWatchedDataSources()
      );
    }
  }

  // Note that coordinator must be up to get segments
  private CloseableIterator<SegmentStatusInCluster> getMetadataSegments(
      CoordinatorClient coordinatorClient,
      Set<String> watchedDataSources
  )
  {
    // includeRealtimeSegments flag would additionally request realtime segments
    // note that realtime segments are returned only when druid.centralizedDatasourceSchema.enabled is set on the Coordinator
    return FutureUtils.getUnchecked(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(watchedDataSources, true),
        true
    );
  }

  private class PollTask implements Runnable
  {
    @Override
    public void run()
    {
      long delayMS = pollPeriodInMS;
      try {
        final long pollStartTime = System.nanoTime();
        poll();
        final long pollEndTime = System.nanoTime();
        final long pollTimeNS = pollEndTime - pollStartTime;
        final long pollTimeMS = TimeUnit.NANOSECONDS.toMillis(pollTimeNS);
        delayMS = Math.max(pollPeriodInMS - pollTimeMS, 0);
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling Coordinator.").emit();
      }
      finally {
        if (!Thread.currentThread().isInterrupted()) {
          scheduledExec.schedule(new PollTask(), delayMS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

}
