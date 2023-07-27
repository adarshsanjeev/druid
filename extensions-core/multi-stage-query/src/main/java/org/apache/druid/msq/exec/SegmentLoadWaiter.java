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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Class that periodically checks with the broker if all the segments generated are loaded by querying the sys table
 * and blocks till it is complete. This will account for and not wait for segments that would never be loaded due to
 * load rules. Should only be called if the query generates new segments or tombstones.
 * <br>
 * If an exception is thrown during operation, this will simply log the exception and exit without failing the task,
 * since the segments have already been published successfully, and should be loaded eventually.
 * <br>
 * If the segments are not loaded within {@link #TIMEOUT_DURATION_MILLIS} milliseconds, this logs a warning and exits
 * for the same reason.
 */
public class SegmentLoadWaiter
{
  private static final Logger log = new Logger(SegmentLoadWaiter.class);
  private static final long INITIAL_SLEEP_DURATION_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long SLEEP_DURATION_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long TIMEOUT_DURATION_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final String LOAD_QUERY = "SELECT COUNT(*) AS totalSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_available = 0 AND is_published = 1 AND replication_factor != 0) AS loadingSegments\n"
                                           + "FROM sys.segments\n"
                                           + "WHERE datasource = '%s' AND is_overshadowed = 0 AND version = '%s'";

  private final BrokerClient brokerClient;
  private final ObjectMapper objectMapper;
  // Map of version vs latest load status.
  private final Map<String, VersionLoadStatus> versionToLoadStatusMap;
  private final String datasource;
  private final Set<String> versionsToAwait;
  private final boolean doWait;
  private volatile SegmentLoadWaiterStatus status;

  public SegmentLoadWaiter(ControllerContext context, String datasource, Set<String> versionsToAwait, int initialSegmentCount)
  {
    this.brokerClient = context.injector().getInstance(BrokerClient.class);
    this.objectMapper = context.jsonMapper();
    this.datasource = datasource;
    this.versionsToAwait = new TreeSet<>(versionsToAwait);
    this.versionToLoadStatusMap = new HashMap<>();
    this.status = new SegmentLoadWaiterStatus(State.INIT, null, 0, initialSegmentCount, initialSegmentCount);
    this.doWait = true;
  }

  @VisibleForTesting
  SegmentLoadWaiter(BrokerClient brokerClient, ObjectMapper objectMapper, String datasource, Set<String> versionsToAwait, int initialSegmentCount, boolean doWait)
  {
    this.brokerClient = brokerClient;
    this.objectMapper = objectMapper;
    this.datasource = datasource;
    this.versionsToAwait = new TreeSet<>(versionsToAwait);
    this.versionToLoadStatusMap = new HashMap<>();
    this.status = new SegmentLoadWaiterStatus(State.INIT, null, 0, initialSegmentCount, initialSegmentCount);
    this.doWait = doWait;
  }

  /**
   * Uses broker client to check if all segments created by the ingestion have been loaded and updates the {@link #status)}
   * periodically.
   * <br>
   * If an exception is thrown during operation, this will log the exception and return without failing the task,
   * since the segments have already been published successfully, and should be loaded eventually.
   * <br>
   * Only expected to be called from the main controller thread.
   */
  public void waitForSegmentsToLoad()
  {
    DateTime startTime = DateTimes.nowUtc();

    try {
      // Sleep for a short duration to allow the segments that were just created to reflect in broker queries.
      // This avoids a race condition where the broker returns an empty response as it is not yet aware of the new segments
      // yet. This cannot be differentiated from the case where the segments have been dropped due to load rules already.
      waitIfNeeded(INITIAL_SLEEP_DURATION_MILLIS);

      while (!versionsToAwait.isEmpty()) {
        // Check the timeout and exit if exceeded.
        long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
        if (runningMillis > TIMEOUT_DURATION_MILLIS) {
          log.warn("Runtime [%s] exceeded timeout [%s] while waiting for segments to load. Exiting.", runningMillis, TIMEOUT_DURATION_MILLIS);
          updateStatus(State.TIMED_OUT, startTime);
          return;
        }

        Iterator<String> iterator = versionsToAwait.iterator();

        // Query the broker for all pending versions
        while (iterator.hasNext()) {
          String version = iterator.next();

          // Fetch the load status for this version from the broker
          VersionLoadStatus loadStatus = fetchLoadStatusForVersion(version);
          versionToLoadStatusMap.put(version, loadStatus);

          // If loading is done for this stage, remove it from future loops.
          if (loadStatus.isLoadingComplete()) {
            iterator.remove();
          }
        }

        if (!versionsToAwait.isEmpty()) {
          // Update the status.
          updateStatus(State.WAITING, startTime);

          // Sleep for a while before retrying.
          waitIfNeeded(SLEEP_DURATION_MILLIS);
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Exception occurred while waiting for segments to load. Exiting.");

      // Update the status and return.
      updateStatus(State.FAILED, startTime);
      return;
    }
    // Update the status.
    updateStatus(State.SUCCESS, startTime);
  }

  private void waitIfNeeded(long waitTimeMillis) throws Exception
  {
    if (doWait) {
      Thread.sleep(waitTimeMillis);
    }
  }

  /**
   * Updates the {@link #status} with the latest details based on {@link #versionToLoadStatusMap}
   */
  private void updateStatus(State state, DateTime startTime)
  {
    int totalSegmentCount = 0, pendingSegmentCount = 0;
    for (Map.Entry<String, VersionLoadStatus> entry : versionToLoadStatusMap.entrySet()) {
      totalSegmentCount += entry.getValue().getTotalSegments();
      pendingSegmentCount += entry.getValue().getLoadingSegments();
    }

    long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
    status = new SegmentLoadWaiterStatus(state, startTime, runningMillis, totalSegmentCount, pendingSegmentCount);
  }

  /**
   * Uses {@link #brokerClient} to fetch latest load status for a given version. Converts the response into a
   * {@link VersionLoadStatus} and returns it.
   */
  private VersionLoadStatus fetchLoadStatusForVersion(String version) throws Exception
  {
    Request request = brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/");
    SqlQuery sqlQuery = new SqlQuery(StringUtils.format(LOAD_QUERY, datasource, version),
                                     ResultFormat.OBJECTLINES,
                                     false, false, false, null, null);
    request.setContent("application/json", objectMapper.writeValueAsBytes(sqlQuery));

    String response = brokerClient.sendQuery(request);

    if (response.trim().isEmpty()) {
      // If no segments are returned for a version, all segments have been dropped by a drop rule.
      return new VersionLoadStatus(0, 0);
    } else {
      return objectMapper.readValue(response, VersionLoadStatus.class);
    }
  }

  /**
   * Returns the current status of the load.
   */
  public SegmentLoadWaiterStatus status()
  {
    return status;
  }

  public static class SegmentLoadWaiterStatus
  {
    private final State state;
    private final DateTime startTime;
    private final long duration;
    private final int totalSegments;
    private final int segmentsLeft;

    @JsonCreator
    public SegmentLoadWaiterStatus(
        @JsonProperty("state") SegmentLoadWaiter.State state,
        @JsonProperty("startTime") @Nullable DateTime startTime,
        @JsonProperty("duration") long duration,
        @JsonProperty("totalSegments") int totalSegments,
        @JsonProperty("segmentsLeft") int segmentsLeft
    )
    {
      this.state = state;
      this.startTime = startTime;
      this.duration = duration;
      this.totalSegments = totalSegments;
      this.segmentsLeft = segmentsLeft;
    }

    @JsonProperty
    public SegmentLoadWaiter.State getState()
    {
      return state;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    public long getDuration()
    {
      return duration;
    }

    @JsonProperty
    public long getTotalSegments()
    {
      return totalSegments;
    }

    @JsonProperty
    public int getSegmentsLeft()
    {
      return segmentsLeft;
    }
  }

  public enum State
  {
    INIT,
    WAITING,
    SUCCESS,
    FAILED,
    TIMED_OUT
  }

  public static class VersionLoadStatus
  {
    private final int totalSegments;
    private final int loadingSegments;

    @JsonCreator
    public VersionLoadStatus(
        @JsonProperty("totalSegments") int totalSegments,
        @JsonProperty("loadingSegments") int loadingSegments
    )
    {
      this.totalSegments = totalSegments;
      this.loadingSegments = loadingSegments;
    }

    @JsonProperty("totalSegments")
    public int getTotalSegments()
    {
      return totalSegments;
    }

    @JsonProperty("loadingSegments")
    public int getLoadingSegments()
    {
      return loadingSegments;
    }

    @JsonIgnore
    public boolean isLoadingComplete()
    {
      return loadingSegments == 0;
    }
  }
}
