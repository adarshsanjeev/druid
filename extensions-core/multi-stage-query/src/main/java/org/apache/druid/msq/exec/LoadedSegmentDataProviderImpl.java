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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataServerClient;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class LoadedSegmentDataProviderImpl implements LoadedSegmentDataProvider
{
  private final static int DEFAULT_NUM_TRIES = 1;
  private final RichSegmentDescriptor segmentDescriptor;
  private final String dataSource;
  private final ChannelCounters channelCounters;
  private final ServiceClientFactory serviceClientFactory;
  private final CoordinatorClient coordinatorClient;
  private final ObjectMapper objectMapper;
  private final ObjectMapper smileMapper;

  public LoadedSegmentDataProviderImpl(
      RichSegmentDescriptor segmentDescriptor,
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      CoordinatorClient coordinatorClient,
      ObjectMapper objectMapper,
      ObjectMapper smileMapper
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.coordinatorClient = coordinatorClient;
    this.objectMapper = objectMapper;
    this.smileMapper = smileMapper;
  }

  @Override
  public <ReturnType, QueryType> Sequence<ReturnType> fetchRowsFromDataServer(
      Query<QueryType> query,
      Function<Sequence<QueryType>, Sequence<ReturnType>> mappingFunction,
      Closer closer
  ) throws IOException
  {
    final Query<QueryType> preparedQuery = Queries.withSpecificSegments(
        query.withDataSource(new TableDataSource(dataSource)),
        ImmutableList.of(segmentDescriptor)
    );

    final DataServerClient<QueryType> dataServerClient = new DataServerClient<>(
        serviceClientFactory,
        new FixedSetServiceLocator(segmentDescriptor.getServers()),
        objectMapper,
        smileMapper
    );

    final int numRetriesOnMissingSegments = preparedQuery.context().getNumRetriesOnMissingSegments(DEFAULT_NUM_TRIES);
    final ResponseContext responseContext = new DefaultResponseContext();

    Sequence<QueryType> queryReturnSequence;
    try {
      queryReturnSequence = RetryUtils.retry(
          () -> {
            Sequence<QueryType> sequence = dataServerClient.run(preparedQuery, responseContext, closer);
            final List<SegmentDescriptor> missingSegments = getMissingSegments(responseContext);
            if (missingSegments.isEmpty()) {
              return sequence;
            } else {
              Boolean wasHandedOff = checkSegmentHandoff(coordinatorClient, dataSource, segmentDescriptor);
              if (Boolean.TRUE.equals(wasHandedOff)) {
                throw new HandoffException();
              } else {
                throw new ISE(
                    "Segment[%s] could not be found on data server, but segment was not handed off.",
                    segmentDescriptor
                );
              }
            }
          },
          input -> !(input instanceof HandoffException),
          numRetriesOnMissingSegments
      );
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e, HandoffException.class);
      throw new IOE(e, "Exception while fetching rows from dataservers.");
    }

    return mappingFunction.apply(queryReturnSequence).map(row -> {
      channelCounters.incrementRowCount();
      return row;
    });
  }

  private static List<SegmentDescriptor> getMissingSegments(final ResponseContext responseContext)
  {
    List<SegmentDescriptor> missingSegments = responseContext.getMissingSegments();
    if (missingSegments == null) {
      return ImmutableList.of();
    }
    return missingSegments;
  }

  private static boolean checkSegmentHandoff(
      CoordinatorClient coordinatorClient,
      String dataSource,
      SegmentDescriptor segmentDescriptor
  ) throws Exception {
    Boolean wasHandedOff = RetryUtils.retry(
        () -> FutureUtils.get(coordinatorClient.isHandoffComplete(dataSource, segmentDescriptor), true),
        input -> true,
        RetryUtils.DEFAULT_MAX_TRIES
    );

    return Boolean.TRUE.equals(wasHandedOff);
  }
}
