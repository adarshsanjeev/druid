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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles cloning of historicals. Given the historical to historical clone mappings, based on
 * {@link CoordinatorDynamicConfig#getCloneServers()}, copies any segments load or unload requests from the source
 * historical to the target historical.
 */
public class CloneHistoricals implements CoordinatorDuty
{
  private static final Logger log = new Logger(CloneHistoricals.class);

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, String> cloneServers = params.getCoordinatorDynamicConfig().getCloneServers();
    final CoordinatorRunStats stats = params.getCoordinatorStats();
    final SegmentLoadQueueManager loadQueueManager = params.getLoadQueueManager();

    if (cloneServers.isEmpty()) {
      // No servers to be cloned.
      return params;
    }

    // Create a map of host to historical.
    final Map<String, ServerHolder> historicalMap = params.getDruidCluster()
                                                          .getHistoricals()
                                                          .values()
                                                          .stream()
                                                          .flatMap(Collection::stream)
                                                          .collect(Collectors.toMap(
                                                              serverHolder -> serverHolder.getServer().getHost(),
                                                              serverHolder -> serverHolder
                                                          ));

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      log.debug("Handling cloning for mapping: [%s]", entry);

      final String sourceHistoricalName = entry.getKey();
      final ServerHolder sourceServer = historicalMap.get(sourceHistoricalName);

      if (sourceServer == null) {
        log.warn(
            "Could not find source historical [%s]. Skipping over clone mapping [%s].",
            sourceHistoricalName,
            entry
        );
        continue;
      }

      final String targetHistoricalName = entry.getValue();
      final ServerHolder targetServer = historicalMap.get(targetHistoricalName);

      if (targetServer == null) {
        log.warn(
            "Could not find target historical [%s]. Skipping over clone mapping [%s].",
            targetHistoricalName,
            entry
        );
        continue;
      }

      // Load any segments missing in the clone target.
      for (DataSegment segment : sourceServer.getProjectedSegments()) {
        if (!targetServer.getProjectedSegments().contains(segment)) {
          if (loadQueueManager.loadSegment(segment, targetServer, SegmentAction.LOAD)) {
            stats.add(
                Stats.Segments.CLONE_LOAD,
                RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
                1L
            );
          }
        }
      }

      // Drop any segments missing from the clone source.
      for (DataSegment segment : targetServer.getProjectedSegments()) {
        if (!sourceServer.getProjectedSegments().contains(segment)) {
          if (loadQueueManager.dropSegment(segment, targetServer)) {
            stats.add(
                Stats.Segments.CLONE_DROP,
                RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
                1L
            );
          }
        }
      }
    }

    return params;
  }
}
