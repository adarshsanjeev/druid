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

package org.apache.druid.client;

import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class DynamicConfigurationManager
{
  private static final Logger log = new Logger(DynamicConfigurationManager.class);
  private final CoordinatorClient coordinatorClient;

  @Inject
  public DynamicConfigurationManager(CoordinatorClient coordinatorClient)
  {
    this.coordinatorClient = coordinatorClient;
  }

  private final AtomicReference<CoordinatorDynamicConfig> config = new AtomicReference<>();

  public CoordinatorDynamicConfig getConfig()
  {
    return config.get();
  }

  public Set<String> getTargetCloneServers()
  {
    CoordinatorDynamicConfig coordinatorDynamicConfig = config.get();
    return coordinatorDynamicConfig.getCloneServers().keySet();
  }

  public Set<String> getSourceClusterServers()
  {
    CoordinatorDynamicConfig coordinatorDynamicConfig = config.get();
    return new HashSet<>(coordinatorDynamicConfig.getCloneServers().values());
  }

  public void updateCloneServers(CoordinatorDynamicConfig updatedConfig)
  {
    config.set(updatedConfig);
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    log.info("Initializing dynamic configuration.");

    try {
      // TODO: handle exceptions and timeouts
      CoordinatorDynamicConfig coordinatorDynamicConfig = coordinatorClient.getCoordinatorConfig().get();
      updateCloneServers(coordinatorDynamicConfig);
      log.info("Synced clone servers TRUE [%s]", coordinatorDynamicConfig.getCloneServers());
    }
    catch (Exception e) {
      log.error(e, "Exception");
      throw new RuntimeException(e);
    }
  }
}
