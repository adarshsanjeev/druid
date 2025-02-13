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

package org.apache.druid.indexing.overlord;

import com.google.common.base.Supplier;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningStrategy;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OverlordBlinkLeadershipTest
{
  private RemoteTaskRunnerTestUtils rtrUtils;
  private final TestRemoteTaskRunnerConfig remoteTaskRunnerConfig = new TestRemoteTaskRunnerConfig(new Period("PT5M"));
  private final DefaultWorkerBehaviorConfig defaultWorkerBehaviourConfig = DefaultWorkerBehaviorConfig.defaultConfig();
  private final Supplier<WorkerBehaviorConfig> workerBehaviorConfigSupplier = new Supplier<>()
  {
    @Override
    public DefaultWorkerBehaviorConfig get()
    {
      return defaultWorkerBehaviourConfig;
    }
  };
  private final SimpleWorkerProvisioningStrategy resourceManagement = new SimpleWorkerProvisioningStrategy(
      new SimpleWorkerProvisioningConfig(),
      workerBehaviorConfigSupplier,
      new ProvisioningSchedulerConfig()
  );

  @Before
  public void setUp() throws Exception
  {
    rtrUtils = new RemoteTaskRunnerTestUtils();
    rtrUtils.setUp();
  }

  @After
  public void tearDown() throws Exception
  {
    rtrUtils.tearDown();
  }

  /**
   * Test that we can start taskRunner, then stop it (emulating "losing leadership", see {@link
   * TaskMaster#stopBeingLeader()}), then creating a new taskRunner from {@link
   * org.apache.curator.framework.recipes.leader.LeaderSelectorListener#takeLeadership} implementation in
   * {@link TaskMaster} and start it again.
   */
  @Test(timeout = 60_000L)
  public void testOverlordBlinkLeadership()
  {
    try {
      RemoteTaskRunner remoteTaskRunner1 = rtrUtils.makeRemoteTaskRunner(
          remoteTaskRunnerConfig,
          resourceManagement,
          null
      );
      remoteTaskRunner1.stop();
      RemoteTaskRunner remoteTaskRunner2 = rtrUtils.makeRemoteTaskRunner(
          remoteTaskRunnerConfig,
          resourceManagement,
          null
      );
      remoteTaskRunner2.stop();
    }
    catch (Exception e) {
      Assert.fail("Should have not thrown any exceptions, thrown: " + e);
    }
  }
}
