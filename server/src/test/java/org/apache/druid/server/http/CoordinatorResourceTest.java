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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CloneStatusMetrics;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.duty.DutyGroupStatus;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorResourceTest
{
  private DruidCoordinator mock;
  private CloneStatusManager cloneStatusManager;
  private CoordinatorDynamicConfigSyncer coordinatorDynamicConfigSyncer;

  @Before
  public void setUp()
  {
    mock = EasyMock.createStrictMock(DruidCoordinator.class);
    cloneStatusManager = EasyMock.createStrictMock(CloneStatusManager.class);
    coordinatorDynamicConfigSyncer = EasyMock.createStrictMock(CoordinatorDynamicConfigSyncer.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mock);
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(mock.getCurrentLeader()).andReturn("boz").once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getLeader();
    Assert.assertEquals("boz", response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(mock.isLeader()).andReturn(true).once();
    EasyMock.expect(mock.isLeader()).andReturn(false).once();
    EasyMock.replay(mock);

    // true
    final Response response1 = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", true), response1.getEntity());
    Assert.assertEquals(200, response1.getStatus());

    // false
    final Response response2 = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", false), response2.getEntity());
    Assert.assertEquals(404, response2.getStatus());
  }

  @Test
  public void testGetLoadStatusSimple()
  {
    EasyMock.expect(mock.getLoadManagementPeons())
            .andReturn(ImmutableMap.of("hist1", new TestLoadQueuePeon()))
            .once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getLoadQueue("true", null);
    Assert.assertEquals(
        ImmutableMap.of(
            "hist1",
            ImmutableMap.of(
                "segmentsToDrop", 0,
                "segmentsToLoad", 0,
                "segmentsToLoadSize", 0L,
                "segmentsToDropSize", 0L,
                "expectedLoadTimeMillis", 0L
            )
        ),
        response.getEntity()
    );
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetStatusOfDuties()
  {
    final DateTime now = DateTimes.nowUtc();
    final DutyGroupStatus dutyGroupStatus = new DutyGroupStatus(
        "HistoricalManagementDuties",
        Duration.standardMinutes(1),
        Collections.singletonList("org.apache.druid.duty.RunRules"),
        now.minusMinutes(5),
        now,
        100L,
        500L
    );

    EasyMock.expect(mock.getStatusOfDuties()).andReturn(
        Collections.singletonList(dutyGroupStatus)
    ).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getStatusOfDuties();
    Assert.assertEquals(200, response.getStatus());

    final Object payload = response.getEntity();
    Assert.assertTrue(payload instanceof CoordinatorDutyStatus);

    final List<DutyGroupStatus> observedDutyGroups = ((CoordinatorDutyStatus) payload).getDutyGroups();
    Assert.assertEquals(1, observedDutyGroups.size());

    final DutyGroupStatus observedStatus = observedDutyGroups.get(0);
    Assert.assertEquals("HistoricalManagementDuties", observedStatus.getName());
    Assert.assertEquals(Duration.standardMinutes(1), observedStatus.getPeriod());
    Assert.assertEquals(
        Collections.singletonList("org.apache.druid.duty.RunRules"),
        observedStatus.getDutyNames()
    );
    Assert.assertEquals(now.minusMinutes(5), observedStatus.getLastRunStart());
    Assert.assertEquals(now, observedStatus.getLastRunEnd());
    Assert.assertEquals(100L, observedStatus.getAvgRuntimeMillis());
    Assert.assertEquals(500L, observedStatus.getAvgRunGapMillis());
  }

  @Test
  public void testGetBrokerStatus()
  {
    EasyMock.expect(coordinatorDynamicConfigSyncer.getInSyncBrokers()).andReturn(Set.of("brok1")).once();
    EasyMock.replay(mock);
    EasyMock.replay(coordinatorDynamicConfigSyncer);
    EasyMock.replay(cloneStatusManager);

    final Response response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getBrokerStatus();
    Assert.assertEquals(200, response.getStatus());

    Assert.assertEquals(Set.of("brok1"), response.getEntity());
  }

  @Test
  public void testGetCloneStatus()
  {
    Map<String, CloneStatusMetrics> statusMetrics = ImmutableMap.of(
        "hist1", new CloneStatusMetrics("hist3", CloneStatusMetrics.Status.LOADING, 2, 0, 1000),
        "hist2", CloneStatusMetrics.unknown("hist4")
    );

    EasyMock.expect(cloneStatusManager.getStatusForAllServers()).andReturn(statusMetrics).once();
    EasyMock.expect(cloneStatusManager.getStatusForServer("hist2")).andReturn(CloneStatusMetrics.unknown("hist4")).once();
    EasyMock.replay(mock);
    EasyMock.replay(coordinatorDynamicConfigSyncer);
    EasyMock.replay(cloneStatusManager);

    Response response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getCloneStatus(null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(statusMetrics, response.getEntity());

    response = new CoordinatorResource(mock, cloneStatusManager, coordinatorDynamicConfigSyncer).getCloneStatus("hist2");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(Map.of("hist2", CloneStatusMetrics.unknown("hist4")), response.getEntity());
  }
}
