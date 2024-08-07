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

package org.apache.druid.curator.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.ISE;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ServiceAnnouncerTest extends CuratorTestBase
{
  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
  }

  @Test
  public void testServiceAnnouncement() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    List<String> serviceNames = ImmutableList.of(
        "druid/overlord",
        "druid/coordinator"
    );
    final ServiceDiscovery serviceDiscovery = createAndAnnounceServices(serviceNames);
    Assert.assertTrue(
        Iterators.all(
            serviceNames.iterator(),
            input -> {
              try {
                return serviceDiscovery.queryForInstances(input.replace('/', ':')).size() == 1;
              }
              catch (Exception e) {
                throw new ISE(
                    "Something went wrong while finding instance with name [%s] in Service Discovery",
                    input
                );
              }
            }
        )
    );
  }

  @Test (expected = IllegalArgumentException.class)
  public void testServiceAnnouncementFail() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    createAndAnnounceServices(ImmutableList.of("placeholder/\u0001"));
  }

  private ServiceDiscovery createAndAnnounceServices(List<String> serviceNames) throws Exception
  {
    int port = 1000;
    ServiceDiscovery<Void> serviceDiscovery =
        ServiceDiscoveryBuilder.builder(Void.class)
                               .basePath("/test")
                               .client(curator)
                               .build();
    for (String serviceName : serviceNames) {
      String serviceNameToUse = CuratorServiceUtils.makeCanonicalServiceName(serviceName);
      ServiceInstance instance = ServiceInstance.<Void>builder()
                                                .name(serviceNameToUse)
                                                .address("localhost")
                                                .port(port++)
                                                .build();
      serviceDiscovery.registerService(instance);
    }
    return serviceDiscovery;
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }
}
