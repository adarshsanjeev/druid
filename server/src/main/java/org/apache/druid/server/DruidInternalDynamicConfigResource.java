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

package org.apache.druid.server;

import com.google.inject.Inject;
import org.apache.druid.client.DynamicConfigurationManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicReference;

@Path("/druid-internal/v1/dynamicConfiguration")
public class DruidInternalDynamicConfigResource
{
  // TODO: Probably a better way
  private final AtomicReference<CoordinatorDynamicConfig> reference =
      new AtomicReference<>(CoordinatorDynamicConfig.builder().build());
  private final DynamicConfigurationManager dynamicConfigurationManager;

  @Inject
  public DruidInternalDynamicConfigResource(DynamicConfigurationManager dynamicConfigurationManager)
  {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/coordinatorDynamicConfig")
  public Response getDatasource()
  {
    return Response.ok(reference.get()).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/coordinatorDynamicConfig")
  public Response getDatasource(final CoordinatorDynamicConfig.Builder dynamicConfigBuilder)
  {
    reference.set(dynamicConfigBuilder.build(reference.get()));
    dynamicConfigurationManager.updateCloneServers(reference.get());
    return Response.ok().build();
  }
}
