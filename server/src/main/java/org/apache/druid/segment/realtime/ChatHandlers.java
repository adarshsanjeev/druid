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

package org.apache.druid.segment.realtime;

import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;

public class ChatHandlers
{
  /**
   * Check authorization for the given action and dataSource.
   *
   * @return authorization result
   */
  public static AuthorizationResult authorizationCheck(
      HttpServletRequest req,
      Action action,
      String dataSource,
      AuthorizerMapper authorizerMapper
  )
  {
    ResourceAction resourceAction = new ResourceAction(
        new Resource(dataSource, ResourceType.DATASOURCE),
        action
    );

    AuthorizationResult authResult = AuthorizationUtils.authorizeResourceAction(req, resourceAction, authorizerMapper);
    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }

    return authResult;
  }
}
