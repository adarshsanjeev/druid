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

package org.apache.druid.msq.rpc;

import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Utility methods for MSQ resources such as {@link ControllerResource}.
 */
public class MSQResourceUtils
{
  public static void authorizeAdminRequest(
      final ResourcePermissionMapper permissionMapper,
      final AuthorizerMapper authorizerMapper,
      final HttpServletRequest request
  )
  {
    final List<ResourceAction> resourceActions = permissionMapper.getAdminPermissions();

    AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        request,
        resourceActions,
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }
  }

  public static void authorizeQueryRequest(
      final ResourcePermissionMapper permissionMapper,
      final AuthorizerMapper authorizerMapper,
      final HttpServletRequest request,
      final String queryId
  )
  {
    final List<ResourceAction> resourceActions = permissionMapper.getQueryPermissions(queryId);

    AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        request,
        resourceActions,
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }
  }
}
