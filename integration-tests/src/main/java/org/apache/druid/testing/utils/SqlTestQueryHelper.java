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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.SqlResourceTestClient;

public class SqlTestQueryHelper extends AbstractTestQueryHelper<SqlQueryWithResults>
{
  private static final Logger LOG = new Logger(SqlTestQueryHelper.class);

  @Inject
  public SqlTestQueryHelper(
      ObjectMapper jsonMapper,
      SqlResourceTestClient sqlClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, sqlClient, config);
  }

  @Override
  public String getQueryURL(String schemeAndHost)
  {
    return StringUtils.format("%s/druid/v2/sql", schemeAndHost);
  }

  public boolean isDatasourceLoadedInSQL(String datasource)
  {
    final SqlQuery query = new SqlQuery(
        "SELECT 1 FROM \"" + datasource + "\" LIMIT 1",
        null,
        false,
        false,
        false,
        null,
        null
    );
    
    try {
      //noinspection unchecked
      queryClient.query(getQueryURL(broker), query, "Is datasource loaded");
      return true;
    }
    catch (Exception e) {
      LOG.debug(e, "Check query failed");
      return false;
    }
  }

  public boolean verifyTimeColumnIsPresent(String datasource)
  {
    final SqlQuery query = new SqlQuery(
        "SELECT __time FROM \"" + datasource + "\" LIMIT 1",
        null,
        false,
        false,
        false,
        null,
        null
    );

    try {
      //noinspection unchecked
      queryClient.query(getQueryURL(broker), query, "Is time column present");
      return true;
    }
    catch (Exception e) {
      LOG.debug(e, "Check query failed");
      return false;
    }
  }
}
