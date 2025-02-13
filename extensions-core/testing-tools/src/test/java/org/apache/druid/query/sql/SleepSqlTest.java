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

package org.apache.druid.query.sql;

import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.sql.SleepSqlTest.SleepComponentSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.Test;

@SqlTestFrameworkConfig.ComponentSupplier(SleepComponentSupplier.class)
public class SleepSqlTest extends BaseCalciteQueryTest
{
  public static class SleepComponentSupplier extends StandardComponentSupplier
  {
    public SleepComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new SleepModule());
    }
  }

  @Test
  public void testSleepFunction()
  {
    testQuery(
        "SELECT sleep(m1) from foo where m1 < 2.0",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("foo"))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "sleep(\"m1\")",
                          ColumnType.STRING,
                          queryFramework().macroTable()
                      )
                  )
                  .columns("v0")
                  .columnTypes(ColumnType.STRING)
                  .filters(range("m1", ColumnType.DOUBLE, null, 2.0, false, true))
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{null}
        )
    );
  }
}
