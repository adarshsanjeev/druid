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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CalciteReplaceDmlTest extends CalciteIngestionDmlTest
{
  private static final Map<String, Object> REPLACE_ALL_TIME_CHUNKS = ImmutableMap.of(
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      "{\"type\":\"all\"}",
      DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
      "all"
  );

  @Override
  protected Map<String, Object> queryContextWithGranularity(Granularity granularity)
  {
    String granularityString = null;
    try {
      granularityString = queryJsonMapper.writeValueAsString(granularity);
    }
    catch (JsonProcessingException e) {
      Assert.fail(e.getMessage());
    }
    return ImmutableMap.of(
        DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, granularityString,
        DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS, "all"
    );
  }

  @Test
  public void testReplaceFromTableForAllTime()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableForPartitionInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR (PARTITION '2000-01-01/P1M', PARTITION '2000-03-01/P1M') PARTITIONED BY MONTH")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(ImmutableMap.of(
                    DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
                    "\"MONTH\"",
                    DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
                    "2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z,2000-03-01T00:00:00.000Z/2000-04-01T00:00:00.000Z"))
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableForPartitionTimestamp()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR PARTITION TIMESTAMP '2000-01-01 00:00:00' PARTITIONED BY MONTH")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(ImmutableMap.of(
                    DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
                    "\"MONTH\"",
                    DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS,
                    "2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z"))
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableForMisalignedPartitionInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR PARTITION '2000-01-05/P1M' PARTITIONED BY MONTH")
        .expectValidationError(
            SqlPlanningException.class,
            "FOR contains a partitionSpec which is not aligned with PARTITIONED BY granularity"
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableForInvalidPartition()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR PARTITION '2000-01-05/P1M' PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "FOR must only contain ALL TIME if it is PARTITIONED BY ALL granularity"
        )
        .verify();
  }

  @Test
  public void testReplaceFromTableForWithInvalidInterval()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo FOR PARTITION '2000-01-01/IMP' PARTITIONED BY MONTH")
        .expectValidationError(SqlPlanningException.class)
        .verify();
  }

  @Test
  public void testReplaceFromTableForWithoutPartitionSpec()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class)
        .verify();
  }

  @Test
  public void testReplaceFromView()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM view.aview FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectTarget("dst", RowSignature.builder().add("dim1_firstchar", ColumnType.STRING).build())
        .expectResources(viewRead("aview"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                .filters(selector("dim2", "a", null))
                .columns("v0")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceIntoQualifiedTable()
  {
    testIngestionQuery()
        .sql("REPLACE INTO druid.dst SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectTarget("dst", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceIntoInvalidDataSourceName()
  {
    testIngestionQuery()
        .sql("REPLACE INTO \"in/valid\" SELECT dim1, dim2 FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "Ingestion dataSource cannot contain the '/' character.")
        .verify();
  }

  @Test
  public void testReplaceUsingColumnList()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst (foo, bar) SELECT dim1, dim2 FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectValidationError(SqlPlanningException.class, "Ingestion with target column list is not supported.")
        .verify();
  }

  @Test
  public void testReplaceIntoSystemTable()
  {
    testIngestionQuery()
        .sql("REPLACE INTO INFORMATION_SCHEMA.COLUMNS SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot ingest into [INFORMATION_SCHEMA.COLUMNS] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceIntoView()
  {
    testIngestionQuery()
        .sql("REPLACE INTO view.aview SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot ingest into [view.aview] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceFromUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM \"%s\" FOR ALL TIME PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceIntoUnauthorizedDataSource()
  {
    testIngestionQuery()
        .sql("REPLACE INTO \"%s\" SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME", CalciteTests.FORBIDDEN_DATASOURCE)
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceIntoNonexistentSchema()
  {
    testIngestionQuery()
        .sql("REPLACE INTO nonexistent.dst SELECT * FROM foo FOR ALL TIME PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "Cannot ingest into [nonexistent.dst] because it is not a Druid datasource."
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternal()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM %s FOR ALL TIME PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", externalDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithPartitionedByAndLimitOffset()
  {
    RowSignature targetRowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("floor_m1", ColumnType.FLOAT)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    testIngestionQuery()
        .sql(
            "REPLACE INTO druid.dst SELECT __time, FLOOR(m1) as floor_m1, dim1 FROM foo LIMIT 10 OFFSET 20 FOR ALL TIME PARTITIONED BY DAY")
        .expectTarget("dst", targetRowSignature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("dst"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "v0")
                .virtualColumns(expressionVirtualColumn("v0", "floor(\"m1\")", ColumnType.FLOAT))
                .limit(10)
                .offset(20)
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceWithPartitionedByContainingInvalidGranularity() throws Exception
  {
    // Throws a ValidationException, which gets converted to a SqlPlanningException before throwing to end user
    try {
      testQuery(
          "REPLACE INTO dst SELECT * FROM foo FOR ALL TIME PARTITIONED BY 'invalid_granularity'",
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Exception should be thrown");
    }
    catch (SqlPlanningException e) {
      Assert.assertEquals(
          "Encountered 'invalid_granularity' after PARTITIONED BY. Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or TIME_FLOOR function",
          e.getMessage()
      );
    }
    didTest = true;
  }

  @Test
  public void testExplainReplaceFromExternal() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final ScanQuery expectedQuery = newScanQueryBuilder()
        .dataSource(externalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("x", "y", "z")
        .context(
            queryJsonMapper.readValue(
                "{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"sqlReplaceTimeChunks\":\"all\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}",
                JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
            )
        )
        .build();

    final String expectedExplanation =
        "DruidQueryRel(query=["
        + queryJsonMapper.writeValueAsString(expectedQuery)
        + "], signature=[{x:STRING, y:STRING, z:LONG}])\n";

    // Use testQuery for EXPLAIN (not testIngestionQuery).
    testQuery(
        new PlannerConfig(),
        StringUtils.format(
            "EXPLAIN PLAN FOR REPLACE INTO dst SELECT * FROM %s FOR ALL TIME PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        ),
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                expectedExplanation,
                "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]"
            }
        )
    );

    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testExplainReplaceFromExternalUnauthorized()
  {
    // Use testQuery for EXPLAIN (not testIngestionQuery).
    Assert.assertThrows(
        ForbiddenException.class,
        () ->
            testQuery(
                StringUtils.format(
                    "EXPLAIN PLAN FOR REPLACE INTO dst SELECT * FROM %s FOR ALL TIME PARTITIONED BY ALL TIME",
                    externSql(externalDataSource)
                ),
                ImmutableList.of(),
                ImmutableList.of()
            )
    );

    // Not using testIngestionQuery, so must set didTest manually to satisfy the check in tearDown.
    didTest = true;
  }

  @Test
  public void testReplaceFromExternalUnauthorized()
  {
    testIngestionQuery()
        .sql("REPLACE INTO dst SELECT * FROM %s FOR ALL TIME PARTITIONED BY ALL TIME", externSql(externalDataSource))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testReplaceFromExternalProjectSort()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst SELECT x || y AS xy, z FROM %s FOR ALL TIME PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", RowSignature.builder().add("xy", ColumnType.STRING).add("z", ColumnType.LONG).build())
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"x\",\"y\")", ColumnType.STRING))
                .columns("v0", "z")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternalAggregate()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst SELECT x, SUM(z) AS sum_z, COUNT(*) AS cnt FROM %s GROUP BY 1 FOR ALL TIME PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget(
            "dst",
            RowSignature.builder()
                        .add("x", ColumnType.STRING)
                        .add("sum_z", ColumnType.LONG)
                        .add("cnt", ColumnType.LONG)
                        .build()
        )
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource(externalDataSource)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("x", "d0")))
                        .setAggregatorSpecs(
                            new LongSumAggregatorFactory("a0", "z"),
                            new CountAggregatorFactory("a1")
                        )
                        .setContext(REPLACE_ALL_TIME_CHUNKS)
                        .build()
        )
        .verify();
  }

  @Test
  public void testReplaceFromExternalAggregateAll()
  {
    testIngestionQuery()
        .sql(
            "REPLACE INTO dst SELECT COUNT(*) AS cnt FROM %s FOR ALL TIME PARTITIONED BY ALL TIME",
            externSql(externalDataSource)
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget(
            "dst",
            RowSignature.builder()
                        .add("cnt", ColumnType.LONG)
                        .build()
        )
        .expectResources(dataSourceWrite("dst"), ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            GroupByQuery.builder()
                        .setDataSource(externalDataSource)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(REPLACE_ALL_TIME_CHUNKS)
                        .build()
        )
        .verify();
  }
}
