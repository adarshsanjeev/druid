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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.error.DruidException;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.CalciteExportTest.ExportComponentSupplier;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.destination.ExportDestination;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.local.LocalFileExportStorageProvider;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

@SqlTestFrameworkConfig.ComponentSupplier(ExportComponentSupplier.class)
public class CalciteExportTest extends CalciteIngestionDmlTest
{
  protected static class ExportComponentSupplier extends IngestionDmlComponentSupplier
  {
    public ExportComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(
          super.getCoreModule(),
          new StorageConnectorModule()
      );
    }

    @Override
    public DruidModule getOverrideModule()
    {
      return DruidModuleCollection.of(
          super.getOverrideModule(),
          new LocalOverrideModule()
      );
    }

    private static final class LocalOverrideModule implements DruidModule
    {
      @Override
      public List<? extends Module> getJacksonModules()
      {
        return ImmutableList.of(
            new SimpleModule(StorageConnectorProvider.class.getSimpleName()).registerSubtypes(
                new NamedType(LocalFileExportStorageProvider.class, CalciteTests.FORBIDDEN_DESTINATION)
            )
        );
      }

      @Override
      public void configure(Binder binder)
      {
        binder.bind(StorageConfig.class).toInstance(new StorageConfig("/tmp/export"));
      }
    }
  }

  // Disabled until replace supports external destinations. To be enabled after that point.
  @Test
  @Disabled
  public void testReplaceIntoExtern()
  {
    testIngestionQuery()
        .sql(StringUtils.format("REPLACE INTO EXTERN(%s(exportPath => 'export')) "
                                + "AS CSV "
                                + "OVERWRITE ALL "
                                + "SELECT dim2 FROM foo", LocalFileExportStorageProvider.TYPE_NAME))
        .expectQuery(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      "foo"
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .columnTypes(ColumnType.STRING)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        )
        .expectResources(dataSourceRead("foo"), externalWrite(LocalFileExportStorageProvider.TYPE_NAME))
        .expectTarget(ExportDestination.TYPE_KEY, RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testReplaceIntoExternShouldThrowUnsupportedException()
  {
    testIngestionQuery()
        .sql(StringUtils.format("REPLACE INTO EXTERN(%s(exportPath => 'export')) "
                                + "AS CSV "
                                + "OVERWRITE ALL "
                                + "SELECT dim2 FROM foo", LocalFileExportStorageProvider.TYPE_NAME))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(DruidException.class),
                ThrowableMessageMatcher.hasMessage(
                    CoreMatchers.containsString(
                        "REPLACE operations do no support EXTERN destinations. Use INSERT statements to write to an external destination."
                    )
                )
            )
        )
        .verify();
  }

  @Test
  public void testExportWithoutRequiredParameter()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO EXTERN(%s()) "
                                + "AS CSV "
                                + "SELECT dim2 FROM foo", LocalFileExportStorageProvider.TYPE_NAME))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(IllegalArgumentException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Missing required creator property 'exportPath'"))
            )
        )
        .verify();
  }

  @Test
  public void testExportWithPartitionedBy()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO EXTERN(%s(exportPath=>'/tmp/export')) "
                                + "AS CSV "
                                + "SELECT dim2 FROM foo "
                                + "PARTITIONED BY ALL", LocalFileStorageConnectorProvider.TYPE_NAME))
        .expectValidationError(
            DruidException.class,
            "Export statements do not support a PARTITIONED BY or CLUSTERED BY clause."
        )
        .verify();
  }

  @Test
  public void testInsertIntoExtern()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO EXTERN(%s(exportPath=>'/tmp/export')) "
                                + "AS CSV "
                                + "SELECT dim2 FROM foo", LocalFileStorageConnectorProvider.TYPE_NAME))
        .expectQuery(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      "foo"
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .columnTypes(ColumnType.STRING)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        )
        .expectResources(dataSourceRead("foo"), externalWrite(LocalFileStorageConnectorProvider.TYPE_NAME))
        .expectTarget(ExportDestination.TYPE_KEY, RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testInsertIntoExternParameterized()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO EXTERN(%s(exportPath=>'/tmp/export')) "
                                + "AS CSV "
                                + "SELECT dim2 FROM foo WHERE dim2=?", LocalFileStorageConnectorProvider.TYPE_NAME))
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, "val")))
        .expectQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    "foo"
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("dim2", "val", ColumnType.STRING))
                .columns("dim2")
                .columnTypes(ColumnType.STRING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .build()
        )
        .expectResources(dataSourceRead("foo"), externalWrite(LocalFileStorageConnectorProvider.TYPE_NAME))
        .expectTarget(ExportDestination.TYPE_KEY, RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  // Disabled until replace supports external destinations. To be enabled after that point.
  @Test
  @Disabled
  public void testReplaceIntoExternParameterized()
  {
    testIngestionQuery()
        .sql(StringUtils.format("REPLACE INTO EXTERN(%s(exportPath=>'/tmp/export')) "
                                + "AS CSV "
                                + "SELECT dim2 FROM foo WHERE dim2=?", LocalFileStorageConnectorProvider.TYPE_NAME))
        .parameters(Collections.singletonList(new SqlParameter(SqlType.VARCHAR, "val")))
        .expectQuery(
            Druids.newScanQueryBuilder()
                .dataSource(
                    "foo"
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("dim2", "val", ColumnType.STRING))
                .columns("dim2")
                .columnTypes(ColumnType.STRING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .build()
        )
        .expectResources(dataSourceRead("foo"), externalWrite(LocalFileStorageConnectorProvider.TYPE_NAME))
        .expectTarget(ExportDestination.TYPE_KEY, RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testExportWithoutFormat()
  {
    testIngestionQuery()
        .sql("INSERT INTO EXTERN(testStorage(bucket=>'bucket1',prefix=>'prefix1',tempDir=>'/tempdir',chunkSize=>'5242880',maxRetry=>'1')) "
             + "SELECT dim2 FROM foo")
        .expectValidationError(
            DruidException.class,
            "Exporting rows into an EXTERN destination requires an AS clause to specify the format, but none was found."
        )
        .verify();
  }

  @Test
  public void testWithUnsupportedStorageConnector()
  {
    testIngestionQuery()
        .sql("insert into extern(nonExistent()) as csv select  __time, dim1 from foo")
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(IllegalArgumentException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Could not resolve type id 'nonExistent' as a subtype"))
            )
        )
        .verify();
  }

  @Test
  public void testWithForbiddenDestination()
  {
    testIngestionQuery()
        .sql(StringUtils.format("insert into extern(%s(exportPath=>'/tmp/export')) as csv select  __time, dim1 from foo", CalciteTests.FORBIDDEN_DESTINATION))
        .expectValidationError(ForbiddenException.class)
        .verify();
  }

  @Test
  public void testSelectFromTableNamedExport()
  {
    testIngestionQuery()
        .sql("INSERT INTO csv SELECT dim2 FROM foo PARTITIONED BY ALL")
        .expectQuery(
            Druids.newScanQueryBuilder()
                  .dataSource("foo")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim2")
                  .columnTypes(ColumnType.STRING)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        )
        .expectResources(dataSourceRead("foo"), dataSourceWrite("csv"))
        .expectTarget("csv", RowSignature.builder().add("dim2", ColumnType.STRING).build())
        .verify();
  }

  @Test
  public void testNormalInsertWithFormat()
  {
    testIngestionQuery()
        .sql("REPLACE INTO testTable "
             + "AS CSV "
             + "OVERWRITE ALL "
             + "SELECT dim2 FROM foo "
             + "PARTITIONED BY ALL")
        .expectValidationError(
            DruidException.class,
            "The AS <format> clause should only be specified while exporting rows into an EXTERN destination."
        )
        .verify();
  }

  @Test
  public void testUnsupportedExportFormat()
  {
    testIngestionQuery()
        .sql("REPLACE INTO testTable "
             + "AS JSON "
             + "OVERWRITE ALL "
             + "SELECT dim2 FROM foo "
             + "PARTITIONED BY ALL")
        .expectValidationError(
            DruidException.class,
            "The AS <format> clause should only be specified while exporting rows into an EXTERN destination."
        )
        .verify();
  }
}
