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

package org.apache.druid.benchmark;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.CursorGranularizer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexTimeBoundaryInspector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 10, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionSelectorBenchmark
{
  static {
    ExpressionProcessing.initializeForTests();
  }

  @Param({"1000000"})
  private int rowsPerSegment;

  private QueryableIndex index;
  private Closer closer;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = new GeneratorSchemaInfo(
        ImmutableList.of(
            GeneratorColumnSchema.makeZipf(
                "n",
                ValueType.LONG,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            ),
            GeneratorColumnSchema.makeZipf(
                "s",
                ValueType.STRING,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            )
        ),
        ImmutableList.of(),
        Intervals.of("2000/P1D"),
        false
    );

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    this.index = closer.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, rowsPerSegment)
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  public void timeFloorUsingExpression(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "timestamp_floor(__time, 'PT1H')",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();

      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void timeFloorUsingExtractionFn(Blackhole blackhole)
  {
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();

      final DimensionSelector selector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(
              new ExtractionDimensionSpec(
                  ColumnHolder.TIME_COLUMN_NAME,
                  "v",
                  new TimeFormatExtractionFn(null, null, null, Granularities.HOUR, true)
              )
          );
      consumeDimension(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void timeFloorUsingCursor(Blackhole blackhole)
  {
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      final CursorGranularizer granularizer = CursorGranularizer.create(
          cursor,
          QueryableIndexTimeBoundaryInspector.create(index),
          Cursors.getTimeOrdering(index.getOrdering()),
          Granularities.HOUR,
          index.getDataInterval()
      );
      final Sequence<Long> results =
          Sequences.simple(granularizer.getBucketIterable())
                   .map(bucketInterval -> {
                     if (!granularizer.advanceToBucket(bucketInterval)) {
                       return 0L;
                     }
                     long count = 0L;
                     while (!cursor.isDone()) {
                       count++;
                       if (!granularizer.advanceCursorWithinBucket()) {
                         break;
                       }
                     }
                     return count;
                   });

      long count = 0L;
      for (Long result : results.toList()) {
        count += result;
      }

      blackhole.consume(count);
    }
  }

  @Benchmark
  public void timeFormatUsingExpression(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "timestamp_format(__time, 'yyyy-MM-dd')",
                                                                 ColumnType.STRING,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final DimensionSelector selector = cursor.getColumnSelectorFactory().makeDimensionSelector(
          DefaultDimensionSpec.of("v")
      );
      consumeDimension(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void timeFormatUsingExtractionFn(Blackhole blackhole)
  {
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      final DimensionSelector selector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(
              new ExtractionDimensionSpec(
                  ColumnHolder.TIME_COLUMN_NAME,
                  "v",
                  new TimeFormatExtractionFn("yyyy-MM-dd", null, null, null, false)
              )
          );
      consumeDimension(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void strlenUsingExpressionAsLong(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "strlen(s)",
                                                                 ColumnType.STRING,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void strlenUsingExpressionAsString(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "strlen(s)",
                                                                 ColumnType.STRING,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final DimensionSelector selector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new DefaultDimensionSpec("v", "v", ColumnType.STRING));

      consumeDimension(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void strlenUsingExtractionFn(Blackhole blackhole)
  {
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      final DimensionSelector selector = cursor
          .getColumnSelectorFactory()
          .makeDimensionSelector(new ExtractionDimensionSpec("x", "v", StrlenExtractionFn.instance()));

      consumeDimension(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void arithmeticOnLong(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "n + 1",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void stringConcatAndCompareOnLong(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "concat(n, ' is my favorite number') == '3 is my favorite number'",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void caseSearched1(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "case_searched(s == 'asd' || isnull(s) || s == 'xxx', 1, s == 'foo' || s == 'bar', 2, 3)",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void caseSearched2(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "case_searched(s == 'asd' || isnull(s) || n == 1, 1, n == 2, 2, 3)",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }


  @Benchmark
  public void caseSearched100(Blackhole blackhole)
  {
    StringBuilder caseBranches = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      caseBranches.append(
          StringUtils.format(
              "n == %d, %d,",
              i,
              i * i
          )
      );
    }

    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "case_searched(s == 'asd' || isnull(s) || n == 1, 1, " + caseBranches + " 3)",
                                                                 ColumnType.LONG,
                                                                 TestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void caseSearchedWithLookup(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             new ExpressionVirtualColumn(
                                                                 "v",
                                                                 "case_searched(n == 1001, -1, "
                                                                 + "lookup(s, 'lookyloo') == 'asd1', 1, "
                                                                 + "lookup(s, 'lookyloo') == 'asd2', 2, "
                                                                 + "lookup(s, 'lookyloo') == 'asd3', 3, "
                                                                 + "lookup(s, 'lookyloo') == 'asd4', 4, "
                                                                 + "lookup(s, 'lookyloo') == 'asd5', 5, "
                                                                 + "-2)",
                                                                 ColumnType.LONG,
                                                                 LookupEnabledTestExprMacroTable.INSTANCE
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }

  @Benchmark
  public void caseSearchedWithLookup2(Blackhole blackhole)
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             ImmutableList.of(
                                                                 new ExpressionVirtualColumn(
                                                                     "ll",
                                                                     "lookup(s, 'lookyloo')",
                                                                     ColumnType.STRING,
                                                                     LookupEnabledTestExprMacroTable.INSTANCE
                                                                 ),
                                                                 new ExpressionVirtualColumn(
                                                                     "v",
                                                                     "case_searched(n == 1001, -1, "
                                                                     + "ll == 'asd1', 1, "
                                                                     + "ll == 'asd2', 2, "
                                                                     + "ll == 'asd3', 3, "
                                                                     + "ll == 'asd4', 4, "
                                                                     + "ll == 'asd5', 5, "
                                                                     + "-2)",
                                                                     ColumnType.LONG,
                                                                     LookupEnabledTestExprMacroTable.INSTANCE
                                                                 )
                                                             )
                                                         )
                                                     )
                                                     .build();

    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
      consumeLong(cursor, selector, blackhole);
    }
  }



  private void consumeDimension(final Cursor cursor, final DimensionSelector selector, final Blackhole blackhole)
  {
    if (selector.getValueCardinality() >= 0) {
      // Read all IDs and then lookup all names.
      final BitSet values = new BitSet();

      while (!cursor.isDone()) {
        final int value = selector.getRow().get(0);
        values.set(value);
        cursor.advance();
      }

      for (int i = values.nextSetBit(0); i >= 0; i = values.nextSetBit(i + 1)) {
        blackhole.consume(selector.lookupName(i));
      }
    } else {
      // Lookup names as we go.
      while (!cursor.isDone()) {
        final int value = selector.getRow().get(0);
        blackhole.consume(selector.lookupName(value));
        cursor.advance();
      }
    }
  }

  private void consumeLong(final Cursor cursor, final ColumnValueSelector selector, final Blackhole blackhole)
  {
    while (!cursor.isDone()) {
      blackhole.consume(selector.getLong());
      cursor.advance();
    }
  }
}
