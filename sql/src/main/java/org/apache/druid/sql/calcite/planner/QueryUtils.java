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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.segment.column.RowSignature;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * Utility class for queries
 */
public class QueryUtils
{

  private QueryUtils()
  {
  }

  /**
   * Builds the mappings for queryColumn to outputColumn.
   *
   * @return Mappings for queryColumn to outputColumn
   */
  public static ColumnMappings buildColumnMappings(
      final List<Entry<Integer, String>> fieldMapping,
      final RowSignature rowSignature
  )
  {
    final List<ColumnMapping> columnMappings = new ArrayList<>();
    for (final Entry<Integer, String> entry : fieldMapping) {
      final String queryColumn = rowSignature.getColumnName(entry.getKey());
      final String outputColumn = entry.getValue();
      columnMappings.add(new ColumnMapping(queryColumn, outputColumn));
    }

    return new ColumnMappings(columnMappings);
  }

  public static JoinAlgorithm getJoinAlgorithm(Join join, PlannerContext plannerContext)
  {
    RelHint closestHint = null;
    for (RelHint hint : join.getHints()) {
      if ((closestHint == null || hint.inheritPath.size() < closestHint.inheritPath.size())
          && DruidHint.DruidJoinHint.fromString(hint.hintName) != null) {
        closestHint = hint;
      }
    }

    if (closestHint != null) {
      return DruidHint.DruidJoinHint.fromString(closestHint.hintName).asJoinAlgorithm();
    } else {
      return plannerContext.getJoinAlgorithm();
    }
  }
}
