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

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

public class DruidSqlUpdate extends DruidSqlIngest
{

  private SqlNodeList targetColumnList;
  private SqlNodeList sourceExpressionList;

  public static DruidSqlUpdate create(SqlParserPos pos,
                                      SqlNodeList keywords,
                                      SqlNode targetTable,
                                      SqlNode source,
                                      SqlNodeList columnList,
                                      @Nullable Granularity partitionedBy,
                                      @Nullable String partitionedByStringForUnparse,
                                      @Nullable SqlNodeList clusteredBy,
                                      SqlNodeList targetColumnList,
                                      SqlNodeList sourceExpressionList)
  {
    SqlNodeList sqlNodes = new SqlNodeList(Collections.singleton(SqlIdentifier.star(pos)), pos);
    SqlSelect sqlSelect = new SqlSelect(pos, keywords, sqlNodes, targetTable, source, null, null, null, null, null, null);
    SqlInsert sqlInsert = new SqlInsert(pos, keywords, targetTable, sqlSelect, columnList);
    return new DruidSqlUpdate(sqlInsert, partitionedBy, partitionedByStringForUnparse, clusteredBy, targetColumnList, sourceExpressionList);
  }

  public DruidSqlUpdate(
      @Nonnull SqlInsert insertNode,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList
  )
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList(),
        partitionedBy,
        partitionedByStringForUnparse,
        clusteredBy
    );
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
  }
}
