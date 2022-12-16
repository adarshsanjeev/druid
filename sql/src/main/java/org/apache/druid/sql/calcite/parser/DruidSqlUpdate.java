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

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DruidSqlUpdate extends DruidSqlReplace
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
                                      SqlNodeList sourceExpressionList
  )
  {
    //SqlBasicCall call = new SqlBasicCall(SqlCaseOperator.INSTANCE, )

    assert targetColumnList.size() == 1;
    SqlCase sqlCase = new SqlCase(pos, null, SqlNodeList.of(source), sourceExpressionList, targetColumnList);
    SqlIdentifier identifier = (SqlIdentifier) targetColumnList.get(0);
    SqlBasicCall asCall = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{sqlCase, identifier}, pos);

    SqlIdentifier star = SqlIdentifier.star(pos);

    SqlNodeList sqlNodes = SqlNodeList.of(asCall, star);

    SqlSelect sqlSelect = new SqlSelect(pos, keywords, sqlNodes, targetTable, null, null, null, null, null, null, null);

    SqlInsert sqlInsert = new SqlInsert(pos, keywords, targetTable, sqlSelect, columnList);
    return new DruidSqlUpdate(sqlInsert, partitionedBy, partitionedByStringForUnparse, clusteredBy, null);
  }

  public DruidSqlUpdate(
      @Nonnull SqlInsert insertNode,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlNode replaceTimeQuery
  )
  {
    super(
        insertNode,
        partitionedBy,
        partitionedByStringForUnparse,
        clusteredBy,
        replaceTimeQuery
    );
  }
}
