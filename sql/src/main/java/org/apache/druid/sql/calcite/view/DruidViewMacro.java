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

package org.apache.druid.sql.calcite.view;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;

import java.util.Collections;
import java.util.List;

public class DruidViewMacro implements TableMacro
{
  private final PlannerFactory plannerFactory;
  private final String viewSql;
  private final String druidSchemaName;

  @Inject
  public DruidViewMacro(
      @Assisted final PlannerFactory plannerFactory,
      @Assisted final String viewSql,
      @DruidSchemaName String druidSchemaName
  )
  {
    this.plannerFactory = plannerFactory;
    this.viewSql = viewSql;
    this.druidSchemaName = druidSchemaName;
  }

  @Override
  public TranslatableTable apply(final List<?> arguments)
  {
    final RelDataType rowType;
    try (final DruidPlanner planner =
             plannerFactory.createPlanner(
                 ViewSqlEngine.INSTANCE,
                 viewSql,
                 DruidSqlParser.parse(viewSql, false).getMainStatement(), // views cannot embed SET
                 Collections.emptyMap(),
                 null
             )
    ) {
      planner.validate();
      rowType = planner.prepare().getValidatedRowType();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new ViewTable(
        null,
        RelDataTypeImpl.proto(rowType),
        viewSql,
        ImmutableList.of(druidSchemaName),
        null
    );
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return ImmutableList.of();
  }
}
