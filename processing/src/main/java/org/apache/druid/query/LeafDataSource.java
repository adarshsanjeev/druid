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

package org.apache.druid.query;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.SegmentMapFunction;

import java.util.Collections;
import java.util.List;

/**
 * Leaf {@link DataSource}-s have no inputs.
 */
public abstract class LeafDataSource implements DataSource
{
  @Override
  public final List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public final DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    return SegmentMapFunction.IDENTITY;
  }
}
