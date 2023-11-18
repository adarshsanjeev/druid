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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;

import java.util.List;

/**
 * Contains the results for a query to a dataserver. {@link #resultsYielder} contains the results fetched and
 * {@link #segmentsInputSlice} is an {@link SegmentsInputSlice} containing the segments which have already been handed
 * off, so that it can be fetched from deep storage.
 */
public class DataServerQueryResult<RowType>
{

  private final Yielder<RowType> resultsYielder;

  private final SegmentsInputSlice segmentsInputSlice;

  public DataServerQueryResult(
      Yielder<RowType> resultsYielder,
      List<RichSegmentDescriptor> handedOffSegments,
      String dataSource
  )
  {
    this.resultsYielder = resultsYielder;
    this.segmentsInputSlice = new SegmentsInputSlice(dataSource, handedOffSegments, ImmutableList.of());
  }

  public Yielder<RowType> getResultsYielder()
  {
    return resultsYielder;
  }

  public SegmentsInputSlice getHandedOffSegments()
  {
    return segmentsInputSlice;
  }
}
