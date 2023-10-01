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

package org.apache.druid.msq.input.table;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.LoadedSegmentDataProvider;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A holder for a supplier of a physical segment.
 */
public class SegmentWithDescriptor
{
  private final Supplier<? extends ResourceHolder<Segment>> segmentSupplier;
  private final LoadedSegmentDataProvider loadedSegmentDataProvider;
  private final RichSegmentDescriptor descriptor;

  /**
   * Create a new instance.
   *
   * @param segmentSupplier           supplier of a {@link ResourceHolder} of segment. The {@link ResourceHolder#close()}
   *                                  logic must include a delegated call to {@link Segment#close()}.
   * @param loadedSegmentDataProvider {@link LoadedSegmentDataProvider} which fetches the corresponding results from a
   *                                  data server where the segment is loaded. The call will fetch the
   * @param descriptor                segment descriptor
   */
  public SegmentWithDescriptor(
      final Supplier<? extends ResourceHolder<Segment>> segmentSupplier,
      final LoadedSegmentDataProvider loadedSegmentDataProvider,
      final RichSegmentDescriptor descriptor
  )
  {
    this.segmentSupplier = Preconditions.checkNotNull(segmentSupplier, "segment");
    this.loadedSegmentDataProvider = loadedSegmentDataProvider;
    this.descriptor = Preconditions.checkNotNull(descriptor, "descriptor");
  }

  /**
   * The physical segment.
   * <br>
   * Named "getOrLoad" because the segment may be generated by a lazy supplier. In this case, the segment is acquired
   * as part of the call to this method.
   * <br>
   * It is not necessary to call {@link Segment#close()} on the returned segment. Calling {@link ResourceHolder#close()}
   * is enough.
   */
  public ResourceHolder<Segment> getOrLoad()
  {
    return segmentSupplier.get();
  }

  public <QueryType, ReturnType> Pair<LoadedSegmentDataProvider.DataServerQueryStatus, Yielder<ReturnType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      Function<Sequence<QueryType>, Sequence<ReturnType>> mappingFunction,
      Class<QueryType> queryResultType,
      Closer closer
  ) throws IOException
  {
    return loadedSegmentDataProvider.fetchRowsFromDataServer(query, descriptor, mappingFunction, queryResultType, closer);
  }

  /**
   * The segment descriptor associated with this physical segment.
   */
  public SegmentDescriptor getDescriptor()
  {
    return descriptor;
  }

  public boolean isLoadedOnServer()
  {
    return !CollectionUtils.isNullOrEmpty(descriptor.getServers());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentWithDescriptor that = (SegmentWithDescriptor) o;
    return Objects.equals(segmentSupplier, that.segmentSupplier) && Objects.equals(descriptor, that.descriptor);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentSupplier, descriptor);
  }
}
