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

package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A wrapper around a non-null-aware VectorAggregator that makes it null-aware. This removes the need for each
 * aggregator class to handle nulls on its own. This class only makes sense as a wrapper for "primitive" aggregators,
 * i.e., ones that take {@link VectorValueSelector} as input.
 *
 * The result of this aggregator will be null if all the values to be aggregated are null values or no values are
 * aggregated at all. If any of the values are non-null, the result will be the aggregated value of the delegate
 * aggregator.
 *
 * When wrapped by this class, the underlying aggregator's required storage space is increased by one byte. The extra
 * byte is a boolean that stores whether or not any non-null values have been seen. The extra byte is placed before
 * the underlying aggregator's normal state. (Buffer layout = [nullability byte] [delegate storage bytes])
 *
 * The result of a NullableAggregator will be null if all the values to be aggregated are null values
 * or no values are aggregated at all. If any of the value is non-null, the result would be the aggregated
 * value of the delegate aggregator. Note that the delegate aggregator is not required to perform check for
 * {@link VectorValueSelector#getNullVector()} on the selector as only non-null values will be passed
 * to the delegate aggregator. This class is only used when SQL compatible null handling is enabled.
 *
 * @see NullableNumericBufferAggregator , the vectorized version.
 */
public class NullableNumericVectorAggregator implements VectorAggregator
{
  private final VectorAggregator delegate;
  private final VectorValueSelector selector;

  @Nullable
  private int[] vAggregationPositions = null;

  @Nullable
  private int[] vAggregationRows = null;

  NullableNumericVectorAggregator(VectorAggregator delegate, VectorValueSelector selector)
  {
    this.delegate = delegate;
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, TypeStrategies.IS_NULL_BYTE);
    delegate.init(buf, position + Byte.BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final boolean[] nullVector = selector.getNullVector();
    if (nullVector != null) {
      // Deferred initialization, since vAggregationPositions and vAggregationRows are only needed if nulls
      // actually occur.
      if (vAggregationPositions == null) {
        vAggregationPositions = new int[selector.getMaxVectorSize()];
        vAggregationRows = new int[selector.getMaxVectorSize()];
      }

      int j = 0;
      for (int i = startRow; i < endRow; i++) {
        if (!nullVector[i]) {
          vAggregationRows[j++] = i;
        }
      }

      Arrays.fill(vAggregationPositions, 0, j, position);

      doAggregate(buf, j, vAggregationPositions, vAggregationRows, 0);
    } else {
      doAggregate(buf, position, startRow, endRow);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final boolean[] nullVector = selector.getNullVector();
    if (nullVector != null) {
      // Deferred initialization, since vAggregationPositions and vAggregationRows are only needed if nulls
      // actually occur.
      if (vAggregationPositions == null) {
        vAggregationPositions = new int[selector.getMaxVectorSize()];
        vAggregationRows = new int[selector.getMaxVectorSize()];
      }

      int j = 0;
      for (int i = 0; i < numRows; i++) {
        final int rowNum = rows == null ? i : rows[i];
        if (!nullVector[rowNum]) {
          vAggregationPositions[j] = positions[i];
          vAggregationRows[j] = rowNum;
          j++;
        }
      }

      doAggregate(buf, j, vAggregationPositions, vAggregationRows, positionOffset);
    } else {
      doAggregate(buf, numRows, positions, rows, positionOffset);
    }
  }

  @Override
  @Nullable
  public Object get(ByteBuffer buf, int position)
  {
    switch (buf.get(position)) {
      case TypeStrategies.IS_NULL_BYTE:
        return null;
      case TypeStrategies.IS_NOT_NULL_BYTE:
        return delegate.get(buf, position + Byte.BYTES);
      default:
        // Corrupted byte?
        throw new ISE("Bad null-marker byte, delegate class[%s]", delegate.getClass().getName());
    }
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    delegate.relocate(oldPosition + Byte.BYTES, newPosition + Byte.BYTES, oldBuffer, newBuffer);
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  private void doAggregate(ByteBuffer buf, int position, int start, int end)
  {
    buf.put(position, TypeStrategies.IS_NOT_NULL_BYTE);
    delegate.aggregate(buf, position + Byte.BYTES, start, end);
  }

  private void doAggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    for (int i = 0; i < numRows; i++) {
      buf.put(positions[i] + positionOffset, TypeStrategies.IS_NOT_NULL_BYTE);
    }

    delegate.aggregate(buf, numRows, positions, rows, positionOffset + Byte.BYTES);
  }
}
