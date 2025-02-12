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

package org.apache.druid.query.aggregation.cardinality.types;

import com.google.common.hash.Hasher;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregator;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;

public class StringCardinalityAggregatorColumnSelectorStrategy implements CardinalityAggregatorColumnSelectorStrategy<DimensionSelector>
{
  public static final String CARDINALITY_AGG_NULL_STRING = "\u0000";
  public static final char CARDINALITY_AGG_SEPARATOR = '\u0001';

  public static void addStringToCollector(final HyperLogLogCollector collector, @Nullable final String s)
  {
    // SQL standard spec does not count null values
    if (s != null) {
      collector.add(CardinalityAggregator.HASH_FUNCTION.hashUnencodedChars(s).asBytes());
    }
  }

  @Override
  public void hashRow(DimensionSelector dimSelector, Hasher hasher)
  {
    final IndexedInts row = dimSelector.getRow();
    final int size = row.size();
    // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
    if (size == 1) {
      final String value = dimSelector.lookupName(row.get(0));
      if (value != null) {
        hasher.putUnencodedChars(value);
      }
    } else if (size != 0) {
      boolean hasNonNullValue = false;
      final String[] values = new String[size];
      for (int i = 0; i < size; ++i) {
        final String value = dimSelector.lookupName(row.get(i));
        if (value != null) {
          hasNonNullValue = true;
        }
        values[i] = nullToSpecial(value);
      }
      // note: this behavior is weird and probably not correct, as it skips things like [null, null] instead of
      // counting them, and it sorts the values so [a, b] and [b, a] would be treated as equivalent... and not treated
      // as distinct rows. Leaving as is to avoid changing the behavior for now though
      if (hasNonNullValue) {
        // Values need to be sorted to ensure consistent multi-value ordering across different segments
        Arrays.sort(values);
        for (int i = 0; i < size; ++i) {
          if (i != 0) {
            hasher.putChar(CARDINALITY_AGG_SEPARATOR);
          }
          hasher.putUnencodedChars(values[i]);
        }
      }
    }
  }

  @Override
  public void hashValues(DimensionSelector dimSelector, HyperLogLogCollector collector)
  {
    IndexedInts row = dimSelector.getRow();
    for (int i = 0, rowSize = row.size(); i < rowSize; i++) {
      int index = row.get(i);
      final String value = dimSelector.lookupName(index);
      addStringToCollector(collector, value);
    }
  }

  private static String nullToSpecial(String value)
  {
    return value == null ? CARDINALITY_AGG_NULL_STRING : value;
  }
}
