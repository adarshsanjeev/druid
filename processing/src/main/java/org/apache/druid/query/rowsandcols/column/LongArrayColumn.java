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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.NotYetImplemented;
import org.apache.druid.query.rowsandcols.util.FindResult;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

public class LongArrayColumn implements Column
{
  private final long[] vals;

  public LongArrayColumn(
      long[] vals
  )
  {
    this.vals = vals;
  }

  @Nonnull
  @Override
  public ColumnAccessor toAccessor()
  {
    return new MyColumnAccessor();
  }

  @Nullable
  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    if (VectorCopier.class.equals(clazz)) {
      return (T) (VectorCopier) (into, intoStart) -> {
        if (Integer.MAX_VALUE - vals.length < intoStart) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                              .build(
                                  "too many rows!!! intoStart[%,d], vals.length[%,d] combine to exceed max_int",
                                  intoStart,
                                  vals.length
                              );
        }
        for (int i = 0; i < vals.length; ++i) {
          into[intoStart + i] = vals[i];
        }
      };
    }
    if (ColumnValueSwapper.class.equals(clazz)) {
      return (T) (ColumnValueSwapper) (lhs, rhs) -> {
        long tmp = vals[lhs];
        vals[lhs] = vals[rhs];
        vals[rhs] = tmp;
      };
    }
    return null;
  }

  private class MyColumnAccessor implements BinarySearchableAccessor
  {
    @Override
    public ColumnType getType()
    {
      return ColumnType.LONG;
    }

    @Override
    public int numRows()
    {
      return vals.length;
    }

    @Override
    public boolean isNull(int rowNum)
    {
      return false;
    }

    @Override
    public Object getObject(int rowNum)
    {
      return vals[rowNum];
    }

    @Override
    public double getDouble(int rowNum)
    {
      return vals[rowNum];
    }

    @Override
    public float getFloat(int rowNum)
    {
      return vals[rowNum];
    }

    @Override
    public long getLong(int rowNum)
    {
      return vals[rowNum];
    }

    @Override
    public int getInt(int rowNum)
    {
      return (int) vals[rowNum];
    }

    @Override
    public int compareRows(int lhsRowNum, int rhsRowNum)
    {
      return Long.compare(vals[lhsRowNum], vals[rhsRowNum]);
    }


    @Override
    public FindResult findNull(int startIndex, int endIndex)
    {
      return FindResult.notFound(endIndex);
    }

    @Override
    public FindResult findDouble(int startIndex, int endIndex, double val)
    {
      return findLong(startIndex, endIndex, (long) val);
    }

    @Override
    public FindResult findFloat(int startIndex, int endIndex, float val)
    {
      return findLong(startIndex, endIndex, (long) val);
    }

    @Override
    public FindResult findLong(int startIndex, int endIndex, long val)
    {
      if (vals[startIndex] == val) {
        int end = startIndex + 1;

        while (end < endIndex && vals[end] == val) {
          ++end;
        }
        return FindResult.found(startIndex, end);
      }

      int i = Arrays.binarySearch(vals, startIndex, endIndex, val);
      if (i > 0) {
        int foundStart = i;
        int foundEnd = i + 1;

        while (foundStart - 1 >= startIndex && vals[foundStart - 1] == val) {
          --foundStart;
        }

        while (foundEnd < endIndex && vals[foundEnd] == val) {
          ++foundEnd;
        }

        return FindResult.found(foundStart, foundEnd);
      } else {
        return FindResult.notFound(-(i + 1));
      }
    }

    @SuppressWarnings("unused")
    public FindResult findInt(int startIndex, int endIndex, int val)
    {
      return findLong(startIndex, endIndex, val);
    }

    @Override
    public FindResult findString(int startIndex, int endIndex, String val)
    {
      throw NotYetImplemented.ex(null, "findString is not currently supported for LongArrayColumns");
    }

    @Override
    public FindResult findComplex(int startIndex, int endIndex, Object val)
    {
      throw NotYetImplemented.ex(null, "findComplex is not currently supported for LongArrayColumns");
    }
  }
}
