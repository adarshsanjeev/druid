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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;
import java.util.Objects;

public class DurableStorageMSQDestination implements MSQDestination
{
  public static final String TYPE = "durableStorage";

  private final ResultFormat resultFormat;

  @JsonCreator
  public DurableStorageMSQDestination(@JsonProperty("resultFormat") @Nullable ResultFormat resultFormat)
  {
    this.resultFormat = resultFormat == null ? ResultFormat.DEFAULT_RESULT_FORMAT : resultFormat;
  }

  @Override
  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
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
    DurableStorageMSQDestination that = (DurableStorageMSQDestination) o;
    return resultFormat == that.resultFormat;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(resultFormat);
  }

  @Override
  public String toString()
  {
    return "DurableStorageMSQDestination{" +
           "resultFormat=" + resultFormat +
           '}';
  }
}
