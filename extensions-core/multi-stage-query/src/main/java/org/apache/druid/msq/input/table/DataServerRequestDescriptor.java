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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.coordination.DruidServerMetadata;

import java.util.List;
import java.util.Objects;

/**
 * Contains information on a set of segments, and the {@link DruidServerMetadata} of a data server, serving
 * those segments.
 */
public class DataServerRequestDescriptor
{
  private final DruidServerMetadata serverMetadata;
  private final List<RichSegmentDescriptor> segments;

  @JsonCreator
  public DataServerRequestDescriptor(
      @JsonProperty("serverMetadata") DruidServerMetadata serverMetadata,
      @JsonProperty("segments") List<RichSegmentDescriptor> segments
  ) {
    this.segments = segments;
    this.serverMetadata = serverMetadata;
  }

  @JsonProperty("serverMetadata")
  public DruidServerMetadata getServerMetadata()
  {
    return serverMetadata;
  }

  @JsonProperty("segments")
  public List<RichSegmentDescriptor> getSegments()
  {
    return segments;
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
    DataServerRequestDescriptor that = (DataServerRequestDescriptor) o;
    return Objects.equals(serverMetadata, that.serverMetadata) && Objects.equals(
        segments,
        that.segments
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(serverMetadata, segments);
  }

  @Override
  public String toString()
  {
    return "DataServerRequestDescriptor{" +
           "serverMetadata=" + serverMetadata +
           ", segments=" + segments +
           '}';
  }
}
