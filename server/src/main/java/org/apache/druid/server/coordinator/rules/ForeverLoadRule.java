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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ForeverLoadRule extends LoadRule
{
  private final Map<String, Integer> tieredReplicants;
  private final boolean useDefaultTierForNull;

  @JsonCreator
  public ForeverLoadRule(
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants,
      @JsonProperty("useDefaultTierForNull") @Nullable Boolean useDefaultTierForNull
  )
  {
    this.useDefaultTierForNull = Configs.valueOrDefault(useDefaultTierForNull, true);
    this.tieredReplicants = createTieredReplicants(tieredReplicants, this.useDefaultTierForNull);
    validateTieredReplicants(this.tieredReplicants, this.useDefaultTierForNull);
  }

  @JsonProperty("useDefaultTierForNull")
  public boolean useDefaultTierForNull()
  {
    return useDefaultTierForNull;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadForever";
  }

  @Override
  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @Override
  public int getNumReplicants(String tier)
  {
    Integer retVal = tieredReplicants.get(tier);
    return (retVal == null) ? 0 : retVal;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return true;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return true;
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
    ForeverLoadRule that = (ForeverLoadRule) o;
    return Objects.equals(tieredReplicants, that.tieredReplicants);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tieredReplicants);
  }
}
