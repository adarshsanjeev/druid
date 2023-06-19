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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IntervalLoadRule}
 */
public class IntervalLoadRuleTest
{
  @Test
  public void testSerde() throws Exception
  {
    IntervalLoadRule rule = new IntervalLoadRule(
        Intervals.of("0/3000"),
        ImmutableMap.of(DruidServer.DEFAULT_TIER, 2)
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule, reread);
  }

  @Test
  public void testSerdeNullTieredReplicants() throws Exception
  {
    IntervalLoadRule rule = new IntervalLoadRule(
        Intervals.of("0/3000"), null, true
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule, reread);
    Assert.assertEquals(ImmutableMap.of(), rule.getTieredReplicants());
  }

  @Test(expected = IAE.class)
  public void testCreatingNegativeTieredReplicants()
  {
    new IntervalLoadRule(Intervals.of("0/3000"), ImmutableMap.of(DruidServer.DEFAULT_TIER, -1));
  }

  @Test(expected = IAE.class)
  public void testEmptyReplicantValue() throws Exception
  {
    // Immutable map does not allow null values
    Map<String, Integer> tieredReplicants = new HashMap<>();
    tieredReplicants.put("tier", null);
    IntervalLoadRule rule = new IntervalLoadRule(
        Intervals.of("0/3000"),
        tieredReplicants
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);
  }

  @Test
  public void testWithoutAllowEmptyReplicantShouldCreateDefaultTier() throws Exception
  {
    String inputJson = "     {\n"
                       + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                       + "      \"type\": \"loadByInterval\"\n"
                       + "   }";
    String expectedJson = "     {\n"
                          + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                          + "      \"tieredReplicants\": {\n"
                          + "          \"" + DruidServer.DEFAULT_TIER + "\": " + DruidServer.DEFAULT_NUM_REPLICANTS + "\n"
                          + "      },\n"
                          + "      \"type\": \"loadByInterval\"\n"
                          + "   }";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    IntervalLoadRule inputForeverLoadRule = jsonMapper.readValue(inputJson, IntervalLoadRule.class);
    IntervalLoadRule expectedForeverLoadRule = jsonMapper.readValue(expectedJson, IntervalLoadRule.class);
    Assert.assertEquals(expectedForeverLoadRule.getTieredReplicants(), inputForeverLoadRule.getTieredReplicants());
  }

  @Test
  public void testWithAllowEmptyReplicantShouldDefaultToEmpty() throws Exception
  {
    String inputJson = "     {\n"
                       + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                       + "      \"type\": \"loadByInterval\",\n"
                       + "      \"allowEmptyTieredReplicants\": \"true\"\n"
                       + "   }";
    String expectedJson = "     {\n"
                          + "      \"interval\": \"0000-01-01T00:00:00.000-05:50:36/3000-01-01T00:00:00.000-06:00\",\n"
                          + "      \"tieredReplicants\": {},\n"
                          + "      \"type\": \"loadByInterval\"\n"
                          + "   }";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    IntervalLoadRule inputForeverLoadRule = jsonMapper.readValue(inputJson, IntervalLoadRule.class);
    IntervalLoadRule expectedForeverLoadRule = jsonMapper.readValue(expectedJson, IntervalLoadRule.class);
    Assert.assertEquals(expectedForeverLoadRule.getTieredReplicants(), inputForeverLoadRule.getTieredReplicants());
  }
}
