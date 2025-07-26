/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKafkaSource {

  @Test
  public void testFilterKafkaParameters() {
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("custom1.config.streamer", "offer");
    kafkaParams.put("boostrap.servers", "dns:port");
    kafkaParams.put("custom2.config.capture", "s3://folder1");
    kafkaParams.put("custom1config.sourceprofile.refresh.mode", "ENABLED");

    // Case 1: No prefixes are configured.
    assertEquals(kafkaParams, KafkaSource.filterKafkaParameters(kafkaParams, ""));
    // Case 2: Only prefix matching is done, sub strings within the config key are ignored and config is passed down to kafka consumer.
    assertEquals(kafkaParams, KafkaSource.filterKafkaParameters(kafkaParams, "config.,port"));
    // Case 3: There are no configs with the given prefixes.
    assertEquals(kafkaParams, KafkaSource.filterKafkaParameters(kafkaParams, "custom3,custom4"));
    // Case 4: Ensure only the appropriate configs are filtered out.
    Map<String, Object> filteredParams = KafkaSource.filterKafkaParameters(kafkaParams, "custom1,custom2");
    Map<String, Object> expectedParams = new HashMap<>();
    expectedParams.put("boostrap.servers", "dns:port");
    assertEquals(expectedParams, filteredParams);
  }
}
