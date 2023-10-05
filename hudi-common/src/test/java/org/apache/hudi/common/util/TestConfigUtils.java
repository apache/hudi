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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.ConfigProperty;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestConfigUtils {
  public static final ConfigProperty<String> TEST_BOOLEAN_CONFIG_PROPERTY = ConfigProperty
      .key("hoodie.test.boolean.config")
      .defaultValue("true")
      .withAlternatives("hudi.test.boolean.config")
      .markAdvanced()
      .withDocumentation("Testing boolean config.");

  @Test
  public void testToMapSucceeds() {
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("k.1.1.2", "v1");
    expectedMap.put("k.2.1.2", "v2");
    expectedMap.put("k.3.1.2", "v3");

    // Test base case
    String srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3";
    Map<String, String> outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test ends with new line
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3\n";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines with spaces in between
    srcKv = "k.1.1.2=v1\n  \nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test with random spaces if trim works properly
    srcKv = " k.1.1.2 =   v1\n k.2.1.2 = v2 \nk.3.1.2 = v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);
  }

  @Test
  public void testToMapThrowError() {
    String srcKv = "k.1.1.2=v1=v1.1\nk.2.1.2=v2\nk.3.1.2=v3";
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.toMap(srcKv));
  }

  @Test
  public void testGetRawValueWithAltKeysFromHadoopConf() {
    Configuration conf = new Configuration();
    assertEquals(Option.empty(), ConfigUtils.getRawValueWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    conf.setBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue);
    assertEquals(Option.of(String.valueOf(setValue)),
        ConfigUtils.getRawValueWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));

    conf = new Configuration();
    conf.setBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue);
    assertEquals(Option.of(String.valueOf(setValue)),
        ConfigUtils.getRawValueWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }

  @Test
  public void testGetBooleanWithAltKeysFromHadoopConf() {
    Configuration conf = new Configuration();
    assertEquals(Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue()),
        ConfigUtils.getBooleanWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    conf.setBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue);
    assertEquals(setValue,
        ConfigUtils.getBooleanWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));

    conf = new Configuration();
    conf.setBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue);
    assertEquals(setValue,
        ConfigUtils.getBooleanWithAltKeys(conf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }
}