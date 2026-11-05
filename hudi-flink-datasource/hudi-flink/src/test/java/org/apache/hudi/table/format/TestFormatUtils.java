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

package org.apache.hudi.table.format;

import org.apache.hudi.common.util.Option;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.util.TestConfigUtils.TEST_BOOLEAN_CONFIG_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link FormatUtils}
 */
public class TestFormatUtils {
  @Test
  public void testGetRawValueWithAltKeys() {
    Configuration flinkConf = new Configuration();
    assertEquals(Option.empty(),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue + "");
    assertEquals(Option.of(String.valueOf(setValue)),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    flinkConf = new Configuration();
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue + "");
    assertEquals(Option.of(String.valueOf(setValue)),
        FormatUtils.getRawValueWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }

  @Test
  public void testGetBooleanWithAltKeys() {
    Configuration flinkConf = new Configuration();
    assertEquals(Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue()),
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    boolean setValue = !Boolean.parseBoolean(TEST_BOOLEAN_CONFIG_PROPERTY.defaultValue());
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.key(), setValue + "");
    assertEquals(setValue,
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));

    flinkConf = new Configuration();
    flinkConf.setString(TEST_BOOLEAN_CONFIG_PROPERTY.getAlternatives().get(0), setValue + "");
    assertEquals(setValue,
        FormatUtils.getBooleanWithAltKeys(flinkConf, TEST_BOOLEAN_CONFIG_PROPERTY));
  }
}
