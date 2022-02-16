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

package org.apache.hudi.common.config;

import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConfigProperty extends HoodieConfig {

  public static ConfigProperty<String> FAKE_STRING_CONFIG = ConfigProperty
      .key("test.fake.string.config")
      .defaultValue("1")
      .withAlternatives("test.fake.string.alternative.config")
      .withDocumentation("Fake config only for testing");

  public static ConfigProperty<String> FAKE_BOOLEAN_CONFIG = ConfigProperty
      .key("test.fake.boolean.config")
      .defaultValue("false")
      .withDocumentation("Fake config only for testing");

  public static ConfigProperty<String> FAKE_BOOLEAN_CONFIG_NO_DEFAULT = ConfigProperty
      .key("test.fake.boolean.config")
      .noDefaultValue()
      .withDocumentation("Fake config only for testing");

  public static ConfigProperty<Integer> FAKE_INTEGER_CONFIG = ConfigProperty
      .key("test.fake.integer.config")
      .defaultValue(0)
      .withInferFunction(p -> {
        if (p.contains(FAKE_STRING_CONFIG) && p.getString(FAKE_STRING_CONFIG).equals("5")) {
          return Option.of(100);
        }
        return Option.empty();
      })
      .withDocumentation("Fake config only for testing");

  @Test
  public void testGetTypedValue() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    assertNull(hoodieConfig.getInt(FAKE_STRING_CONFIG));
    hoodieConfig.setValue(FAKE_STRING_CONFIG, "5");
    assertEquals(5, hoodieConfig.getInt(FAKE_STRING_CONFIG));

    assertEquals(false, hoodieConfig.getBoolean(FAKE_BOOLEAN_CONFIG));
    hoodieConfig.setValue(FAKE_BOOLEAN_CONFIG, "true");
    assertEquals(true, hoodieConfig.getBoolean(FAKE_BOOLEAN_CONFIG));
  }

  @Test
  public void testGetBooleanShouldReturnFalseWhenDefaultValueFalseButNotSet() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    assertEquals(false, hoodieConfig.getBoolean(FAKE_BOOLEAN_CONFIG));
  }

  @Test
  public void testGetBooleanShouldReturnNullWhenNoDefaultValuePresent() {
    HoodieConfig hoodieConfig = new HoodieConfig();
    assertNull(hoodieConfig.getBoolean(FAKE_BOOLEAN_CONFIG_NO_DEFAULT));
  }

  @Test
  public void testGetOrDefault() {
    Properties props = new Properties();
    props.put("test.unknown.config", "abc");
    HoodieConfig hoodieConfig = new HoodieConfig(props);
    assertEquals("1", hoodieConfig.getStringOrDefault(FAKE_STRING_CONFIG));
    assertEquals("2", hoodieConfig.getStringOrDefault(FAKE_STRING_CONFIG, "2"));
  }

  @Test
  public void testAlternatives() {
    Properties props = new Properties();
    props.put("test.fake.string.alternative.config", "1");
    HoodieConfig hoodieConfig = new HoodieConfig(props);
    assertTrue(hoodieConfig.contains(FAKE_STRING_CONFIG));
    assertEquals("1", hoodieConfig.getString(FAKE_STRING_CONFIG));
  }

  @Test
  public void testInference() {
    HoodieConfig hoodieConfig1 = new HoodieConfig();
    hoodieConfig1.setDefaultValue(FAKE_INTEGER_CONFIG);
    assertEquals(0, hoodieConfig1.getInt(FAKE_INTEGER_CONFIG));

    HoodieConfig hoodieConfig2 = new HoodieConfig();
    hoodieConfig2.setValue(FAKE_STRING_CONFIG, "5");
    hoodieConfig2.setDefaultValue(FAKE_INTEGER_CONFIG);
    assertEquals(100, hoodieConfig2.getInt(FAKE_INTEGER_CONFIG));
  }

  @Test
  public void testSetDefaults() {
    setDefaults(this.getClass().getName());
    assertEquals(3, getProps().size());
  }
}
