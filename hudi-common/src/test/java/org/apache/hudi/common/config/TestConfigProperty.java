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

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ConfigProperty}.
 */
public class TestConfigProperty extends HoodieConfig {

  public static ConfigProperty<String> FAKE_STRING_CONFIG = ConfigProperty
      .key("test.fake.string.config")
      .defaultValue("1")
      .supportedVersions("a.b.c", "d.e.f")
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

  public static ConfigProperty<String> FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER = ConfigProperty
      .key("test.fake.string.config.no_default_with_infer")
      .noDefaultValue()
      .withInferFunction(p -> {
        if (p.getStringOrDefault(FAKE_STRING_CONFIG).equals("value1")) {
          return Option.of("value2");
        }
        return Option.of("value3");
      })
      .withDocumentation("Fake config with infer function and without default only for testing");

  public static ConfigProperty<String> FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY = ConfigProperty
      .key("test.fake.string.config.no_default_with_infer_empty")
      .noDefaultValue()
      .withInferFunction(p -> {
        if (p.getStringOrDefault(FAKE_STRING_CONFIG).equals("value1")) {
          return Option.of("value10");
        }
        return Option.empty();
      })
      .withDocumentation("Fake config with infer function that ca return empty value "
          + "and without default only for testing");

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

    HoodieConfig hoodieConfig3 = new HoodieConfig();
    hoodieConfig3.setValue(FAKE_STRING_CONFIG, "value1");
    hoodieConfig3.setDefaultValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER);
    hoodieConfig3.setDefaultValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY);
    assertEquals("value2", hoodieConfig3.getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER));
    assertEquals("value10", hoodieConfig3.getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY));

    HoodieConfig hoodieConfig4 = new HoodieConfig();
    hoodieConfig4.setValue(FAKE_STRING_CONFIG, "other");
    hoodieConfig4.setDefaultValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER);
    assertEquals("value3", hoodieConfig4.getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER));
    assertEquals(null, hoodieConfig4.getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY));

    HoodieConfig hoodieConfig5 = new HoodieConfig();
    hoodieConfig5.setValue(FAKE_STRING_CONFIG, "other");
    hoodieConfig5.setValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, "value4");
    hoodieConfig5.setDefaultValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER);
    assertEquals("value4", hoodieConfig5.getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER));
  }

  @Test
  public void testSetDefaults() {
    setDefaults(this.getClass().getName());
    assertEquals(4, getProps().size());
  }

  @Test
  public void testAdvancedValue() {
    assertFalse(FAKE_BOOLEAN_CONFIG.isAdvanced());
    assertFalse(FAKE_BOOLEAN_CONFIG_NO_DEFAULT.isAdvanced());

    assertTrue(FAKE_BOOLEAN_CONFIG.markAdvanced().isAdvanced());
    assertTrue(FAKE_BOOLEAN_CONFIG_NO_DEFAULT.markAdvanced().isAdvanced());
  }

  @EnumDescription("Test enum description.")
  public enum TestEnum {
    @EnumFieldDescription("Test val a")
    TEST_VAL_A,

    @EnumFieldDescription("Test val b")
    TEST_VAL_B,

    @EnumFieldDescription("Other val")
    OTHER_VAL
  }

  @Test
  void testEnumConfigs() {
    ConfigProperty<String> testConfig = ConfigProperty.key("test.config")
        .defaultValue(TestEnum.TEST_VAL_B.name())
        .withDocumentation(TestEnum.class);
    String[] lines = testConfig.doc().split("\n");
    assertEquals("org.apache.hudi.common.config.TestConfigProperty$TestEnum: Test enum description.", lines[0]);
    assertEquals("    TEST_VAL_A: Test val a", lines[1]);
    assertEquals("    TEST_VAL_B(default): Test val b", lines[2]);
    assertEquals("    OTHER_VAL: Other val", lines[3]);
  }

  @Test
  void testGetSupportedVersions() {
    assertEquals(
        Arrays.stream(new String[] {"a.b.c", "d.e.f"}).collect(Collectors.toList()),
        FAKE_STRING_CONFIG.getSupportedVersions());
    assertEquals(Collections.EMPTY_LIST, FAKE_INTEGER_CONFIG.getSupportedVersions());
  }
}
