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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConfigUtils {
  public static final ConfigProperty<String> TEST_BOOLEAN_CONFIG_PROPERTY = ConfigProperty
      .key("hoodie.test.boolean.config")
      .defaultValue("true")
      .withAlternatives("hudi.test.boolean.config")
      .markAdvanced()
      .withDocumentation("Testing boolean config.");

  private static Stream<Arguments> separatorArgs() {
    List<Option<String>> separatorList = new ArrayList<>();
    separatorList.add(Option.empty());
    separatorList.add(Option.of("\n"));
    separatorList.add(Option.of(","));
    return separatorList.stream().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("separatorArgs")
  public void testToMapSucceeds(Option<String> separator) {
    String sepString = separator.isPresent() ? separator.get() : "\n";
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("k.1.1.2", "v1");
    expectedMap.put("k.2.1.2", "v2");
    expectedMap.put("k.3.1.2", "v3");

    // Test base case
    String srcKv = String.format(
        "k.1.1.2=v1%sk.2.1.2=v2%sk.3.1.2=v3", sepString, sepString);
    Map<String, String> outMap = toMap(srcKv, separator);
    assertEquals(expectedMap, outMap);

    // Test ends with new line
    srcKv = String.format(
        "k.1.1.2=v1%sk.2.1.2=v2%sk.3.1.2=v3%s", sepString, sepString, sepString);
    outMap = toMap(srcKv, separator);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines
    srcKv = String.format(
        "k.1.1.2=v1%sk.2.1.2=v2%s%sk.3.1.2=v3", sepString, sepString, sepString);
    outMap = toMap(srcKv, separator);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines with spaces in between
    srcKv = String.format(
        "k.1.1.2=v1%s  %sk.2.1.2=v2%s%sk.3.1.2=v3", sepString, sepString, sepString, sepString);
    outMap = toMap(srcKv, separator);
    assertEquals(expectedMap, outMap);

    // Test with random spaces if trim works properly
    srcKv = String.format(
        " k.1.1.2 =   v1%s k.2.1.2 = v2 %sk.3.1.2 = v3", sepString, sepString);
    outMap = toMap(srcKv, separator);
    assertEquals(expectedMap, outMap);
  }

  @Test
  void testGetRawValueWithAltKeys() {
    TypedProperties properties = new TypedProperties();
    DiskMapType diskMapType = ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, true);
    Assertions.assertEquals(DiskMapType.BITCASK, diskMapType);
    properties.put(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), DiskMapType.ROCKS_DB);
    diskMapType = ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, true);
    Assertions.assertEquals(DiskMapType.ROCKS_DB, diskMapType);
    properties.remove(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key());
    Assertions.assertThrows(IllegalArgumentException.class, () -> ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, false));
  }

  @ParameterizedTest
  @MethodSource("separatorArgs")
  public void testToMapThrowError(Option<String> separator) {
    String sepString = separator.isPresent() ? separator.get() : "\n";
    String srcKv = String.format(
        "k.1.1.2=v1=v1.1%sk.2.1.2=v2%sk.3.1.2=v3", sepString, sepString);
    assertThrows(IllegalArgumentException.class, () -> toMap(srcKv, separator));
  }

  private Map<String, String> toMap(String config, Option<String> separator) {
    if (separator.isEmpty()) {
      return ConfigUtils.toMap(config);
    }
    return ConfigUtils.toMap(config, separator.get());
  }

  @Test
  void testParseValidProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "Ki", "Vi");
    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(1, result.size());
    assertEquals("Vi", result.get("Ki"));
  }

  @Test
  void testMissingKeyReturnsEmptyMap() {
    TypedProperties props = new TypedProperties(); // no property set
    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertTrue(result.isEmpty());
  }

  @Test
  void testMultipleValidProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key1", "value1");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key2", "value2");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key3", "value3");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(3, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  void testPropertiesWithDifferentPrefixes() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "mergeKey", "mergeValue");
    props.setProperty("other.prefix.key", "otherValue");
    props.setProperty("hoodie.merge.custom.property.prefix", "directPrefixValue");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(1, result.size());
    assertEquals("mergeValue", result.get("mergeKey"));
  }

  @Test
  void testPropertiesWithEmptyValues() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "emptyKey", "");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(1, result.size());
    assertEquals("", result.get("emptyKey"));
  }

  @Test
  void testPropertiesWithSpecialCharacters() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key.with.dots", "value.with.dots");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key_with_underscores", "value_with_underscores");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key-with-dashes", "value-with-dashes");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(3, result.size());
    assertEquals("value.with.dots", result.get("key.with.dots"));
    assertEquals("value_with_underscores", result.get("key_with_underscores"));
    assertEquals("value-with-dashes", result.get("key-with-dashes"));
  }

  @Test
  void testPropertiesWithNumericValues() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "intKey", "123");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "doubleKey", "123.45");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "booleanKey", "true");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(3, result.size());
    assertEquals("123", result.get("intKey"));
    assertEquals("123.45", result.get("doubleKey"));
    assertEquals("true", result.get("booleanKey"));
  }

  @Test
  void testPropertiesWithWhitespace() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "  spacedKey  ", "  spacedValue  ");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(1, result.size());
    assertEquals("spacedValue", result.get("spacedKey")); // Values should be trimmed
  }

  @Test
  void testPropertiesWithWhitespaceInKeysAndValues() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "  keyWithSpaces  ", "  valueWithSpaces  ");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "keyWithoutSpaces", "valueWithoutSpaces");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(2, result.size());
    assertEquals("valueWithSpaces", result.get("keyWithSpaces")); // Both key and value should be trimmed
    assertEquals("valueWithoutSpaces", result.get("keyWithoutSpaces"));
  }

  @Test
  void testPropertiesWithExactPrefixMatch() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX, "exactPrefixValue");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(0, result.size()); // Exact prefix match should not be included as it has no suffix
  }

  @Test
  void testPropertiesWithPrefixFollowedByDot() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX, "valueAfterDot");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(0, result.size()); // Empty key after trimming should be filtered out
  }

  @Test
  void testPropertiesWithWhitespaceOnlyKeys() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "   ", "valueForWhitespaceKey");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "  \t  \n  ", "valueForTabNewlineKey");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(0, result.size());
  }

  @Test
  void testPropertiesWithNullKeys() {
    TypedProperties props = new TypedProperties();
    // Note: TypedProperties doesn't allow null keys, but we test the edge case
    props.setProperty(MERGE_PROPERTIES_PREFIX, "valueForNullKey");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(0, result.size()); // Empty key should be filtered out
  }

  @Test
  void testPropertiesWithMixedValidAndInvalidKeys() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "validKey", "validValue");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "   ", "invalidValue1");
    props.setProperty(MERGE_PROPERTIES_PREFIX, "invalidValue2");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "anotherValidKey", "anotherValidValue");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(2, result.size()); // Only valid keys should be included
    assertEquals("validValue", result.get("validKey"));
    assertEquals("anotherValidValue", result.get("anotherValidKey"));
  }

  @Test
  void testPropertiesWithCaseSensitivity() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "Key1", "Value1");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "key1", "value1");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(2, result.size());
    assertEquals("Value1", result.get("Key1"));
    assertEquals("value1", result.get("key1"));
  }

  @Test
  void testPropertiesWithLeadingAndTrailingWhitespace() {
    TypedProperties props = new TypedProperties();
    props.setProperty(MERGE_PROPERTIES_PREFIX + "  leadingSpaceKey", "trailingSpaceValue  ");
    props.setProperty(MERGE_PROPERTIES_PREFIX + "trailingSpaceKey  ", "  leadingSpaceValue");

    Map<String, String> result = ConfigUtils.extractWithPrefix(props, MERGE_PROPERTIES_PREFIX);
    assertEquals(2, result.size());
    assertEquals("trailingSpaceValue", result.get("leadingSpaceKey")); // Trimmed
    assertEquals("leadingSpaceValue", result.get("trailingSpaceKey")); // Trimmed
  }

  @Test
  void testNullProperties() {
    Map<String, String> result = ConfigUtils.extractWithPrefix(null, MERGE_PROPERTIES_PREFIX);
    assertTrue(result.isEmpty());
  }
}