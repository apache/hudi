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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}