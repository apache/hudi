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

package org.apache.hudi.keygen;

import org.apache.hudi.exception.HoodieKeyException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestKeyGenerator {
  private static Stream<Arguments> testKeyConstruction() {
    return Stream.of(
        Arguments.of(new String[] {"key1"}, Collections.singletonList("value1"), "key1:value1"),
        Arguments.of(new String[]{"key1", "key2"}, Arrays.asList("value1", "value2"), "key1:value1,key2:value2"),
        Arguments.of(new String[]{"key1", "key2"}, Arrays.asList("value1", ""), "key1:value1,key2:__empty__"),
        Arguments.of(new String[]{"key1", "key2"}, Arrays.asList(null, "value2"), "key1:__null__,key2:value2"));
  }

  @ParameterizedTest
  @MethodSource
  void testKeyConstruction(String[] keys, List<String> values, String expected) {
    assertEquals(expected, KeyGenerator.constructRecordKey(keys, (key, index) -> values.get(index)));
  }

  @Test
  void testKeyConstructionWithOnlyNulls() {
    assertThrows(HoodieKeyException.class, () -> KeyGenerator.constructRecordKey(new String[]{"key1"}, (key, index) -> null));
    assertThrows(HoodieKeyException.class, () -> KeyGenerator.constructRecordKey(new String[]{"key1", "key2"}, (key, index) -> null));
  }
}
