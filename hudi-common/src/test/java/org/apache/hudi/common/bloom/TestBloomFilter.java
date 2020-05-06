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

package org.apache.hudi.common.bloom;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link SimpleBloomFilter} and {@link HoodieDynamicBoundedBloomFilter}.
 */
public class TestBloomFilter {

  // name attribute is optional, provide a unique name for test
  // multiple parameters, uses Collection<Object[]>
  public static List<Arguments> bloomFilterTypeCodes() {
    return Arrays.asList(
        Arguments.of(BloomFilterTypeCode.SIMPLE.name()),
        Arguments.of(BloomFilterTypeCode.DYNAMIC_V0.name())
    );
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testAddKey(String typeCode) {
    List<String> inputs;
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(typeCode, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }
      for (java.lang.String key : inputs) {
        assertTrue(filter.mightContain(key), "Filter should have returned true for " + key);
      }
      for (int i = 0; i < 100; i++) {
        String randomKey = UUID.randomUUID().toString();
        if (inputs.contains(randomKey)) {
          assertTrue(filter.mightContain(randomKey), "Filter should have returned true for " + randomKey);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testSerialize(String typeCode) {

    List<String> inputs;
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(typeCode, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }

      String serString = filter.serializeToString();
      BloomFilter recreatedBloomFilter = BloomFilterFactory
          .fromString(serString, typeCode);
      for (String key : inputs) {
        assertTrue(recreatedBloomFilter.mightContain(key), "Filter should have returned true for " + key);
      }
    }
  }

  BloomFilter getBloomFilter(String typeCode, int numEntries, double errorRate, int maxEntries) {
    if (typeCode.equalsIgnoreCase(BloomFilterTypeCode.SIMPLE.name())) {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, -1, typeCode);
    } else {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, maxEntries, typeCode);
    }
  }
}
