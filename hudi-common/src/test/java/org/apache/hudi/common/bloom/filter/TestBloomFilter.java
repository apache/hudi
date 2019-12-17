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

package org.apache.hudi.common.bloom.filter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests {@link SimpleBloomFilter} and {@link HoodieDynamicBoundedBloomFilter}.
 */
@RunWith(Parameterized.class)
public class TestBloomFilter {

  private final String versionToTest;

  // name attribute is optional, provide an unique name for test
  // multiple parameters, uses Collection<Object[]>
  @Parameters()
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {BloomFilterTypeCode.SIMPLE.name()},
        {BloomFilterTypeCode.DYNAMIC_V0.name()}
    });
  }

  public TestBloomFilter(String versionToTest) {
    this.versionToTest = versionToTest;
  }

  @Test
  public void testAddKey() {
    List<String> inputs = new ArrayList<>();
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(versionToTest, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }
      for (java.lang.String key : inputs) {
        Assert.assertTrue("Filter should have returned true for " + key, filter.mightContain(key));
      }
      for (int i = 0; i < 100; i++) {
        String randomKey = UUID.randomUUID().toString();
        if (inputs.contains(randomKey)) {
          Assert.assertTrue("Filter should have returned true for " + randomKey, filter.mightContain(randomKey));
        }
      }
    }
  }

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {

    List<String> inputs = new ArrayList<>();
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(versionToTest, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }

      String serString = filter.serializeToString();
      BloomFilter recreatedBloomFilter = BloomFilterFactory
          .fromString(serString, versionToTest);
      for (String key : inputs) {
        Assert.assertTrue("Filter should have returned true for " + key, recreatedBloomFilter.mightContain(key));
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
