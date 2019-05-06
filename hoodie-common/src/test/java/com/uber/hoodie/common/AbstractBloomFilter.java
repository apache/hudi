package com.uber.hoodie.common;/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.util.UUID;

public abstract class AbstractBloomFilter {

  protected void testAddKey(boolean isDynamic) {
    BloomFilter filter = new BloomFilter(100, 0.0000001, isDynamic);
    filter.add("key1");
    assert (filter.mightContain("key1"));
  }

  protected int testOverloadBloom(boolean isDynamic) {
    BloomFilter filter = new BloomFilter(10, 0.0000001, isDynamic);
    for (int i = 0; i < 100; i++) {
      filter.add(UUID.randomUUID().toString());
    }
    int falsePositives = 0;
    for (int i = 0; i < 100; i++) {
      if (filter.mightContain(UUID.randomUUID().toString())) {
        falsePositives++;
      }
    }
    return falsePositives;
  }

  protected void testSerialize(boolean isDynamic) throws IOException, ClassNotFoundException {
    BloomFilter filter = new BloomFilter(1000, 0.0000001, isDynamic);
    filter.add("key1");
    filter.add("key2");
    String filterStr = filter.serializeToString();

    // Rebuild
    BloomFilter newFilter = new BloomFilter(filterStr, isDynamic);
    assert (newFilter.mightContain("key1"));
    assert (newFilter.mightContain("key2"));
  }
}
