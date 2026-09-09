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

package org.apache.hudi.table.action.commit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the list- and map-backed {@link SparkBucketInfoGetter} implementations.
 */
public class TestSparkBucketInfoGetter {

  private static BucketInfo bucket(String fileIdPrefix) {
    return new BucketInfo(BucketType.INSERT, fileIdPrefix, "partition");
  }

  @Test
  void listBasedGetterIndexesByPosition() {
    BucketInfo first = bucket("f0");
    BucketInfo second = bucket("f1");
    List<BucketInfo> bucketInfoList = Arrays.asList(first, second);
    ListBasedSparkBucketInfoGetter getter = new ListBasedSparkBucketInfoGetter(bucketInfoList);
    assertSame(first, getter.getBucketInfo(0));
    assertSame(second, getter.getBucketInfo(1));
  }

  @Test
  void listBasedGetterRejectsOutOfRangeIndex() {
    ListBasedSparkBucketInfoGetter getter =
        new ListBasedSparkBucketInfoGetter(Arrays.asList(bucket("f0")));
    assertThrows(IndexOutOfBoundsException.class, () -> getter.getBucketInfo(5));
  }

  @Test
  void mapBasedGetterLooksUpByBucketNumber() {
    Map<Integer, BucketInfo> bucketInfoMap = new HashMap<>();
    BucketInfo target = bucket("f7");
    bucketInfoMap.put(7, target);
    MapBasedSparkBucketInfoGetter getter = new MapBasedSparkBucketInfoGetter(bucketInfoMap);
    assertSame(target, getter.getBucketInfo(7));
    // Missing keys resolve to null rather than throwing.
    assertNull(getter.getBucketInfo(0));
  }
}
