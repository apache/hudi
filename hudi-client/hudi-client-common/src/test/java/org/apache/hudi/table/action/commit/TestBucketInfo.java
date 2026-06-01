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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TestBucketInfo {

  @Test
  void testThreeArgConstructorDefaultsNumUpdatesToUnknown() {
    BucketInfo bucket = new BucketInfo(BucketType.UPDATE, "file1", "partition1");
    assertEquals(BucketType.UPDATE, bucket.getBucketType());
    assertEquals("file1", bucket.getFileIdPrefix());
    assertEquals("partition1", bucket.getPartitionPath());
    assertEquals(-1L, bucket.getNumUpdates());
  }

  @Test
  void testFourArgConstructorSetsNumUpdates() {
    BucketInfo bucket = new BucketInfo(BucketType.UPDATE, "file1", "partition1", 42L);
    assertEquals(BucketType.UPDATE, bucket.getBucketType());
    assertEquals("file1", bucket.getFileIdPrefix());
    assertEquals("partition1", bucket.getPartitionPath());
    assertEquals(42L, bucket.getNumUpdates());
  }

  @Test
  void testFourArgConstructorWithZeroNumUpdates() {
    BucketInfo bucket = new BucketInfo(BucketType.INSERT, "file2", "partition2", 0L);
    assertEquals(0L, bucket.getNumUpdates());
  }

  @Test
  void testInsertBucketDefaultNumUpdates() {
    BucketInfo bucket = new BucketInfo(BucketType.INSERT, "file1", "partition1");
    assertEquals(BucketType.INSERT, bucket.getBucketType());
    assertEquals(-1L, bucket.getNumUpdates());
  }

  @Test
  void testEqualsIgnoresNumUpdates() {
    BucketInfo bucket1 = new BucketInfo(BucketType.UPDATE, "file1", "partition1", 10L);
    BucketInfo bucket2 = new BucketInfo(BucketType.UPDATE, "file1", "partition1", 20L);
    assertEquals(bucket1, bucket2);
    assertEquals(bucket1.hashCode(), bucket2.hashCode());
  }

  @Test
  void testNotEqualsDifferentType() {
    BucketInfo bucket1 = new BucketInfo(BucketType.UPDATE, "file1", "partition1", 10L);
    BucketInfo bucket2 = new BucketInfo(BucketType.INSERT, "file1", "partition1", 10L);
    assertNotEquals(bucket1, bucket2);
  }

  @Test
  void testToStringIncludesNumUpdates() {
    BucketInfo bucket = new BucketInfo(BucketType.UPDATE, "file1", "partition1", 42L);
    String str = bucket.toString();
    assertEquals("BucketInfo {bucketType=UPDATE, fileIdPrefix=file1, partitionPath=partition1, numUpdates=42}", str);
  }
}