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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link BucketType} and the {@link BucketInfo} value holder that carries it.
 */
public class TestBucketTypeAndInfo {

  @Test
  void bucketTypeHasUpdateAndInsert() {
    assertEquals(2, BucketType.values().length);
    assertSame(BucketType.UPDATE, BucketType.valueOf("UPDATE"));
    assertSame(BucketType.INSERT, BucketType.valueOf("INSERT"));
  }

  @Test
  void bucketInfoGettersReturnConstructorArgs() {
    BucketInfo info = new BucketInfo(BucketType.INSERT, "fileId-1", "2024/01/01");
    assertSame(BucketType.INSERT, info.getBucketType());
    assertEquals("fileId-1", info.getFileIdPrefix());
    assertEquals("2024/01/01", info.getPartitionPath());
  }

  @Test
  void bucketInfoEqualsAndHashCodeUseAllFields() {
    BucketInfo a = new BucketInfo(BucketType.UPDATE, "f1", "p1");
    BucketInfo same = new BucketInfo(BucketType.UPDATE, "f1", "p1");
    BucketInfo differentType = new BucketInfo(BucketType.INSERT, "f1", "p1");
    BucketInfo differentFile = new BucketInfo(BucketType.UPDATE, "f2", "p1");
    BucketInfo differentPartition = new BucketInfo(BucketType.UPDATE, "f1", "p2");

    assertEquals(a, same);
    assertEquals(a.hashCode(), same.hashCode());
    assertNotEquals(a, differentType);
    assertNotEquals(a, differentFile);
    assertNotEquals(a, differentPartition);
    assertNotEquals(a, null);
    assertNotEquals(a, "not a bucket info");
  }

  @Test
  void bucketInfoToStringContainsFields() {
    String rendered = new BucketInfo(BucketType.INSERT, "fileId-9", "part-9").toString();
    assertTrue(rendered.contains("INSERT"));
    assertTrue(rendered.contains("fileId-9"));
    assertTrue(rendered.contains("part-9"));
  }
}
