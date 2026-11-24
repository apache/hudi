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

package org.apache.hudi.index;

import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TestHoodieInMemoryHashIndex {

  @Test
  void testTableKeyTreatsSchemeLessPathAsLocal() {
    String expected = HoodieInMemoryHashIndex.tableKey(new StoragePath("file:/tmp/t"));
    assertEquals(expected, HoodieInMemoryHashIndex.tableKey(new StoragePath("/tmp/t")));
    assertEquals(expected, HoodieInMemoryHashIndex.tableKey(new StoragePath(URI.create("file:///tmp/t"))));
  }

  @Test
  void testTableKeyKeepsSchemeAndAuthority() {
    assertNotEquals(HoodieInMemoryHashIndex.tableKey(new StoragePath("s3a://bucket-a/t")),
        HoodieInMemoryHashIndex.tableKey(new StoragePath("s3a://bucket-b/t")));
    assertNotEquals(HoodieInMemoryHashIndex.tableKey(new StoragePath("s3a://bucket-a/t")),
        HoodieInMemoryHashIndex.tableKey(new StoragePath("/t")));
  }
}
