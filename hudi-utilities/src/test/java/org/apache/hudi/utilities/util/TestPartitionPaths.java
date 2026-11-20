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

package org.apache.hudi.utilities.util;

import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPartitionPaths {

  private static final StoragePath BASE = new StoragePath("file:/tmp/table");

  @Test
  void relativeKeyIsReturnedAsItStands() {
    assertEquals("2024/01/01", PartitionPaths.relative(BASE, "2024/01/01"));
  }

  @Test
  void anAbsoluteKeyIsMadeRelativeToTheTable() {
    assertEquals("2024/01/01", PartitionPaths.relative(BASE, "file:/tmp/table/2024/01/01"));
  }

  @Test
  void schemeAndAuthorityAreIgnored() {
    assertEquals("2024/01/01", PartitionPaths.relative(BASE, "hdfs://nn:8020/tmp/table/2024/01/01"));
  }

  @Test
  void leadingAndTrailingSeparatorsAreTrimmed() {
    assertEquals("2024/01/01", PartitionPaths.relative(BASE, "/2024/01/01/"));
  }

  @Test
  void theNonPartitionedTablesPartitionStaysEmpty() {
    assertEquals("", PartitionPaths.relative(BASE, ""));
    assertEquals("", PartitionPaths.relative(BASE, null));
  }

  @Test
  void siblingTableIsNotTreatedAsAChild() {
    // "/tmp/table" is not a parent of "/tmp/table2"; substring arithmetic on that pair produces a
    // plausible-looking wrong answer rather than an error.
    assertEquals("tmp/table2/2024", PartitionPaths.relative(BASE, "file:/tmp/table2/2024"));
  }
}
