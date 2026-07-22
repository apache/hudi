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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieKeyException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartitionPathEncodeUtils {

  @ParameterizedTest
  @ValueSource(strings = {
      "..",
      "../foo",
      "foo/..",
      "foo/../bar",
      "../../../../tmp/evil",
      "a/../../b",
      "..\\foo",
      "foo\\..\\bar"
  })
  void detectsPathTraversal(String partitionPath) {
    assertTrue(PartitionPathEncodeUtils.hasPathTraversal(partitionPath),
        "Expected traversal to be detected for: " + partitionPath);
    assertThrows(HoodieKeyException.class,
        () -> PartitionPathEncodeUtils.validateNoPathTraversal(partitionPath));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "2024/01/01",
      "2024-01-01",
      "region=us-west-2",
      "year=2024/month=01",
      "rider=..",
      "rider=../evil",
      "..foo",
      "foo..",
      "foo..bar",
      "a.b.c",
      "...",
      "....",
      "a/.hidden/b",
      "."
  })
  void allowsLegitimatePartitionPaths(String partitionPath) {
    assertFalse(PartitionPathEncodeUtils.hasPathTraversal(partitionPath),
        "Expected no traversal for: " + partitionPath);
    // validate should return the same value unchanged.
    assertEquals(partitionPath, PartitionPathEncodeUtils.validateNoPathTraversal(partitionPath));
  }

  @Test
  void nullAndEmptyAreSafe() {
    assertFalse(PartitionPathEncodeUtils.hasPathTraversal(null));
    assertFalse(PartitionPathEncodeUtils.hasPathTraversal(""));
    assertEquals(null, PartitionPathEncodeUtils.validateNoPathTraversal(null));
    assertEquals("", PartitionPathEncodeUtils.validateNoPathTraversal(""));
  }
}
