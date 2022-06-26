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

package org.apache.hudi.sync.common.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestDefaultPartitionValueExtractor {

  @Test
  void testNonPartition() {
    PartitionValueExtractor extractor = new DefaultPartitionValueExtractor(Collections.emptyList());
    assertEquals(Collections.emptyList(), extractor.extractPartitionValuesInPath(null));
  }

  @Test
  void testNonEmptyPartitionsParsingWithIllegalPartitionPath() {
    PartitionValueExtractor extractor = new DefaultPartitionValueExtractor(Arrays.asList("foo", "bar"));
    assertThrows(IllegalArgumentException.class, () -> extractor.extractPartitionValuesInPath(""));
    assertThrows(IllegalArgumentException.class, () -> extractor.extractPartitionValuesInPath(null));
  }

  @ParameterizedTest
  @CsvSource(value = {
      "a:a:1",
      "a,b:a/b:2",
      "a,b,c:a/b/c:3",
      "a:0=a:1",
      "a,b:0=a/1=b:2",
      "a,b,c:0=a/1=b/2=c:3"}, delimiter = ':')
  void testMultiPartPartitions(String expected, String partitionPath, String depthStr) {
    int depth = Integer.parseInt(depthStr);
    List<String> partitionFields = IntStream.range(0, depth).mapToObj(String::valueOf).collect(Collectors.toList());
    PartitionValueExtractor extractor = new DefaultPartitionValueExtractor(partitionFields);
    List<String> partitionValues = extractor.extractPartitionValuesInPath(partitionPath);
    List<String> expectedPartitionValues = Arrays.asList(expected.split(","));
    assertEquals(expectedPartitionValues, partitionValues);
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true:0=a/b:2",
      "false:a/1=b/2=c:3"}, delimiter = ':')
  void testInconsistentHiveStylePartitions(boolean expectedHiveStyle, String partitionPath, String depthStr) {
    int depth = Integer.parseInt(depthStr);
    List<String> partitionFields = IntStream.range(0, depth).mapToObj(String::valueOf).collect(Collectors.toList());
    PartitionValueExtractor extractor = new DefaultPartitionValueExtractor(partitionFields);
    Throwable t = assertThrows(IllegalArgumentException.class, () -> extractor.extractPartitionValuesInPath(partitionPath));
    assertEquals("Expected hiveStyle=" + expectedHiveStyle + " at depth=1 but got hiveStyle=" + !expectedHiveStyle, t.getMessage());
  }

  @ParameterizedTest
  @CsvSource(value = {
      "0=a/00=b:2:00",
      "0=a/P1=b/2=c:3:P1"}, delimiter = ':')
  void testInconsistentFieldNameInHiveStylePartitions(String partitionPath, String depthStr, String unexpectedFieldName) {
    int depth = Integer.parseInt(depthStr);
    List<String> partitionFields = IntStream.range(0, depth).mapToObj(String::valueOf).collect(Collectors.toList());
    PartitionValueExtractor extractor = new DefaultPartitionValueExtractor(partitionFields);
    Throwable t = assertThrows(IllegalArgumentException.class, () -> extractor.extractPartitionValuesInPath(partitionPath));
    assertEquals("Expected field `1` at depth=1 but got `" + unexpectedFieldName + "`", t.getMessage());
  }
}
