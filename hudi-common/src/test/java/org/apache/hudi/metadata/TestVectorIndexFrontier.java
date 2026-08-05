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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorIndexFrontier {

  @Test
  void testAdvancesAcrossContiguousMarkedInstants() {
    assertEquals("005", VectorIndexFrontier.advance(
        "001", "002", Arrays.asList("001", "002", "003", "004", "005"),
        new HashSet<>(Arrays.asList("003", "004", "005"))));
  }

  @Test
  void testStopsAtFirstUnmarkedInstant() {
    assertEquals("003", VectorIndexFrontier.advance(
        "001", "002", Arrays.asList("003", "004", "005"),
        new HashSet<>(Arrays.asList("003", "005"))));
  }

  @Test
  void testNullFrontierStartsAtBootstrapAndIgnoresEarlierInstants() {
    assertEquals("004", VectorIndexFrontier.advance(
        "002", null, Arrays.asList("001", "002", "003", "004"),
        new HashSet<>(Arrays.asList("003", "004"))));
  }

  @Test
  void testRejectsUnorderedSourceInstants() {
    assertThrows(IllegalArgumentException.class, () -> VectorIndexFrontier.advance(
        "001", "001", Arrays.asList("002", "004", "003"),
        new HashSet<>(Arrays.asList("002", "003", "004"))));
  }
}
