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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorSourceInstantCoverage {

  @Test
  void testStopsAtFirstMarkerGap() {
    assertEquals("002", VectorSourceInstantCoverage.advance(
        "001",
        Arrays.asList("002", "003", "004"),
        new HashSet<>(Arrays.asList("002", "004"))));
  }

  @Test
  void testAdvancesAcrossNoOpCommitMarker() {
    assertEquals("004", VectorSourceInstantCoverage.advance(
        "001",
        Arrays.asList("002", "003", "004"),
        new HashSet<>(Arrays.asList("002", "003", "004"))));
  }

  @Test
  void testRejectsUnorderedSourceTimeline() {
    assertThrows(IllegalArgumentException.class, () -> VectorSourceInstantCoverage.advance(
        "002", Arrays.asList("003", "001"), Collections.singleton("003")));
  }
}
