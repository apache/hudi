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

package org.apache.hudi.index.bloom;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link HoodieBloomFilterProbingResult} value holder.
 */
public class TestHoodieBloomFilterProbingResult {

  @Test
  void exposesCandidateKeys() {
    Set<String> keys = new HashSet<>();
    keys.add("k1");
    keys.add("k2");
    HoodieBloomFilterProbingResult result = new HoodieBloomFilterProbingResult(keys);
    assertEquals(keys, result.getCandidateKeys());
  }

  @Test
  void supportsEmptyCandidateSet() {
    HoodieBloomFilterProbingResult result =
        new HoodieBloomFilterProbingResult(Collections.emptySet());
    assertTrue(result.getCandidateKeys().isEmpty());
  }

  @Test
  void valueSemanticsForEqualsAndHashCode() {
    HoodieBloomFilterProbingResult a =
        new HoodieBloomFilterProbingResult(new HashSet<>(Collections.singletonList("k")));
    HoodieBloomFilterProbingResult same =
        new HoodieBloomFilterProbingResult(new HashSet<>(Collections.singletonList("k")));
    HoodieBloomFilterProbingResult different =
        new HoodieBloomFilterProbingResult(new HashSet<>(Collections.singletonList("other")));
    assertEquals(a, same);
    assertEquals(a.hashCode(), same.hashCode());
    assertNotEquals(a, different);
  }
}
