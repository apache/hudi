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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link VectorIndexFreshness}. */
class TestVectorIndexFreshness {

  @Test
  void queryAtOrBeforeFrontierIsEligible() {
    assertTrue(VectorIndexFreshness.isIndexEligible("20240101010101", "20240101010102"));
    assertTrue(VectorIndexFreshness.isIndexEligible("20240101010102", "20240101010102"));
  }

  @Test
  void queryAfterFrontierIsIneligible() {
    assertFalse(VectorIndexFreshness.isIndexEligible("20240101010103", "20240101010102"));
  }

  @Test
  void missingFrontierIsIneligible() {
    assertFalse(VectorIndexFreshness.isIndexEligible("20240101010101", null));
  }

  @Test
  void emptyQueryIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexFreshness.isIndexEligible("", "20240101010102"));
  }
}
