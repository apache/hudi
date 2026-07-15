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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the default exact scorer (RFC-104 v3 §10): squared-L2 for L2 (order-preserving vs the
 * true metric), negated dot for DOT_PRODUCT, and 1-cos for COSINE, all in float64.
 */
public class TestDefaultExactVectorScorer {

  private final ExactVectorScorer scorer = new DefaultExactVectorScorer();

  @Test
  void l2ReturnsSquaredDistance() {
    float[] q = {1f, 2f, 3f};
    float[] c = {4f, 6f, 3f};
    // diffs: 3,4,0 -> squared sum = 9 + 16 + 0 = 25 (NOT sqrt=5)
    assertEquals(25.0, scorer.distance(q, c, VectorDistanceMetric.L2), 1e-9);
  }

  @Test
  void l2SquaredPreservesOrderVsTrueMetric() {
    float[] q = {0f, 0f};
    float[] near = {1f, 0f};   // true L2 = 1, squared = 1
    float[] far = {3f, 0f};    // true L2 = 3, squared = 9
    double dNear = scorer.distance(q, near, VectorDistanceMetric.L2);
    double dFar = scorer.distance(q, far, VectorDistanceMetric.L2);
    assertTrue(dNear < dFar, "squared L2 must preserve nearest-neighbor ordering");
  }

  @Test
  void dotProductIsNegated() {
    float[] q = {1f, 2f, 3f};
    float[] c = {1f, 1f, 1f};
    // dot = 6 -> negated = -6
    assertEquals(-6.0, scorer.distance(q, c, VectorDistanceMetric.DOT_PRODUCT), 1e-9);
  }

  @Test
  void cosineOfParallelVectorsIsZero() {
    float[] q = {1f, 2f, 3f};
    float[] c = {2f, 4f, 6f}; // same direction
    assertEquals(0.0, scorer.distance(q, c, VectorDistanceMetric.COSINE), 1e-9);
  }

  @Test
  void cosineHandlesZeroVector() {
    float[] q = {0f, 0f, 0f};
    float[] c = {1f, 2f, 3f};
    assertEquals(1.0, scorer.distance(q, c, VectorDistanceMetric.COSINE), 1e-9);
  }

  @Test
  void dimensionMismatchThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> scorer.distance(new float[] {1f, 2f}, new float[] {1f}, VectorDistanceMetric.L2));
  }
}
