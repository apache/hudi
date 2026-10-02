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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link VectorDistanceMetric}.
 */
class TestVectorDistanceMetric {

  @Test
  void cosineIdenticalVectors() {
    float[] v = {1f, 2f, 3f};
    assertEquals(0f, VectorDistanceMetric.COSINE.compute(v, v), 1e-6f);
  }

  @Test
  void cosineOrthogonal() {
    float[] a = {1f, 0f};
    float[] b = {0f, 1f};
    assertEquals(1f, VectorDistanceMetric.COSINE.compute(a, b), 1e-6f);
  }

  @Test
  void cosineOpposite() {
    float[] a = {1f, 0f};
    float[] b = {-1f, 0f};
    assertEquals(2f, VectorDistanceMetric.COSINE.compute(a, b), 1e-6f);
  }

  @Test
  void l2ZeroDistance() {
    float[] v = {3f, 4f};
    assertEquals(0f, VectorDistanceMetric.L2.compute(v, v), 1e-6f);
  }

  @Test
  void l2KnownDistance() {
    float[] a = {0f, 0f};
    float[] b = {3f, 4f};
    assertEquals(5f, VectorDistanceMetric.L2.compute(a, b), 1e-6f);
  }

  @Test
  void dotProductSameDirection() {
    float[] a = {1f, 0f};
    float[] b = {2f, 0f};
    // dot = 2, negated distance = -2
    assertEquals(-2f, VectorDistanceMetric.DOT_PRODUCT.compute(a, b), 1e-6f);
  }

  @Test
  void dotProductOrthogonal() {
    float[] a = {1f, 0f};
    float[] b = {0f, 1f};
    assertEquals(0f, VectorDistanceMetric.DOT_PRODUCT.compute(a, b), 1e-6f);
  }

  @Test
  void dimensionMismatchThrows() {
    float[] a = {1f, 2f};
    float[] b = {1f};
    assertThrows(IllegalArgumentException.class,
        () -> VectorDistanceMetric.L2.compute(a, b));
  }

  @Test
  void fromStringCaseInsensitive() {
    assertEquals(VectorDistanceMetric.COSINE, VectorDistanceMetric.fromString("cosine"));
    assertEquals(VectorDistanceMetric.L2, VectorDistanceMetric.fromString("L2"));
    assertEquals(VectorDistanceMetric.DOT_PRODUCT, VectorDistanceMetric.fromString("dot_product"));
  }
}