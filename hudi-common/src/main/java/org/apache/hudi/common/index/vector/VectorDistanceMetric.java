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

import java.io.Serializable;

/**
 * Distance metrics for vector similarity search.
 *
 * <p>All metrics are returned as distances (smaller = more similar),
 * so they can be compared uniformly with a min-heap.
 */
public enum VectorDistanceMetric implements Serializable {

  /**
   * Cosine distance: 1 - cosine_similarity.
   * Range: [0, 2]. 0 = identical direction, 2 = opposite.
   */
  COSINE {
    @Override
    public float compute(float[] a, float[] b) {
      checkDimensions(a, b);
      double dot = 0;
      double normA = 0;
      double normB = 0;
      for (int i = 0; i < a.length; i++) {
        dot   += (double) a[i] * b[i];
        normA += (double) a[i] * a[i];
        normB += (double) b[i] * b[i];
      }
      double denom = Math.sqrt(normA) * Math.sqrt(normB);
      return denom == 0.0 ? 1.0f : (float) (1.0 - dot / denom);
    }
  },

  /**
   * Euclidean (L2) distance.
   * Range: [0, ∞). 0 = identical.
   */
  L2 {
    @Override
    public float compute(float[] a, float[] b) {
      checkDimensions(a, b);
      double sum = 0;
      for (int i = 0; i < a.length; i++) {
        double d = (double) a[i] - b[i];
        sum += d * d;
      }
      return (float) Math.sqrt(sum);
    }
  },

  /**
   * Maximum inner product distance: negated dot product.
   * Negated so smaller = higher similarity, consistent with the min-heap contract.
   */
  DOT_PRODUCT {
    @Override
    public float compute(float[] a, float[] b) {
      checkDimensions(a, b);
      double dot = 0;
      for (int i = 0; i < a.length; i++) {
        dot += (double) a[i] * b[i];
      }
      return (float) -dot;
    }
  };

  /**
   * Compute the distance between two float vectors.
   *
   * @param a first vector
   * @param b second vector
   * @return non-negative distance (smaller = more similar)
   */
  public abstract float compute(float[] a, float[] b);

  /**
   * Parse a metric name (case-insensitive).
   *
   * @param name e.g. "cosine", "l2", "dot_product"
   * @return matching enum constant
   */
  public static VectorDistanceMetric fromString(String name) {
    return valueOf(name.toUpperCase().replace(" ", "_").replace("-", "_"));
  }

  // ---- helpers -----------------------------------------------------------

  private static void checkDimensions(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: " + a.length + " vs " + b.length);
    }
  }
}