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

/**
 * Default exact scorer (RFC-109 §10). Accumulates in float64 and returns an order-preserving
 * ranking distance (smaller = more similar), consistent with the approximate path:
 *
 * <ul>
 *   <li>{@code L2}: squared L2 (kept squared internally through ranking; callers may take the
 *       square root only for final presentation).</li>
 *   <li>{@code DOT_PRODUCT}: negated dot product.</li>
 *   <li>{@code COSINE}: {@code 1 - cosine_similarity}.</li>
 * </ul>
 */
public final class DefaultExactVectorScorer implements ExactVectorScorer {

  private static final long serialVersionUID = 1L;

  @Override
  public double distance(float[] query, float[] candidate, VectorDistanceMetric metric) {
    if (query.length != candidate.length) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: " + query.length + " vs " + candidate.length);
    }
    switch (metric) {
      case L2:
        return squaredL2(query, candidate);
      case DOT_PRODUCT:
        return -dot(query, candidate);
      case COSINE:
        return cosineDistance(query, candidate);
      default:
        throw new IllegalArgumentException("Unsupported metric: " + metric);
    }
  }

  private static double squaredL2(float[] a, float[] b) {
    double sum = 0.0;
    for (int i = 0; i < a.length; i++) {
      double d = (double) a[i] - (double) b[i];
      sum += d * d;
    }
    return sum;
  }

  private static double dot(float[] a, float[] b) {
    double dot = 0.0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * (double) b[i];
    }
    return dot;
  }

  private static double cosineDistance(float[] a, float[] b) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * (double) b[i];
      normA += (double) a[i] * (double) a[i];
      normB += (double) b[i] * (double) b[i];
    }
    double denom = Math.sqrt(normA) * Math.sqrt(normB);
    return denom == 0.0 ? 1.0 : 1.0 - dot / denom;
  }
}
