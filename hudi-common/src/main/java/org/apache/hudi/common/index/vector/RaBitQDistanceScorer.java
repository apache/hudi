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

/** Distance reconstruction for sign-encoded RaBitQ vectors. */
public final class RaBitQDistanceScorer {

  private RaBitQDistanceScorer() {
  }

  /** Estimates the legacy norm-weighted angular proxy from two packed sign codes. */
  public static float estimateDistance(byte[] queryCode, byte[] encodedCode,
                                       float scalar, int dimension) {
    if (!Float.isFinite(scalar) || scalar < 0f) {
      throw new IllegalArgumentException("scalar must be finite and non-negative: " + scalar);
    }
    float cosine = symmetricCosine(queryCode, encodedCode, dimension);
    return scalar * (1.0f - cosine);
  }

  /** Cosine estimate from binary-vs-binary Hamming distance. */
  public static float symmetricCosine(byte[] queryCode, byte[] encodedCode, int dimension) {
    validatePackedCode(queryCode, dimension, "queryCode");
    validatePackedCode(encodedCode, dimension, "encodedCode");
    int fullBytes = dimension / Byte.SIZE;
    int hamming = 0;
    for (int i = 0; i < fullBytes; i++) {
      hamming += Integer.bitCount((queryCode[i] ^ encodedCode[i]) & 0xFF);
    }
    int remainingBits = dimension % Byte.SIZE;
    if (remainingBits != 0) {
      int mask = (1 << remainingBits) - 1;
      hamming += Integer.bitCount((queryCode[fullBytes] ^ encodedCode[fullBytes]) & mask);
    }
    return 1.0f - 2.0f * hamming / dimension;
  }

  /**
   * Cosine estimate from the rotated float query and a packed sign code.
   * This retains full query precision and therefore has lower variance than Hamming scoring.
   */
  public static float asymmetricCosine(float[] rotatedQuery, byte[] encodedCode, int dimension) {
    if (rotatedQuery == null || rotatedQuery.length != dimension) {
      throw new IllegalArgumentException("rotatedQuery length must equal dimension " + dimension);
    }
    validatePackedCode(encodedCode, dimension, "encodedCode");
    double sum = 0.0;
    for (int i = 0; i < dimension; i++) {
      if (!Float.isFinite(rotatedQuery[i])) {
        throw new IllegalArgumentException("rotatedQuery contains a non-finite value at dimension " + i);
      }
      boolean positive = (encodedCode[i >> 3] & (1 << (i & 7))) != 0;
      sum += positive ? rotatedQuery[i] : -rotatedQuery[i];
    }
    return (float) (sum / Math.sqrt(dimension));
  }

  /** Reconstructs the configured distance from a cosine estimate and vector norms. */
  public static float reconstructDistance(VectorDistanceMetric metric,
                                          float cosineEstimate,
                                          float queryNorm,
                                          float vectorNorm) {
    if (metric == null || !Float.isFinite(cosineEstimate)
        || !Float.isFinite(queryNorm) || queryNorm < 0f
        || !Float.isFinite(vectorNorm) || vectorNorm < 0f) {
      throw new IllegalArgumentException("Metric, cosine estimate, and norms must be valid");
    }
    float cosine = Math.max(-1.0f, Math.min(1.0f, cosineEstimate));
    switch (metric) {
      case L2:
        double squaredDistance = (double) queryNorm * queryNorm
            + (double) vectorNorm * vectorNorm
            - 2.0 * queryNorm * vectorNorm * cosine;
        return (float) Math.sqrt(Math.max(0.0, squaredDistance));
      case DOT_PRODUCT:
        return -(queryNorm * vectorNorm * cosine);
      case COSINE:
      default:
        return queryNorm == 0f || vectorNorm == 0f ? 1.0f : 1.0f - cosine;
    }
  }

  /** Computes Hamming distance between equally sized packed binary codes. */
  public static int hammingDistance(byte[] left, byte[] right) {
    if (left == null || right == null) {
      throw new IllegalArgumentException("Packed codes must not be null");
    }
    if (left.length != right.length) {
      throw new IllegalArgumentException(
          "Packed code length mismatch: " + left.length + " != " + right.length);
    }
    int count = 0;
    for (int i = 0; i < left.length; i++) {
      count += Integer.bitCount((left[i] ^ right[i]) & 0xFF);
    }
    return count;
  }

  private static void validatePackedCode(byte[] code, int dimension, String name) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be positive: " + dimension);
    }
    int expectedBytes = (dimension + Byte.SIZE - 1) / Byte.SIZE;
    if (code == null || code.length != expectedBytes) {
      throw new IllegalArgumentException(
          name + " length must be " + expectedBytes + " bytes for dimension " + dimension);
    }
  }
}
