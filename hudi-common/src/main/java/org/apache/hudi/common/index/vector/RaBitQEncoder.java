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

import java.util.Random;

/**
 * RaBitQ (Randomized Binary Quantization) encoder.
 *
 * <p>Encodes float vectors into 1-bit-per-dimension binary codes using a random
 * orthogonal rotation matrix R. The rotation is deterministic given (seed, dimension),
 * so R is stored as a seed — it never needs retraining.
 *
 * <p><b>Encoding (write path):</b>
 * <ol>
 *   <li>Normalize: v̂ = v / ||v||</li>
 *   <li>Rotate:    v_rot = R @ v̂</li>
 *   <li>Binarize:  code = pack(sign(v_rot))  — D bits → ceil(D/8) bytes</li>
 *   <li>Scalar:    s = ||v||  (stored alongside code; 1.0 if assume_normalized)</li>
 * </ol>
 *
 * <p><b>Query scan (read path):</b>
 * <ol>
 *   <li>Normalize + rotate the query vector (same R)</li>
 *   <li>Binarize to get q_bin</li>
 *   <li>For each database code b: Hamming(q_bin, b) → estimated cosine</li>
 *   <li>Re-rank top-R candidates with exact distance</li>
 * </ol>
 *
 * <p>Memory: the rotation matrix is D×D floats = 4·D² bytes (~2.3 MB at D=768).
 * It is built lazily on first use and reused across encode calls.
 *
 * <p>Thread-safe after the first call to {@link #encode} or {@link #encodeQuery}
 * (the lazy init is synchronized).
 */
public final class RaBitQEncoder implements VectorQuantizer {

  private static final long serialVersionUID = 1L;

  private final int dimension;
  private final long seed;
  private final boolean assumeNormalized;

  /** Rotation matrix, row-major. Populated lazily. */
  private transient volatile float[][] rotMat;

  public RaBitQEncoder(int dimension, long seed, boolean assumeNormalized) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension must be positive, got: " + dimension);
    }
    this.dimension        = dimension;
    this.seed             = seed;
    this.assumeNormalized = assumeNormalized;
  }

  /** Convenience constructor using default seed. */
  public RaBitQEncoder(int dimension) {
    this(dimension, 42L, false);
  }

  // ---- VectorQuantizer ---------------------------------------------------

  @Override
  public QuantizedVector encode(float[] vector) {
    if (vector.length != dimension) {
      throw new IllegalArgumentException(
          "Expected dimension " + dimension + ", got " + vector.length);
    }
    float norm = norm(vector);
    float scalar = assumeNormalized ? 1.0f : norm;
    float[] normalized = (norm == 0f || assumeNormalized)
        ? vector
        : normalize(vector, norm);
    float[] rotated = rotate(normalized);
    return new QuantizedVector(binarize(rotated), scalar);
  }

  @Override
  public QueryState encodeQuery(float[] queryVector) {
    if (queryVector.length != dimension) {
      throw new IllegalArgumentException(
          "Expected dimension " + dimension + ", got " + queryVector.length);
    }
    float norm = norm(queryVector);
    float[] normalized = norm == 0f ? queryVector : normalize(queryVector, norm);
    float[] rotated = rotate(normalized);
    return new RaBitQQueryState(binarize(rotated), rotated);
  }

  @Override
  public float estimateDistance(QueryState queryState, QuantizedVector encoded) {
    RaBitQQueryState qs = (RaBitQQueryState) queryState;
    int hamming = hammingDistance(qs.binaryCode, encoded.code);
    // cosine_sim ≈ 1 - 2 * hamming / D  (valid for random orthogonal R)
    float cosineSim = 1.0f - 2.0f * hamming / dimension;
    // Distance = 1 - cosine_sim; scaled by scalar for unnormalized vectors
    return encoded.scalar * (1.0f - cosineSim);
  }

  @Override
  public int codeBytes() {
    return (dimension + 7) / 8;
  }

  // ---- public utilities --------------------------------------------------

  /**
   * Compute Hamming distance between two packed binary codes.
   * Both arrays must have the same length = {@link #codeBytes()}.
   */
  public static int hammingDistance(byte[] a, byte[] b) {
    int count = 0;
    for (int i = 0; i < a.length; i++) {
      count += Integer.bitCount((a[i] ^ b[i]) & 0xFF);
    }
    return count;
  }

  /** L2 norm of a float vector. */
  public static float norm(float[] v) {
    double sum = 0.0;
    for (float x : v) {
      sum += (double) x * x;
    }
    return (float) Math.sqrt(sum);
  }

  // ---- private -----------------------------------------------------------

  private float[] normalize(float[] v, float norm) {
    float[] out = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      out[i] = v[i] / norm;
    }
    return out;
  }

  /** Applies the D×D rotation matrix to the (normalized) input. */
  private float[] rotate(float[] v) {
    float[][] rotation = getRotationMatrix();
    float[] out = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      double acc = 0.0;
      float[] row = rotation[i];
      for (int j = 0; j < dimension; j++) {
        acc += (double) row[j] * v[j];
      }
      out[i] = (float) acc;
    }
    return out;
  }

  /** Packs sign(v[i]) into ceil(D/8) bytes. Positive = bit 1, non-positive = bit 0. */
  private byte[] binarize(float[] v) {
    byte[] code = new byte[(dimension + 7) / 8];
    for (int i = 0; i < dimension; i++) {
      if (v[i] > 0f) {
        code[i >> 3] |= (byte) (1 << (i & 7));
      }
    }
    return code;
  }

  /** Lazy double-checked rotation matrix construction (Modified Gram-Schmidt). */
  private float[][] getRotationMatrix() {
    if (rotMat == null) {
      synchronized (this) {
        if (rotMat == null) {
          rotMat = buildRotationMatrix(dimension, seed);
        }
      }
    }
    return rotMat;
  }

  /**
   * Builds a random orthogonal D×D matrix using Modified Gram-Schmidt (MGS).
   * Runtime: O(D³) — acceptable as a one-time cost (~0.5 s at D=768).
   * Memory:  4 * D² bytes (~2.3 MB at D=768).
   */
  static float[][] buildRotationMatrix(int d, long seed) {
    Random rng = new Random(seed);
    // Gaussian random matrix
    double[][] randMatrix = new double[d][d];
    for (int i = 0; i < d; i++) {
      for (int j = 0; j < d; j++) {
        randMatrix[i][j] = rng.nextGaussian();
      }
    }
    // Modified Gram-Schmidt orthogonalization (column-wise)
    for (int j = 0; j < d; j++) {
      // Orthogonalize column j against all previous columns
      for (int k = 0; k < j; k++) {
        double dot = 0.0;
        for (int i = 0; i < d; i++) {
          dot += randMatrix[i][k] * randMatrix[i][j];
        }
        for (int i = 0; i < d; i++) {
          randMatrix[i][j] -= dot * randMatrix[i][k];
        }
      }
      // Normalize column j
      double colNorm = 0.0;
      for (int i = 0; i < d; i++) {
        colNorm += randMatrix[i][j] * randMatrix[i][j];
      }
      colNorm = Math.sqrt(colNorm);
      if (colNorm > 1e-10) {
        for (int i = 0; i < d; i++) {
          randMatrix[i][j] /= colNorm;
        }
      }
    }
    // Transpose to row-major for efficient row-vector multiply
    float[][] rotMat = new float[d][d];
    for (int i = 0; i < d; i++) {
      for (int j = 0; j < d; j++) {
        rotMat[i][j] = (float) randMatrix[j][i];
      }
    }
    return rotMat;
  }

  // ---- inner types -------------------------------------------------------

  /** Query state for RaBitQ: packed binary query + rotated float query. */
  public static final class RaBitQQueryState implements QueryState {
    private static final long serialVersionUID = 1L;

    public final byte[] binaryCode;
    public final float[] rotatedQuery; // kept for potential exact query scoring

    public RaBitQQueryState(byte[] binaryCode, float[] rotatedQuery) {
      this.binaryCode    = binaryCode;
      this.rotatedQuery  = rotatedQuery;
    }
  }
}