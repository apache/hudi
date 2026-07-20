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

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
  private static final Map<RotationKey, float[][]> ROTATION_MATRIX_CACHE = new ConcurrentHashMap<>();
  /** Factor-version-2 configuration used for neutral-factor computation (RFC-109 v3 §3). */
  private static final RaBitQFactorConfig FACTOR_CONFIG = RaBitQFactorConfig.defaultsV2();
  private final int dimension;
  private final int bits;
  private final long seed;
  private final boolean assumeNormalized;

  /** Rotation matrix, row-major. Populated lazily. */
  private transient volatile float[][] rotMat;

  public RaBitQEncoder(int dimension, long seed, boolean assumeNormalized) {
    this(dimension, 1, seed, assumeNormalized);
  }

  public RaBitQEncoder(int dimension, int bits, long seed, boolean assumeNormalized) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension must be positive, got: " + dimension);
    }
    if (bits <= 0 || bits > 8) {
      throw new IllegalArgumentException("RaBitQ bits must be in [1, 8], got: " + bits);
    }
    this.dimension = dimension;
    this.bits = bits;
    this.seed = seed;
    this.assumeNormalized = assumeNormalized;
  }

  /** Convenience constructor using default seed. */
  public RaBitQEncoder(int dimension) {
    this(dimension, 1, 42L, false);
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

  /**
   * Metric-neutral residual encoding used by MDT postings.
   *
   * <p>The optional {@code center} is the IVF centroid in the <b>original</b> vector space.
   * When null, the vector is encoded relative to the origin.
   */
  public QuantizedVector encodeResidual(float[] vector, float[] center) {
    validateDimension(vector);
    float[] rotatedVector = rotate(vector);
    float[] rotatedCenter = center == null ? new float[dimension] : rotate(center);
    float[] residual = subtract(rotatedVector, rotatedCenter);
    byte[] binaryCode = binarize(residual);

    float residualNorm = norm(residual);
    if (residualNorm == 0.0f) {
      return new QuantizedVector(
          binaryCode,
          new byte[extendedCodeBytes()],
          0.0f,
          0.0f,
          0.0f,
          0.0f,
          0.0f,
          0.0f,
          norm(vector),
          bits);
    }

    if (bits <= 1) {
      double ipResidual1 = 0.0d;
      for (int i = 0; i < dimension; i++) {
        ipResidual1 += residual[i] * (residual[i] > 0f ? 0.5d : -0.5d);
      }
      RaBitQNeutralFactors.Factors factors = RaBitQNeutralFactors.compute(
          residual,
          rotatedCenter,
          rotatedVector,
          ipResidual1,
          ipResidual1,
          dimension,
          FACTOR_CONFIG);
      return new QuantizedVector(
          binaryCode,
          new byte[0],
          factors.residualNorm,
          factors.centerRip,
          factors.fRescale1,
          factors.centerRip,
          factors.fRescale1,
          factors.err1,
          factors.vectorNorm,
          bits);
    }

    int exBits = bits - 1;

    float[] absNormalizedResidual = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      absNormalizedResidual[i] = Math.abs(residual[i] / residualNorm);
    }

    QuantizedLevels quantizedLevels = quantizeEx(absNormalizedResidual, exBits);
    int[] exCode = quantizedLevels.levels;
    int mask = (1 << exBits) - 1;
    for (int i = 0; i < dimension; i++) {
      if (residual[i] < 0f) {
        exCode[i] = mask - exCode[i];
      }
    }

    float cBias = (float) -((1 << exBits) - 0.5d);
    double ipResidual1 = 0.0d;
    double ipResidual = 0.0d;
    for (int i = 0; i < dimension; i++) {
      boolean positive = residual[i] > 0f;
      double signOnlyCode = positive ? 0.5d : -0.5d;
      float centeredCode = exCode[i] + (positive ? (1 << exBits) : 0) + cBias;
      ipResidual1 += residual[i] * signOnlyCode;
      ipResidual += residual[i] * centeredCode;
    }

    RaBitQNeutralFactors.Factors factors = RaBitQNeutralFactors.compute(
        residual,
        rotatedCenter,
        rotatedVector,
        ipResidual1,
        ipResidual,
        dimension,
        FACTOR_CONFIG);
    return new QuantizedVector(
        binaryCode,
        packUnsignedLevels(exCode, exBits),
        factors.residualNorm,
        factors.centerRip,
        factors.fRescaleEx,
        factors.centerRip,
        factors.fRescale1,
        factors.err1,
        factors.vectorNorm,
        bits);
  }

  /**
   * Compatibility shim for old call sites. New postings must use the metric-neutral
   * residual factor convention produced by {@link #encodeResidual(float[], float[])}.
   */
  public QuantizedVector encodeForL2(float[] vector, float[] center) {
    return encodeResidual(vector, center);
  }

  @Override
  public QueryState encodeQuery(float[] queryVector) {
    validateDimension(queryVector);
    float norm = norm(queryVector);
    float[] normalized = norm == 0f ? queryVector : normalize(queryVector, norm);
    float[] rotated = rotate(normalized);
    return new RaBitQQueryState(binarize(rotated), rotated, norm);
  }

  public RaBitQQueryState encodeQueryForL2(float[] queryVector) {
    validateDimension(queryVector);
    float[] rotated = rotate(queryVector);
    return new RaBitQQueryState(binarize(rotated), rotated, norm(queryVector));
  }

  @Override
  public float estimateDistance(QueryState queryState, QuantizedVector encoded) {
    RaBitQQueryState qs = (RaBitQQueryState) queryState;
    return estimateDistance(qs.binaryCode, encoded.code, encoded.scalar, dimension);
  }

  public float estimateDistance(byte[] queryBinaryCode, byte[] encodedCode, float scalar) {
    return estimateDistance(queryBinaryCode, encodedCode, scalar, dimension);
  }

  public static float estimateDistance(byte[] queryBinaryCode, byte[] encodedCode, float scalar, int dimension) {
    int hamming = hammingDistance(queryBinaryCode, encodedCode);
    // cosine_sim ≈ 1 - 2 * hamming / D  (valid for random orthogonal R)
    float cosineSim = 1.0f - 2.0f * hamming / dimension;
    // Distance = 1 - cosine_sim; scaled by scalar for unnormalized vectors
    return scalar * (1.0f - cosineSim);
  }

  // ---- metric-aware estimation (RFC-109 L2 fix) --------------------------

  /**
   * Metric-aware approximate distance.
   *
   * <p>The binary code captures direction only (sign of the rotated <i>unit</i> vector);
   * magnitude lives in {@code encoded.scalar = ||x||}. We recover a cosine estimate of the
   * angle between query and database vector — from binary-vs-binary Hamming ({@code symmetric})
   * or float-vs-binary dot ({@code asymmetric}, lower variance) — then reconstruct the
   * configured metric using {@code ||q||} and {@code ||x||}.
   */
  public float estimateDistance(QueryState queryState,
                                QuantizedVector encoded,
                                VectorDistanceMetric metric,
                                boolean asymmetric) {
    RaBitQQueryState qs = (RaBitQQueryState) queryState;
    float cosEst = asymmetric
        ? asymmetricCosine(qs.rotatedQuery, encoded.code, dimension)
        : symmetricCosine(qs.binaryCode, encoded.code, dimension);
    return reconstructDistance(metric, cosEst, qs.queryNorm, encoded.scalar);
  }

  /** Cosine estimate from binary-vs-binary Hamming: {@code 1 - 2*hamming/D}. Cheap (popcount). */
  public static float symmetricCosine(byte[] queryBinaryCode, byte[] encodedCode, int dimension) {
    int hamming = hammingDistance(queryBinaryCode, encodedCode);
    return 1.0f - 2.0f * hamming / dimension;
  }

  /**
   * Cosine estimate from float-vs-binary:
   * {@code (1/sqrt(D)) * sum_i(rotatedQuery_i * sign(code_i))}.
   *
   * <p>{@code rotatedQuery} is the rotation of the unit query, so the result is a
   * cosine estimate in [-1, 1] with lower variance than the Hamming estimate (the query
   * keeps full float precision). Cost is ~D adds/posting vs D/8 popcount bytes.
   */
  public static float asymmetricCosine(float[] rotatedQuery, byte[] encodedCode, int dimension) {
    double acc = 0.0;
    for (int i = 0; i < dimension; i++) {
      boolean bit = (encodedCode[i >> 3] & (1 << (i & 7))) != 0;
      acc += bit ? rotatedQuery[i] : -rotatedQuery[i];
    }
    return (float) (acc / Math.sqrt(dimension));
  }

  /**
   * Reconstruct a distance (smaller = more similar) from a cosine estimate and the two norms.
   * <ul>
   *   <li>COSINE:      {@code 1 - cos}</li>
   *   <li>L2:          {@code sqrt(||q||^2 + ||x||^2 - 2*||q||*||x||*cos)}</li>
   *   <li>DOT_PRODUCT: {@code -(||q||*||x||*cos)}  (negated to match the min-heap contract)</li>
   * </ul>
   */
  public static float reconstructDistance(VectorDistanceMetric metric,
                                          float cosEst,
                                          float queryNorm,
                                          float scalar) {
    float cos = cosEst < -1.0f ? -1.0f : (cosEst > 1.0f ? 1.0f : cosEst);
    switch (metric) {
      case L2: {
        double sq = (double) queryNorm * queryNorm
            + (double) scalar * scalar
            - 2.0 * queryNorm * scalar * cos;
        return (float) Math.sqrt(Math.max(0.0, sq));
      }
      case DOT_PRODUCT:
        return -(queryNorm * scalar * cos);
      case COSINE:
      default:
        return 1.0f - cos;
    }
  }

  @Override
  public int codeBytes() {
    return (dimension + 7) / 8;
  }

  public int extendedCodeBytes() {
    return bits <= 1 ? 0 : (dimension * (bits - 1) + 7) / 8;
  }

  public int totalCodeBytes() {
    return codeBytes() + extendedCodeBytes();
  }

  public int getBits() {
    return bits;
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

  public static float l2Squared(float[] left, float[] right) {
    double sum = 0.0;
    for (int i = 0; i < left.length; i++) {
      double delta = (double) left[i] - right[i];
      sum += delta * delta;
    }
    return (float) sum;
  }

  public float[] rotateVector(float[] vector) {
    validateDimension(vector);
    return rotate(vector);
  }

  public static float dotPackedBinary(byte[] binaryCode, float[] query, int dimension) {
    double sum = 0.0;
    for (int i = 0; i < dimension; i++) {
      if ((binaryCode[i >> 3] & (1 << (i & 7))) != 0) {
        sum += query[i];
      }
    }
    return (float) sum;
  }

  public static float dotPackedUnsigned(byte[] packedLevels, int bitsPerValue, float[] query, int dimension) {
    if (packedLevels == null || bitsPerValue <= 0) {
      return 0.0f;
    }
    double sum = 0.0;
    int bitOffset = 0;
    for (int i = 0; i < dimension; i++) {
      int value = 0;
      for (int bit = 0; bit < bitsPerValue; bit++) {
        int absoluteBit = bitOffset + bit;
        int byteIndex = absoluteBit >> 3;
        int bitIndex = absoluteBit & 7;
        if ((packedLevels[byteIndex] & (1 << bitIndex)) != 0) {
          value |= (1 << bit);
        }
      }
      sum += (double) value * query[i];
      bitOffset += bitsPerValue;
    }
    return (float) sum;
  }

  public static float multibitDotTerm(float[] rotatedQuery,
                                      float sumQuery,
                                      byte[] binaryCode,
                                      byte[] extendedCode,
                                      int dimension,
                                      int bits) {
    int exBits = bits - 1;
    float binaryDot = dotPackedBinary(binaryCode, rotatedQuery, dimension);
    if (exBits <= 0) {
      return binaryDot + (sumQuery * -0.5f);
    }
    float extendedDot = dotPackedUnsigned(extendedCode, exBits, rotatedQuery, dimension);
    float cBias = (float) -((1 << bits) - 1) / 2.0f;
    return ((float) (1 << exBits) * binaryDot) + extendedDot + (sumQuery * cBias);
  }

  public static float multibitDotTerm(float[] rotatedQuery,
                                      byte[] binaryCode,
                                      byte[] extendedCode,
                                      int dimension,
                                      int bits) {
    return multibitDotTerm(rotatedQuery, sum(rotatedQuery), binaryCode, extendedCode, dimension, bits);
  }

  // ---- private -----------------------------------------------------------

  private void validateDimension(float[] v) {
    if (v.length != dimension) {
      throw new IllegalArgumentException(
          "Expected dimension " + dimension + ", got " + v.length);
    }
  }

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
          rotMat = getOrBuildRotationMatrix(dimension, seed);
        }
      }
    }
    return rotMat;
  }

  private static float[][] getOrBuildRotationMatrix(int dimension, long seed) {
    return ROTATION_MATRIX_CACHE.computeIfAbsent(new RotationKey(dimension, seed),
        key -> buildRotationMatrix(key.dimension, key.seed));
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

  private float[] subtract(float[] left, float[] right) {
    float[] out = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      out[i] = left[i] - right[i];
    }
    return out;
  }

  private QuantizedLevels quantizeEx(float[] absValues, int exBits) {
    double rescale = bestRescaleFactor(absValues, exBits);
    int[] levels = new int[dimension];
    double ipNorm = 0.0;
    int maxLevel = (1 << exBits) - 1;
    for (int i = 0; i < dimension; i++) {
      int level = (int) Math.floor((rescale * absValues[i]) + 1.0e-5);
      if (level < 0) {
        level = 0;
      } else if (level > maxLevel) {
        level = maxLevel;
      }
      levels[i] = level;
      ipNorm += (level + 0.5d) * absValues[i];
    }
    double ipNormInv = (Double.isFinite(ipNorm) && Math.abs(ipNorm) > 1.0e-12d) ? (1.0d / ipNorm) : 1.0d;
    return new QuantizedLevels(levels, ipNormInv);
  }

  private static float sum(float[] values) {
    float sum = 0.0f;
    for (float value : values) {
      sum += value;
    }
    return sum;
  }

  private double bestRescaleFactor(float[] absValues, int exBits) {
    double max = 0.0d;
    for (float value : absValues) {
      max = Math.max(max, value);
    }
    if (max <= 1.0e-12d) {
      return 1.0d;
    }

    int maxLevel = (1 << exBits) - 1;
    double bestFactor = maxLevel / max;
    double bestScore = Double.NEGATIVE_INFINITY;
    for (int step = 1; step <= 64; step++) {
      double factor = (step * maxLevel) / (64.0d * max);
      double numerator = 0.0d;
      double denominator = dimension * 0.25d;
      for (float value : absValues) {
        int level = (int) Math.floor((factor * value) + 1.0e-5d);
        if (level < 0) {
          level = 0;
        } else if (level > maxLevel) {
          level = maxLevel;
        }
        numerator += (level + 0.5d) * value;
        denominator += (level * (double) level) + level;
      }
      double score = numerator / Math.sqrt(denominator);
      if (score > bestScore) {
        bestScore = score;
        bestFactor = factor;
      }
    }
    return bestFactor;
  }

  private byte[] packUnsignedLevels(int[] levels, int bitsPerValue) {
    if (bitsPerValue <= 0) {
      return new byte[0];
    }
    byte[] packed = new byte[(dimension * bitsPerValue + 7) / 8];
    int bitOffset = 0;
    for (int level : levels) {
      for (int bit = 0; bit < bitsPerValue; bit++) {
        if ((level & (1 << bit)) != 0) {
          int absoluteBit = bitOffset + bit;
          packed[absoluteBit >> 3] |= (byte) (1 << (absoluteBit & 7));
        }
      }
      bitOffset += bitsPerValue;
    }
    return packed;
  }

  // ---- inner types -------------------------------------------------------

  private static final class QuantizedLevels {
    private final int[] levels;
    private final double ipNormInv;

    private QuantizedLevels(int[] levels, double ipNormInv) {
      this.levels = levels;
      this.ipNormInv = ipNormInv;
    }
  }

  /** Query state for RaBitQ: packed binary query + rotated float query. */
  public static final class RaBitQQueryState implements QueryState {
    private static final long serialVersionUID = 1L;

    public final byte[] binaryCode;
    public final float[] rotatedQuery; // rotation of the unit query; used by the asymmetric estimator
    public final float queryNorm;      // ||q|| before normalization; needed for L2/dot reconstruction
    public final float querySum;       // sum(rotatedQuery), reused by multibit L2 scoring

    public RaBitQQueryState(byte[] binaryCode, float[] rotatedQuery) {
      this(binaryCode, rotatedQuery, 1.0f);
    }

    public RaBitQQueryState(byte[] binaryCode, float[] rotatedQuery, float queryNorm) {
      this.binaryCode    = binaryCode;
      this.rotatedQuery  = rotatedQuery;
      this.queryNorm     = queryNorm;
      this.querySum      = sum(rotatedQuery);
    }
  }

  private static final class RotationKey {
    private final int dimension;
    private final long seed;

    private RotationKey(int dimension, long seed) {
      this.dimension = dimension;
      this.seed = seed;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      RotationKey that = (RotationKey) other;
      return dimension == that.dimension && seed == that.seed;
    }

    @Override
    public int hashCode() {
      return Objects.hash(dimension, seed);
    }
  }
}