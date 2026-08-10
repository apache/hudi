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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RaBitQEncoder}.
 */
class TestRaBitQEncoder {

  private static final int DIM = 64; // small for speed
  private RaBitQEncoder encoder;

  @BeforeEach
  void setUp() {
    encoder = new RaBitQEncoder(DIM, 42L, false);
  }

  @Test
  void splitExPlanesTransposesPackedDimensionCodes() {
    byte[] interleavedLevels = {(byte) 0xD1, 0x08};

    assertArrayEquals(
        new byte[] {0x05, 0x06, 0x08},
        VectorIndexBootstrapUtils.splitExPlanes(interleavedLevels, 3, 4, 1));
  }

  @Test
  void splitExPlanesSupportsNonByteAlignedDimensions() {
    byte[] interleavedLevels = {(byte) 0xD1, 0x00};

    assertArrayEquals(
        new byte[] {0x05, 0x06, 0x00},
        VectorIndexBootstrapUtils.splitExPlanes(interleavedLevels, 3, 3, 1));
  }

  @Test
  void codeBytesSizeCorrect() {
    assertEquals((DIM + 7) / 8, encoder.codeBytes());
  }

  @Test
  void encodeProducesCorrectCodeLength() {
    float[] v = randomVector(DIM, 0);
    QuantizedVector encoded = encoder.encode(v);
    assertEquals(encoder.codeBytes(), encoded.code.length);
  }

  @Test
  void encodeScalarIsNorm() {
    float[] v = {3f, 4f};
    RaBitQEncoder enc2 = new RaBitQEncoder(2, 42L, false);
    QuantizedVector encoded = enc2.encode(v);
    assertEquals(5f, encoded.scalar, 1e-4f);
  }

  @Test
  void encodeAssumeNormalizedScalarIsOne() {
    RaBitQEncoder enc = new RaBitQEncoder(DIM, 42L, true);
    float[] v = randomVector(DIM, 1);
    QuantizedVector encoded = enc.encode(v);
    assertEquals(1.0f, encoded.scalar, 1e-6f);
  }

  @Test
  void deterministic_sameSeedSameCode() {
    float[] v = randomVector(DIM, 99);
    RaBitQEncoder enc1 = new RaBitQEncoder(DIM, 42L, false);
    RaBitQEncoder enc2 = new RaBitQEncoder(DIM, 42L, false);
    assertArrayEquals(enc1.encode(v).code, enc2.encode(v).code);
  }

  @Test
  void differentSeedDifferentCode() {
    float[] v = randomVector(DIM, 99);
    RaBitQEncoder enc1 = new RaBitQEncoder(DIM, 42L, false);
    RaBitQEncoder enc2 = new RaBitQEncoder(DIM, 7L, false);
    byte[] c1 = enc1.encode(v).code;
    byte[] c2 = enc2.encode(v).code;
    // Very unlikely to be equal for a random vector with different rotation seeds
    assertFalse(java.util.Arrays.equals(c1, c2),
        "Different seeds should produce different codes for a random vector");
  }

  @Test
  void hammingDistanceSelf() {
    byte[] code = {(byte) 0b10110101, (byte) 0b11001100};
    assertEquals(0, RaBitQDistanceScorer.hammingDistance(code, code));
  }

  @Test
  void hammingDistanceKnown() {
    byte[] a = {(byte) 0xFF};
    byte[] b = {(byte) 0x00};
    assertEquals(8, RaBitQDistanceScorer.hammingDistance(a, b));
  }

  @Test
  void estimateDistanceSameVector() {
    float[] v = randomVector(DIM, 5);
    QuantizedVector encoded = encoder.encode(v);
    RaBitQQueryState qs = encoder.encodeQuery(v);
    // Same vector → Hamming distance should be very small → distance close to 0
    float dist = encoder.estimateDistance(qs, encoded);
    assertTrue(dist >= 0f, "Distance must be non-negative");
    assertTrue(dist < 0.2f, "Same-vector distance should be very small, got: " + dist);
  }

  @Test
  void estimateDistanceOppositeVector() {
    float[] v = randomVector(DIM, 5);
    float[] neg = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      neg[i] = -v[i];
    }

    QuantizedVector encoded = encoder.encode(neg);
    RaBitQQueryState qs = encoder.encodeQuery(v);
    float dist = encoder.estimateDistance(qs, encoded);
    // Opposite vectors → distance should be larger than same-vector case
    assertTrue(dist > 0.5f, "Opposite vectors should have large distance, got: " + dist);
  }

  @Test
  void rotationMatrixIsOrthogonal() {
    // Each row should have unit norm and rows should be orthogonal
    float[][] rotMat = RaBitQEncoder.buildRotationMatrix(8, 42L);
    for (int i = 0; i < 8; i++) {
      double norm = 0.0;
      for (float v : rotMat[i]) {
        norm += (double) v * v;
      }
      assertEquals(1.0, norm, 1e-4, "Row " + i + " should have unit norm");
    }
    // Check orthogonality of first two rows
    double dot = 0.0;
    for (int j = 0; j < 8; j++) {
      dot += (double) rotMat[0][j] * rotMat[1][j];
    }
    assertEquals(0.0, dot, 1e-4, "First two rows should be orthogonal");
  }

  @Test
  void wrongDimensionThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> encoder.encode(new float[]{1f, 2f}));
  }

  @Test
  void rejectsNonFiniteVectorsQueriesAndCentroids() {
    RaBitQEncoder twoDimensional = new RaBitQEncoder(2);
    assertThrows(IllegalArgumentException.class,
        () -> twoDimensional.encode(new float[] {Float.NaN, 1f}));
    assertThrows(IllegalArgumentException.class,
        () -> twoDimensional.encodeQuery(new float[] {1f, Float.POSITIVE_INFINITY}));
    assertThrows(IllegalArgumentException.class,
        () -> twoDimensional.encodeResidual(new float[] {1f, 2f}, new float[] {1f}));
  }

  @Test
  void oneDimensionalResidualProducesFiniteExactFactors() {
    QuantizedVector encoded = new RaBitQEncoder(1, 4, 42L, false)
        .encodeResidual(new float[] {2f}, new float[] {0f});
    assertEquals(0f, encoded.error1, 0f);
    assertTrue(Float.isFinite(encoded.rescaleFactor1));
  }

  @Test
  void scorerIgnoresPackedPaddingBits() {
    assertEquals(1f, RaBitQDistanceScorer.symmetricCosine(
        new byte[] {0b00000001}, new byte[] {(byte) 0b11111111}, 1), 0f);
  }

  @Test
  void quantizedVectorRejectsInvalidPayloadsAndCopiesCodes() {
    assertThrows(IllegalArgumentException.class,
        () -> new QuantizedVector(new byte[] {1}, null, 1f, null, null, 2));
    assertThrows(IllegalArgumentException.class,
        () -> new QuantizedVector(new byte[] {1}, new byte[] {1}, 1f, null, null, 1));
    assertThrows(IllegalArgumentException.class,
        () -> new QuantizedVector(new byte[] {1}, new byte[0], 1f,
            0f, 0f, 0f, 0f, -1f, 1f, 1));

    byte[] code = {1};
    QuantizedVector vector = new QuantizedVector(code, 1f);
    code[0] = 2;
    assertArrayEquals(new byte[] {1}, vector.getCode());
    byte[] returned = vector.getCode();
    returned[0] = 3;
    assertArrayEquals(new byte[] {1}, vector.getCode());
  }

  @Test
  void normKnownValue() {
    assertEquals(5f, RaBitQEncoder.norm(new float[]{3f, 4f}), 1e-5f);
    assertEquals(0f, RaBitQEncoder.norm(new float[]{0f, 0f}), 1e-5f);
  }

  // ---- metric-aware reconstruction (RFC-109 metric-aware scoring) ----------------------

  @Test
  void reconstructDistanceL2CollinearRanksByMagnitude() {
    // Worked example: query ||q||=10, collinear candidates (cos=1).
    // Legacy cosine proxy tied both at 0; correct L2 ranks by magnitude difference.
    float qNorm = 10f;
    float distSame = RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.L2, 1.0f, qNorm, 10f);
    float distTiny = RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.L2, 1.0f, qNorm, 0.1f);
    assertEquals(0f, distSame, 1e-3f, "Identical-magnitude collinear vector should be L2 distance 0");
    assertEquals(9.9f, distTiny, 1e-2f, "Tiny collinear vector should be ~9.9 away");
    assertTrue(distSame < distTiny, "L2 must rank same-magnitude closer than tiny collinear");
  }

  @Test
  void reconstructDistanceCosineIgnoresMagnitude() {
    assertEquals(0f, RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.COSINE, 1.0f, 10f, 10f), 1e-5f);
    assertEquals(0f, RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.COSINE, 1.0f, 10f, 0.1f), 1e-5f);
    assertEquals(2f, RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.COSINE, -1.0f, 1f, 1f), 1e-5f);
  }

  @Test
  void reconstructDistanceDotProductNegatesSimilarity() {
    assertEquals(-100f, RaBitQDistanceScorer.reconstructDistance(VectorDistanceMetric.DOT_PRODUCT, 1.0f, 10f, 10f), 1e-3f);
  }

  @Test
  void metricAwareL2EstimateRanksTrueNeighborFirst() {
    // End-to-end: an L2 near neighbour must score below a collinear-but-far vector.
    // The legacy cosine scoring scored them identically (the root-cause bug).
    int dim = 64;
    RaBitQEncoder enc = new RaBitQEncoder(dim, 42L, false);
    float[] q = randomVector(dim, 5);
    float[] a = q.clone();                 // same direction, same magnitude -> true L2 ~0
    float[] b = new float[dim];            // same direction, 10x magnitude -> far in L2, identical cosine
    for (int i = 0; i < dim; i++) {
      b[i] = q[i] * 10f;
    }
    RaBitQQueryState qs = enc.encodeQuery(q);
    float dA = enc.estimateDistance(qs, enc.encode(a), VectorDistanceMetric.L2, false);
    float dB = enc.estimateDistance(qs, enc.encode(b), VectorDistanceMetric.L2, false);
    assertTrue(dA < dB,
        "L2 must rank same-magnitude neighbour (" + dA + ") closer than 10x-magnitude (" + dB + ")");
  }

  @Test
  void asymmetricCosineSameVectorIsHigh() {
    int dim = 64;
    RaBitQEncoder enc = new RaBitQEncoder(dim, 42L, false);
    float[] v = randomVector(dim, 7);
    RaBitQQueryState qs = enc.encodeQuery(v);
    float cos = RaBitQDistanceScorer.asymmetricCosine(qs.getRotatedQuery(), enc.encode(v).code, dim);
    assertTrue(cos > 0.6f, "Asymmetric cosine of a vector with itself should be high, got: " + cos);
  }

  @Test
  void encodeQueryCapturesQueryNorm() {
    RaBitQEncoder enc2 = new RaBitQEncoder(2, 42L, false);
    RaBitQQueryState qs = enc2.encodeQuery(new float[]{3f, 4f});
    assertEquals(5f, qs.getQueryNorm(), 1e-4f);
  }

  @Test
  void encodeResidualFactorsMatchNeutralCodeIdentity() {
    int dim = 16;
    int bits = 3;
    long seed = 101L;
    float[] vector = randomVector(dim, 11);
    float[] center = randomVector(dim, 13);
    RaBitQEncoder enc = new RaBitQEncoder(dim, bits, seed, false);
    QuantizedVector encoded = enc.encodeResidual(vector, center);

    float[][] rotation = RaBitQEncoder.buildRotationMatrix(dim, seed);
    float[] rotatedVector = rotate(rotation, vector);
    float[] rotatedCenter = rotate(rotation, center);
    float[] residual = subtract(rotatedVector, rotatedCenter);
    double residualSq = dot(residual, residual);
    double centerRip = dot(rotatedCenter, residual);

    float[] signCode = signOnlyCode(encoded.code, dim);
    double signIpResidual = dot(residual, signCode);
    assertRelativeEquals(centerRip, encoded.additiveFactor1, 1.0e-4d);
    assertRelativeEquals(residualSq / signIpResidual, encoded.rescaleFactor1, 1.0e-4d);

    float[] fullCode = fullCenteredCode(encoded.code, encoded.extendedCode, dim, bits);
    double fullIpResidual = dot(residual, fullCode);
    assertRelativeEquals(centerRip, encoded.additiveFactor, 1.0e-4d);
    assertRelativeEquals(residualSq / fullIpResidual, encoded.rescaleFactor, 1.0e-4d);
    assertRelativeEquals(Math.sqrt(residualSq), encoded.scalar, 1.0e-4d);
    assertRelativeEquals(RaBitQEncoder.norm(vector), encoded.vectorNorm, 1.0e-4d);
  }

  // ---- helpers -----------------------------------------------------------

  private static float[] randomVector(int dim, int seed) {
    Random rng = new Random(seed);
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rng.nextGaussian();
    }
    return v;
  }

  private static float[] rotate(float[][] rotation, float[] vector) {
    float[] out = new float[vector.length];
    for (int i = 0; i < vector.length; i++) {
      double acc = 0.0d;
      for (int j = 0; j < vector.length; j++) {
        acc += (double) rotation[i][j] * vector[j];
      }
      out[i] = (float) acc;
    }
    return out;
  }

  private static float[] subtract(float[] left, float[] right) {
    float[] out = new float[left.length];
    for (int i = 0; i < left.length; i++) {
      out[i] = left[i] - right[i];
    }
    return out;
  }

  private static double dot(float[] left, float[] right) {
    double sum = 0.0d;
    for (int i = 0; i < left.length; i++) {
      sum += (double) left[i] * right[i];
    }
    return sum;
  }

  private static float[] signOnlyCode(byte[] signCode, int dimension) {
    float[] code = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      code[i] = (signCode[i >> 3] & (1 << (i & 7))) != 0 ? 0.5f : -0.5f;
    }
    return code;
  }

  private static float[] fullCenteredCode(byte[] signCode, byte[] extendedCode, int dimension, int bits) {
    int exBits = bits - 1;
    float cBias = (float) -((1 << exBits) - 0.5d);
    float[] code = new float[dimension];
    int bitOffset = 0;
    for (int i = 0; i < dimension; i++) {
      int level = 0;
      for (int bit = 0; bit < exBits; bit++) {
        int absoluteBit = bitOffset + bit;
        if ((extendedCode[absoluteBit >> 3] & (1 << (absoluteBit & 7))) != 0) {
          level |= 1 << bit;
        }
      }
      boolean positive = (signCode[i >> 3] & (1 << (i & 7))) != 0;
      code[i] = level + (positive ? (1 << exBits) : 0) + cBias;
      bitOffset += exBits;
    }
    return code;
  }

  private static void assertRelativeEquals(double expected, double actual, double tolerance) {
    double scale = Math.max(1.0d, Math.abs(expected));
    assertTrue(Math.abs(expected - actual) <= tolerance * scale,
        "expected=" + expected + ", actual=" + actual);
  }
}