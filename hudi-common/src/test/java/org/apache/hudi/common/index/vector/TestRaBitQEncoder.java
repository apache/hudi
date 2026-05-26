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
  void codeBytesSizeCorrect() {
    assertEquals((DIM + 7) / 8, encoder.codeBytes());
  }

  @Test
  void encodeProducesCorrectCodeLength() {
    float[] v = randomVector(DIM, 0);
    VectorQuantizer.QuantizedVector encoded = encoder.encode(v);
    assertEquals(encoder.codeBytes(), encoded.code.length);
  }

  @Test
  void encodeScalarIsNorm() {
    float[] v = {3f, 4f};
    RaBitQEncoder enc2 = new RaBitQEncoder(2, 42L, false);
    VectorQuantizer.QuantizedVector encoded = enc2.encode(v);
    assertEquals(5f, encoded.scalar, 1e-4f);
  }

  @Test
  void encodeAssumeNormalizedScalarIsOne() {
    RaBitQEncoder enc = new RaBitQEncoder(DIM, 42L, true);
    float[] v = randomVector(DIM, 1);
    VectorQuantizer.QuantizedVector encoded = enc.encode(v);
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
    assertEquals(0, RaBitQEncoder.hammingDistance(code, code));
  }

  @Test
  void hammingDistanceKnown() {
    byte[] a = {(byte) 0xFF};
    byte[] b = {(byte) 0x00};
    assertEquals(8, RaBitQEncoder.hammingDistance(a, b));
  }

  @Test
  void estimateDistanceSameVector() {
    float[] v = randomVector(DIM, 5);
    VectorQuantizer.QuantizedVector encoded = encoder.encode(v);
    VectorQuantizer.QueryState qs = encoder.encodeQuery(v);
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

    VectorQuantizer.QuantizedVector encoded = encoder.encode(neg);
    VectorQuantizer.QueryState qs = encoder.encodeQuery(v);
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
  void normKnownValue() {
    assertEquals(5f, RaBitQEncoder.norm(new float[]{3f, 4f}), 1e-5f);
    assertEquals(0f, RaBitQEncoder.norm(new float[]{0f, 0f}), 1e-5f);
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
}