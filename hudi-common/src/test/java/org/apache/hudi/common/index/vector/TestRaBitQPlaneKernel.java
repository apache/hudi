/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scalar-vs-popcount identity for the direct plane kernel (RFC-109 §3). Proves the
 * {@link RaBitQPlaneKernel} popcount result equals a scalar reference dot of the same quantized
 * query {@code qPrime} against the same centered data code, for both pass 1 (sign only) and pass 2
 * (full multibit).
 */
public class TestRaBitQPlaneKernel {

  private static long[] planeFromBits(boolean[] bits, int words) {
    long[] p = new long[words];
    for (int i = 0; i < bits.length; i++) {
      if (bits[i]) {
        p[i >> 6] |= 1L << (i & 63);
      }
    }
    return p;
  }

  @Test
  void pass2PopcountEqualsScalarReference() {
    for (int trial = 0; trial < 50; trial++) {
      Random rng = new Random(1000 + trial);
      int dim = 100;              // deliberately not a multiple of 64
      int bq = 4;
      int exBits = 3;             // bits = 4
      int words = (dim + 63) >> 6;

      // Random query and its plane quantization.
      float[] query = new float[dim];
      for (int i = 0; i < dim; i++) {
        query[i] = (float) (rng.nextGaussian() * 3.0);
      }
      VectorQueryPlanes q = VectorQueryPlanes.quantize(query, bq);
      float[] qPrime = q.reconstruct();

      // Random data code: sign bit + exLevel in [0, 2^exBits - 1].
      boolean[] signBit = new boolean[dim];
      int[] exLevel = new int[dim];
      boolean[][] exBitsArr = new boolean[exBits][dim];
      for (int i = 0; i < dim; i++) {
        signBit[i] = rng.nextBoolean();
        exLevel[i] = rng.nextInt(1 << exBits);
        for (int b = 0; b < exBits; b++) {
          exBitsArr[b][i] = ((exLevel[i] >> b) & 1) != 0;
        }
      }
      long[] signPlane = planeFromBits(signBit, words);
      long[][] exPlanes = new long[exBits][];
      for (int b = 0; b < exBits; b++) {
        exPlanes[b] = planeFromBits(exBitsArr[b], words);
      }

      // Scalar reference: centeredCode[i] = 2^exBits*sign + exLevel + cBias.
      double cBias = -((double) ((1 << (exBits + 1)) - 1)) / 2.0;
      double refPass2 = 0.0;
      double refPass1 = 0.0;
      for (int i = 0; i < dim; i++) {
        double centered = (1 << exBits) * (signBit[i] ? 1.0 : 0.0) + exLevel[i] + cBias;
        refPass2 += qPrime[i] * centered;
        refPass1 += qPrime[i] * ((signBit[i] ? 1.0 : 0.0) - 0.5);
      }

      double gotPass2 = RaBitQPlaneKernel.scorePass2(q, signPlane, exPlanes, exBits);
      double gotPass1 = RaBitQPlaneKernel.scorePass1(q, signPlane);

      double tol = 1e-6 * Math.max(1.0, Math.abs(refPass2));
      assertEquals(refPass2, gotPass2, tol, "pass2 popcount != scalar at trial " + trial);
      assertEquals(refPass1, gotPass1, 1e-6 * Math.max(1.0, Math.abs(refPass1)),
          "pass1 popcount != scalar at trial " + trial);
    }
  }

  @Test
  void quantizeReconstructRoundTripsWithinOneStep() {
    Random rng = new Random(7);
    int dim = 64;
    float[] query = new float[dim];
    for (int i = 0; i < dim; i++) {
      query[i] = (float) (rng.nextGaussian());
    }
    VectorQueryPlanes q = VectorQueryPlanes.quantize(query, 4);
    float[] qp = q.reconstruct();
    for (int i = 0; i < dim; i++) {
      assertTrue(Math.abs(query[i] - qp[i]) <= q.deltaQ() + 1e-6,
          "reconstruction error exceeds one quantization step at i=" + i);
    }
  }

  @Test
  void planeErrorBoundCombinesFactorAndQuantizationError() {
    float[] query = {1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f};
    VectorQueryPlanes q = VectorQueryPlanes.quantize(query, 4);
    double err1 = 0.1;
    double residualNorm = 2.0;
    double expected = err1 * q.queryPrimeNorm() + residualNorm * q.quantizationErrorNorm();
    assertEquals(expected, q.planeErrorBound(err1, residualNorm), 1e-9);
  }
}
