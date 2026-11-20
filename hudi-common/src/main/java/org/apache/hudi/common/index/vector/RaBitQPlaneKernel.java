/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

/**
 * Direct plane popcount kernel (RFC-109 §3). Scores a quantized query ({@link VectorQueryPlanes})
 * against a data code stored as bit-planes (one sign plane + {@code exBits} extended planes, each a
 * {@code long[]}), computing the inner product {@code <qPrime, dataCode>} purely with
 * {@code Long.bitCount(queryWord & dataWord)} — no per-dimension unpacking, sign-row copies, or
 * extended-level repacking.
 *
 * <p>Decomposition. With {@code qPrime[i] = qMin + qLevel[i]*deltaQ} and, per dimension,
 * {@code dataCode[i] = 2^exBits * signBit[i] + exLevel[i] + cBias}:
 * <pre>
 *   &lt;qPrime, dataCode&gt; = qMin * sumDataCode + deltaQ * &lt;qLevel, dataCode&gt;
 * </pre>
 * where every {@code sum}/{@code <.,.>} term is a weighted popcount over plane words. The result is
 * bit-exact with a scalar reference dot of the same {@code qPrime} against the same code.
 */
public final class RaBitQPlaneKernel {

  private RaBitQPlaneKernel() {
  }

  /**
   * Pass-1 inner product {@code <qPrime, code1>} where {@code code1[i] = signBit[i] - 0.5}
   * (sign-only 1-bit code). Uses only the sign plane.
   */
  public static double scorePass1(VectorQueryPlanes q, long[] signPlane) {
    int words = q.words();
    checkWords(signPlane, words, "signPlane");
    double qDotSignFull = queryDotPlane(q, signPlane); // <qPrime, signBit>
    return qDotSignFull - 0.5 * q.sumQPrime();
  }

  /**
   * Pass-2 inner product {@code <qPrime, dataCode>} for the full {@code (exBits+1)}-bit centered
   * code. {@code exPlanes[b]} is the b-th extended bit-plane (LSB first), each {@code long[words]}.
   */
  public static double scorePass2(VectorQueryPlanes q, long[] signPlane, long[][] exPlanes, int exBits) {
    int words = q.words();
    checkWords(signPlane, words, "signPlane");
    if (exBits <= 0) {
      return scorePass1(q, signPlane);
    }
    if (exPlanes == null || exPlanes.length != exBits) {
      throw new IllegalArgumentException("expected " + exBits + " extended planes");
    }

    double signScale = (double) (1L << exBits);
    double cBias = -((double) (1L << (exBits + 1)) - 1.0) / 2.0; // -((2^bits - 1)/2), bits = exBits+1

    // sumDataCode = 2^exBits * popcount(sign) + sum_b 2^b * popcount(exPlane_b) + cBias * dim
    long popSign = popcount(signPlane);
    double sumExLevels = 0.0;
    for (int b = 0; b < exBits; b++) {
      checkWords(exPlanes[b], words, "exPlane[" + b + "]");
      sumExLevels += (double) (1L << b) * popcount(exPlanes[b]);
    }
    double sumDataCode = signScale * popSign + sumExLevels + cBias * q.dim();

    // <qLevel, dataCode> = 2^exBits * <qLevel, sign> + <qLevel, ex> + cBias * sumQLevel
    double qlDotSign = queryLevelDotPlane(q, signPlane);
    double qlDotEx = 0.0;
    for (int b = 0; b < exBits; b++) {
      qlDotEx += (double) (1L << b) * queryLevelDotPlane(q, exPlanes[b]);
    }
    double sumQLevel = queryLevelSum(q);
    double qlDotDataCode = signScale * qlDotSign + qlDotEx + cBias * sumQLevel;

    return q.queryMin() * sumDataCode + q.deltaQ() * qlDotDataCode;
  }

  // <qPrime, plane> = qMin*popcount(plane) + deltaQ * <qLevel, plane>
  private static double queryDotPlane(VectorQueryPlanes q, long[] plane) {
    return q.queryMin() * popcount(plane) + q.deltaQ() * queryLevelDotPlane(q, plane);
  }

  // <qLevel, plane> = sum_a 2^a * popcount(queryPlane_a & plane)
  private static double queryLevelDotPlane(VectorQueryPlanes q, long[] plane) {
    double acc = 0.0;
    for (int a = 0; a < q.bq(); a++) {
      long[] qp = q.plane(a);
      long pop = 0;
      for (int w = 0; w < plane.length; w++) {
        pop += Long.bitCount(qp[w] & plane[w]);
      }
      acc += (double) (1L << a) * pop;
    }
    return acc;
  }

  // sum_i qLevel[i] = sum_a 2^a * popcount(queryPlane_a)
  private static double queryLevelSum(VectorQueryPlanes q) {
    double acc = 0.0;
    for (int a = 0; a < q.bq(); a++) {
      acc += (double) (1L << a) * popcount(q.plane(a));
    }
    return acc;
  }

  private static long popcount(long[] words) {
    long pop = 0;
    for (long w : words) {
      pop += Long.bitCount(w);
    }
    return pop;
  }

  private static void checkWords(long[] plane, int words, String name) {
    if (plane == null || plane.length != words) {
      throw new IllegalArgumentException(name + " must have " + words + " words");
    }
  }
}
