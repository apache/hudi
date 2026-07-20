/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import java.io.Serializable;

/**
 * The probed-cluster query quantized to {@code Bq} bit-planes for direct popcount scoring
 * (RFC-109 v3 §5). The selected cluster query {@code wRot} is uniformly scalar-quantized to
 * {@code Bq}-bit levels once per probed cluster; each bit position is stored as a {@code long[]}
 * plane so the scan kernel can score with {@code Long.bitCount(dataWord & queryWord)} instead of
 * per-dimension float multiply-add.
 *
 * <p>Reconstruction: {@code qPrime[i] = qMin + level[i] * deltaQ}. The scan kernel computes
 * {@code <qPrime, dataCode>} exactly; the residual {@code selectedQuery - qPrime} is captured by
 * {@link #quantizationErrorNorm} and folds into the plane error bound
 * {@code errAbs = ERR_1 * ||qPrime|| + RESIDUAL_NORM * ||selectedQuery - qPrime||}.
 */
public final class VectorQueryPlanes implements Serializable {

  private static final long serialVersionUID = 1L;

  private final long[][] planes;   // [bq][words]
  private final int dim;
  private final int bq;
  private final int words;
  private final double qMin;
  private final double deltaQ;
  private final double sumQPrime;
  private final double qPrimeNorm;
  private final double quantizationErrorNorm;

  private VectorQueryPlanes(long[][] planes, int dim, int bq, int words, double qMin, double deltaQ,
                            double sumQPrime, double qPrimeNorm, double quantizationErrorNorm) {
    this.planes = planes;
    this.dim = dim;
    this.bq = bq;
    this.words = words;
    this.qMin = qMin;
    this.deltaQ = deltaQ;
    this.sumQPrime = sumQPrime;
    this.qPrimeNorm = qPrimeNorm;
    this.quantizationErrorNorm = quantizationErrorNorm;
  }

  /**
   * Quantize the (rotated) selected cluster query to {@code bq} bit-planes.
   *
   * @param selectedQuery the rotated residual query {@code wRot} (length = dimPadded)
   * @param bq            query bit width (RFC-109 default {@code Bq = 4})
   */
  public static VectorQueryPlanes quantize(float[] selectedQuery, int bq) {
    if (bq <= 0 || bq > 16) {
      throw new IllegalArgumentException("Bq must be in [1, 16], got: " + bq);
    }
    int dim = selectedQuery.length;
    int words = (dim + 63) >> 6;
    int maxLevel = (1 << bq) - 1;

    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (float v : selectedQuery) {
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    double deltaQ = (max > min) ? (max - min) / maxLevel : 0.0;

    long[][] planes = new long[bq][words];
    double sumQPrime = 0.0;
    double qPrimeNormSq = 0.0;
    double errNormSq = 0.0;
    for (int i = 0; i < dim; i++) {
      int level = 0;
      if (deltaQ > 0.0) {
        level = (int) Math.round((selectedQuery[i] - min) / deltaQ);
        if (level < 0) {
          level = 0;
        } else if (level > maxLevel) {
          level = maxLevel;
        }
      }
      double qp = min + level * deltaQ;
      int w = i >> 6;
      long bit = 1L << (i & 63);
      for (int b = 0; b < bq; b++) {
        if (((level >> b) & 1) != 0) {
          planes[b][w] |= bit;
        }
      }
      sumQPrime += qp;
      qPrimeNormSq += qp * qp;
      double d = selectedQuery[i] - qp;
      errNormSq += d * d;
    }

    return new VectorQueryPlanes(planes, dim, bq, words, min, deltaQ, sumQPrime,
        Math.sqrt(qPrimeNormSq), Math.sqrt(errNormSq));
  }

  /** Reconstruct the quantized query {@code qPrime} (mainly for tests / scalar reference). */
  public float[] reconstruct() {
    float[] out = new float[dim];
    for (int i = 0; i < dim; i++) {
      int w = i >> 6;
      long bit = 1L << (i & 63);
      int level = 0;
      for (int b = 0; b < bq; b++) {
        if ((planes[b][w] & bit) != 0) {
          level |= (1 << b);
        }
      }
      out[i] = (float) (qMin + level * deltaQ);
    }
    return out;
  }

  public long[] plane(int b) {
    return planes[b];
  }

  public int dim() {
    return dim;
  }

  public int bq() {
    return bq;
  }

  public int words() {
    return words;
  }

  public double queryMin() {
    return qMin;
  }

  public double deltaQ() {
    return deltaQ;
  }

  public double sumQPrime() {
    return sumQPrime;
  }

  public double queryPrimeNorm() {
    return qPrimeNorm;
  }

  public double quantizationErrorNorm() {
    return quantizationErrorNorm;
  }

  /**
   * Plane error bound (RFC-109 v3 §5):
   * {@code errAbs = err1 * ||qPrime|| + residualNorm * ||selectedQuery - qPrime||}.
   */
  public double planeErrorBound(double err1, double residualNorm) {
    return err1 * qPrimeNorm + residualNorm * quantizationErrorNorm;
  }
}
