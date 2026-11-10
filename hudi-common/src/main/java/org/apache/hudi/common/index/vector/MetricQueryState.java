/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import java.io.Serializable;

/**
 * Per-metric query state using residual-query estimation in rotated space (RFC-109 §3).
 *
 * <p>The MDT stores metric-neutral residual factors. The padded query is rotated <em>exactly
 * once</em> in the constructor to {@code qRot = P(qPad)}. For each probed IVF cluster the scorer
 * consumes an <em>already-rotated</em> centroid {@code cRot} and forms the rotated residual query
 * {@code wRot = qRot - cRot} by subtraction only — it never rotates {@code q - c} per cluster.
 * Because rotation is a linear isometry, {@code P(q - c) = P(q) - P(c)} and
 * the inner product {@code &lt;q, c&gt; = &lt;qRot, cRot&gt;}, so exact terms are computed directly
 * in rotated space.
 */
public abstract class MetricQueryState implements Serializable {

  private static final long serialVersionUID = 2L;
  protected static final double EPS_NORM = 1.0e-9;

  /** Rotated padded query {@code qRot = P(qPad)}; computed once. */
  protected final float[] rotatedQuery;
  /** ||q|| (rotation-invariant). */
  protected final double queryNorm;
  /** The shared query rotation {@code P}; retained only to rotate centroids once (cached). */
  private final QueryRotation rotation;

  protected MetricQueryState(QueryRotation rotation, float[] paddedQuery) {
    this.rotation = rotation;
    this.rotatedQuery = validateRotationResult(
        rotation.apply(paddedQuery.clone()), paddedQuery.length, "query");
    this.queryNorm = Math.sqrt(sqNorm(rotatedQuery));
  }

  /**
   * Apply the shared rotation {@code P} to a raw padded centroid, yielding {@code cRot = P(c)}.
   * Callers should invoke this at most once per distinct probed cluster (cached) — never per
   * {@code q - c} — and pass the result to {@link #forRotatedCentroid(float[])}. Prefer feeding
   * pre-rotated centroids from the index cache (RFC-109 §3) where available.
   */
  public final float[] rotateCentroid(float[] paddedCentroid) {
    if (paddedCentroid == null) {
      throw new IllegalArgumentException("paddedCentroid must not be null");
    }
    return validateRotationResult(
        rotation.apply(paddedCentroid.clone()), paddedCentroid.length, "centroid");
  }

  /** Whether this metric requires the 7th (VECTOR_NORM) scalar array in blocks. */
  public boolean requiresVectorNorm() {
    return false;
  }

  /** Reject degenerate queries per the spec's normative rules. */
  public void validate() {
    // L2 permits any query (s ~ 0 handled per cluster); overridden where zero-q is invalid.
  }

  /** The rotated padded query {@code qRot}; exposed so callers may compute error bounds. */
  public final float[] rotatedQuery() {
    return rotatedQuery.clone();
  }

  /**
   * Build per-cluster query state from an already-rotated centroid {@code cRot = P(c)}. The float
   * kernel consumes {@code wRot = qRot - cRot} and its sum. No rotation is performed here — callers
   * must rotate (and preferably cache) centroids once, never per {@code q - c}.
   */
  public final ClusterQuery forRotatedCentroid(float[] rotatedCentroid) {
    int n = rotatedQuery.length;
    if (rotatedCentroid.length != n) {
      throw new IllegalArgumentException(
          "Rotated centroid dimension mismatch: expected " + n + ", got " + rotatedCentroid.length);
    }
    float[] residualQuery = new float[n];
    for (int i = 0; i < n; i++) {
      residualQuery[i] = rotatedQuery[i] - rotatedCentroid[i];
    }
    double wNormSq = sqNorm(residualQuery);
    return new ClusterQuery(
        residualQuery,
        sum(residualQuery),
        exactTerms(rotatedCentroid, wNormSq),
        wNormSq);
  }

  /** Exact per-metric terms; {@code rotatedCentroid} is {@code P(c)}, {@code wNormSq = ||q - c||^2}. */
  protected abstract ExactTerms exactTerms(float[] rotatedCentroid, double wNormSq);

  /**
   * Metric-specific ranking value from the residual inner-product estimate {@code ripW}.
   * L2 uses squared Euclidean distance to avoid a square root in the posting scan; cosine and
   * dot-product use the same units as {@link VectorDistanceMetric}.
   */
  public abstract double rankingDistance(double ripW, float centerRip, float residualNorm,
                                  float vectorNorm, ClusterQuery cq);

  /** Optimistic ranking value for pass-1 pruning, using the residual-query norm. */
  public final double optimisticRankingDistance(double ripW, float err1, float centerRip,
                                         float residualNorm, float vectorNorm,
                                         ClusterQuery cq) {
    double errAbs = (double) err1 * Math.sqrt(cq.wNormSq);
    return rankingDistance(ripW + errAbs, centerRip, residualNorm, vectorNorm, cq);
  }

  public static final class ClusterQuery implements Serializable {
    private static final long serialVersionUID = 2L;
    public final float[] rotatedQuery;
    public final float querySum;
    public final ExactTerms terms;
    public final double wNormSq;
    public final boolean queryAtCentroid;

    ClusterQuery(float[] rotatedQuery, float querySum, ExactTerms terms, double wNormSq) {
      this.rotatedQuery = rotatedQuery;
      this.querySum = querySum;
      this.terms = terms;
      this.wNormSq = wNormSq;
      this.queryAtCentroid = wNormSq < EPS_NORM * EPS_NORM;
    }
  }

  public static final class ExactTerms implements Serializable {
    private static final long serialVersionUID = 2L;
    public final double qDotC;

    ExactTerms(double qDotC) {
      this.qDotC = qDotC;
    }
  }

  // --------------------------------------------------------------------------------------

  public static MetricQueryState create(VectorDistanceMetric metric, QueryRotation rotation,
                                        float[] paddedQuery,
                                        boolean assumeNormalized) {
    if (metric == null || rotation == null || paddedQuery == null) {
      throw new IllegalArgumentException("metric, rotation, and paddedQuery must not be null");
    }
    for (int i = 0; i < paddedQuery.length; i++) {
      if (!Float.isFinite(paddedQuery[i])) {
        throw new IllegalArgumentException("paddedQuery contains a non-finite value at dimension " + i);
      }
    }
    MetricQueryState state;
    switch (metric) {
      case L2:
        state = new L2QueryState(rotation, paddedQuery);
        break;
      case DOT_PRODUCT:
        state = new DotQueryState(rotation, paddedQuery);
        break;
      case COSINE:
        state = new CosineQueryState(rotation, paddedQuery, assumeNormalized);
        break;
      default:
        throw new IllegalArgumentException("Unsupported metric: " + metric);
    }
    state.validate();
    return state;
  }

  // --------------------------------------------------------------------------------------

  /** d2 = ||q - c||^2 + n^2 - 2 * ripW. */
  public static final class L2QueryState extends MetricQueryState {
    L2QueryState(QueryRotation rotation, float[] paddedQuery) {
      super(rotation, paddedQuery);
    }

    @Override
    protected ExactTerms exactTerms(float[] rotatedCentroid, double wNormSq) {
      return new ExactTerms(0.0);
    }

    @Override
    public double rankingDistance(double ripW, float centerRip, float residualNorm,
                           float vectorNorm, ClusterQuery cq) {
      if (cq.queryAtCentroid) {
        return (double) residualNorm * residualNorm;   // exact; scanner may skip the kernel
      }
      return cq.wNormSq + (double) residualNorm * residualNorm - 2.0 * ripW;
    }
  }

  /** Dot-product distance finish. Zero query rejected. */
  public static final class DotQueryState extends MetricQueryState {
    DotQueryState(QueryRotation rotation, float[] paddedQuery) {
      super(rotation, paddedQuery);
    }

    @Override
    public void validate() {
      if (queryNorm < 1.0e-9f) {
        throw new IllegalArgumentException("DOT_PRODUCT query must be non-zero.");
      }
    }

    @Override
    protected ExactTerms exactTerms(float[] rotatedCentroid, double wNormSq) {
      return new ExactTerms(dot(rotatedQuery, rotatedCentroid));
    }

    @Override
    public double rankingDistance(double ripW, float centerRip, float residualNorm,
                           float vectorNorm, ClusterQuery cq) {
      return -(cq.terms.qDotC + centerRip + ripW);
    }
  }

  /** Cosine distance finish. Zero query rejected. */
  public static final class CosineQueryState extends MetricQueryState {
    private final boolean assumeNormalized;

    CosineQueryState(QueryRotation rotation, float[] paddedQuery, boolean assumeNormalized) {
      super(rotation, paddedQuery);
      this.assumeNormalized = assumeNormalized;
    }

    @Override
    public void validate() {
      if (queryNorm < 1.0e-9f) {
        throw new IllegalArgumentException("COSINE query must be non-zero.");
      }
    }

    @Override
    public boolean requiresVectorNorm() {
      return !assumeNormalized;
    }

    @Override
    protected ExactTerms exactTerms(float[] rotatedCentroid, double wNormSq) {
      return new ExactTerms(dot(rotatedQuery, rotatedCentroid));
    }

    @Override
    public double rankingDistance(double ripW, float centerRip, float residualNorm,
                           float vectorNorm, ClusterQuery cq) {
      double xNorm = assumeNormalized ? 1.0 : vectorNorm;
      if (xNorm < EPS_NORM) {
        return 1.0;                                    // zero vector: orthogonal by convention
      }
      return 1.0 - (cq.terms.qDotC + centerRip + ripW) / (queryNorm * xNorm);
    }
  }

  // --------------------------------------------------------------------------------------

  private static float[] validateRotationResult(float[] result, int expectedLength, String name) {
    if (result == null || result.length != expectedLength) {
      throw new IllegalArgumentException("Rotation returned an invalid " + name + " dimension");
    }
    for (int i = 0; i < result.length; i++) {
      if (!Float.isFinite(result[i])) {
        throw new IllegalArgumentException("Rotation returned a non-finite " + name + " value at " + i);
      }
    }
    return result.clone();
  }

  private static double sqNorm(float[] v) {
    double s = 0.0;
    for (float x : v) {
      s += (double) x * x;
    }
    return s;
  }

  private static double dot(float[] a, float[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      s += (double) a[i] * b[i];
    }
    return s;
  }

  private static float sum(float[] values) {
    float sum = 0.0f;
    for (float value : values) {
      sum += value;
    }
    return sum;
  }

  public interface QueryRotation extends Serializable {
    float[] apply(float[] vector);
  }
}
