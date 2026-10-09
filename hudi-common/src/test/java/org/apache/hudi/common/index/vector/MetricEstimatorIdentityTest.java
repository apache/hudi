/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The metric certification suite. Three families:
 *
 * <p>IDENTITY: for random (x, c, q), the query-state composition over the NEUTRAL factors
 * must equal the exact estimator composed in double precision, for both passes and every
 * metric. This pins encoder and scorer to each other; every silent-recall bug this index
 * has had (zero factors, folded conventions, sign drift) fails this test.
 *
 * <p>BOUND: optimisticRankingDistance must never exceed the estimator-composed distance, and the
 * true distance must lie within the propagated bound at the calibrated rate. The direction
 * (rip + errAbs for ALL metrics) is the designated sign-bug site.
 *
 * <p>DEGENERATE: the normative special cases compose to EXACT values.
 *
 * <p>Codes here are synthetic centered codes built directly from the residual (sign code
 * c1_i = +-0.5; multibit codeEx from B-bit grid) so the suite tests the FACTOR/COMPOSITION
 * contract independent of bit-packing; the golden round-trip test (separate) certifies
 * packing. Together they certify the full path.
 */
public class MetricEstimatorIdentityTest {

  private static final int DIM = 96;
  private static final int TRIALS = 200;
  private static final int BITS = 4;
  private static final double REL_TOL = 1e-5;

  @ParameterizedTest
  @EnumSource(value = VectorDistanceMetric.class)
  public void identityBothPasses(VectorDistanceMetric metric) {
    Random rnd = new Random(42);
    for (int t = 0; t < TRIALS; t++) {
      Fixture f = Fixture.random(rnd, DIM, BITS, metric == VectorDistanceMetric.COSINE);

      MetricQueryState state = state(metric, f.q, false);
      MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(f.c);

      // ---- pass 1 ----
      double rip1 = (double) f.factors.fRescale1 * dot(f.residualQuery(), f.code1);
      double got1 = state.rankingDistance(rip1, f.factors.centerRip, f.factors.residualNorm,
          f.factors.vectorNorm, cq);
      double exact1 = exactComposition(metric, f, dot(f.residualQuery(), f.code1), f.ipResidual1, cq);
      assertRel(exact1, got1, "pass1 " + metric + " trial " + t);

      // ---- pass 2 ----
      double ripEx = (double) f.factors.fRescaleEx * dot(f.residualQuery(), f.codeEx);
      double gotEx = state.rankingDistance(ripEx, f.factors.centerRip, f.factors.residualNorm,
          f.factors.vectorNorm, cq);
      double exactEx = exactComposition(metric, f, dot(f.residualQuery(), f.codeEx), f.ipResidualEx, cq);
      assertRel(exactEx, gotEx, "pass2 " + metric + " trial " + t);
    }
  }

  /**
   * Exact estimator composition in double precision, straight from definitions:
   * rip* = n^2 * &lt;q, code&gt; / &lt;r, code&gt;; centroid component exact.
   */
  private static double exactComposition(VectorDistanceMetric metric, Fixture f,
                                         double wDotCode, double ipResidual,
                                         MetricQueryState.ClusterQuery cq) {
    double n = norm(f.r);
    double rip = Math.abs(ipResidual) < 1.0e-12
        ? 0.0 : n * n * wDotCode / ipResidual;
    double centerRip = dot(f.c, f.r);
    switch (metric) {
      case L2:
        return cq.wNormSq + n * n - 2.0 * rip;
      case DOT_PRODUCT:
        return -(dot(f.q, f.c) + centerRip + rip);
      case COSINE:
        return 1.0 - (dot(f.q, f.c) + centerRip + rip) / (norm(f.q) * norm(f.x));
      default:
        throw new AssertionError();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VectorDistanceMetric.class)
  public void optimisticBoundNeverExceedsEstimate(VectorDistanceMetric metric) {
    Random rnd = new Random(7);
    for (int t = 0; t < TRIALS; t++) {
      Fixture f = Fixture.random(rnd, DIM, BITS, metric == VectorDistanceMetric.COSINE);
      MetricQueryState state = state(metric, f.q, false);
      MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(f.c);

      double rip1 = (double) f.factors.fRescale1 * dot(f.residualQuery(), f.code1);
      double est = state.rankingDistance(rip1, f.factors.centerRip, f.factors.residualNorm,
          f.factors.vectorNorm, cq);
      double opt = state.optimisticRankingDistance(rip1, f.factors.err1, f.factors.centerRip,
          f.factors.residualNorm, f.factors.vectorNorm, cq);
      // The sign-bug canary: optimistic must be <= estimate for EVERY metric
      // (larger rip => smaller distance under all three finishes).
      assertTrue(opt <= est + 1e-9,
          metric + " optimistic bound above estimate at trial " + t + ": " + opt + " > " + est);
    }
  }

  @ParameterizedTest
  @EnumSource(value = VectorDistanceMetric.class)
  public void trueDistanceWithinBound(VectorDistanceMetric metric) {
    Random rnd = new Random(11);
    int violations = 0;
    int trials = 2000;
    for (int t = 0; t < trials; t++) {
      Fixture f = Fixture.random(rnd, DIM, BITS, metric == VectorDistanceMetric.COSINE);
      MetricQueryState state = state(metric, f.q, false);
      MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(f.c);

      double rip1 = (double) f.factors.fRescale1 * dot(f.residualQuery(), f.code1);
      double opt = state.optimisticRankingDistance(rip1, f.factors.err1, f.factors.centerRip,
          f.factors.residualNorm, f.factors.vectorNorm, cq);
      double trueDist = trueDistance(metric, f);
      if (opt > trueDist + 1e-9) {
        violations++;
      }
    }
    // kappa = 1.9 targets ~5% one-sided miss rate; allow slack for finite trials/dim.
    assertTrue(violations < trials * 0.08,
        metric + " bound violation rate " + violations + "/" + trials);
  }

  @ParameterizedTest
  @EnumSource(value = VectorDistanceMetric.class)
  public void degenerateVectorAtCentroidIsExact(VectorDistanceMetric metric) {
    Random rnd = new Random(3);
    Fixture f = Fixture.atCentroid(rnd, DIM);
    MetricQueryState state = state(metric, f.q, false);
    MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(f.c);

    double got = state.rankingDistance(0.0 /* rip: fRescale=0 */, f.factors.centerRip,
        f.factors.residualNorm, f.factors.vectorNorm, cq);
    assertRel(trueDistance(metric, f), got, "degenerate " + metric);
    assertEquals(0f, f.factors.err1, 0f, "degenerate err1 must be zero (exact => prunable)");
  }

  @Test
  public void l2QueryAtCentroidIsExact() {
    Random rnd = new Random(5);
    Fixture f = Fixture.random(rnd, DIM, BITS, false);
    MetricQueryState state = state(VectorDistanceMetric.L2, f.c.clone(), false);
    MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(f.c);
    assertTrue(cq.queryAtCentroid);
    double got = state.rankingDistance(123.456 /* rip must be ignored */, f.factors.centerRip,
        f.factors.residualNorm, f.factors.vectorNorm, cq);
    assertRel((double) f.factors.residualNorm * f.factors.residualNorm, got, "q==c exact d2=n2");
  }

  @Test
  public void zeroQueryRejectedForDotAndCosine() {
    float[] zero = new float[DIM];
    assertThrows(IllegalArgumentException.class,
        () -> state(VectorDistanceMetric.DOT_PRODUCT, zero, false));
    assertThrows(IllegalArgumentException.class,
        () -> state(VectorDistanceMetric.COSINE, zero, false));
    state(VectorDistanceMetric.L2, zero, false); // permitted
  }

  // ======================================================================================

  /** Synthetic fixture: x, c, q, residual, both codes, neutral factors. Rotation = identity
   *  (a rotation is an isometry; factor/composition algebra is rotation-invariant, and the
   *  rotation itself is certified by the golden round-trip test). */
  private static final class Fixture {
    final float[] x;
    final float[] c;
    final float[] q;
    final float[] r;
    final float[] code1;
    final float[] codeEx;
    final double ipResidual1;
    final double ipResidualEx;
    final RaBitQNeutralFactors.Factors factors;

    private Fixture(float[] x, float[] c, float[] q, float[] r,
                    float[] code1, float[] codeEx) {
      this.x = x;
      this.c = c;
      this.q = q;
      this.r = r;
      this.code1 = code1;
      this.codeEx = codeEx;
      this.ipResidual1 = dot(r, code1);
      this.ipResidualEx = dot(r, codeEx);
      this.factors = RaBitQNeutralFactors.compute(r, c, x, ipResidual1, ipResidualEx, r.length,
          RaBitQFactorConfig.defaults());
    }

    static Fixture random(Random rnd, int dim, int bits, boolean normalizeX) {
      float[] c = gaussian(rnd, dim, 0.5f);
      float[] x = new float[dim];
      float[] rr = gaussian(rnd, dim, 1.0f);
      for (int i = 0; i < dim; i++) {
        x[i] = c[i] + rr[i];
      }
      if (normalizeX) {
        float inv = (float) (1.0 / norm(x));
        for (int i = 0; i < dim; i++) {
          x[i] *= inv;
        }
      }
      float[] q = gaussian(rnd, dim, 1.0f);
      float[] r = new float[dim];
      for (int i = 0; i < dim; i++) {
        r[i] = x[i] - c[i];
      }
      return new Fixture(x, c, q, r, signCode(r), multibitCode(r, bits));
    }

    static Fixture atCentroid(Random rnd, int dim) {
      float[] c = gaussian(rnd, dim, 0.5f);
      float[] x = c.clone();
      float[] q = gaussian(rnd, dim, 1.0f);
      float[] r = new float[dim];
      return new Fixture(x, c, q, r, signCode(r), multibitCode(r, 4));
    }

    float[] residualQuery() {
      float[] w = new float[q.length];
      for (int i = 0; i < q.length; i++) {
        w[i] = q[i] - c[i];
      }
      return w;
    }
  }

  private static double trueDistance(VectorDistanceMetric metric, Fixture f) {
    switch (metric) {
      case L2: {
        double s = 0.0;
        for (int i = 0; i < f.q.length; i++) {
          double d = (double) f.q[i] - f.x[i];
          s += d * d;
        }
        return s;                                        // squared, matching the finish
      }
      case DOT_PRODUCT:
        return -dot(f.q, f.x);
      case COSINE:
        return 1.0 - dot(f.q, f.x) / (norm(f.q) * norm(f.x));
      default:
        throw new AssertionError();
    }
  }

  private static MetricQueryState state(VectorDistanceMetric metric, float[] query, boolean assumeNormalized) {
    return MetricQueryState.create(metric, vector -> vector.clone(), query, assumeNormalized);
  }

  /** c1_i = +-0.5 following the strict > 0 convention. */
  private static float[] signCode(float[] r) {
    float[] code = new float[r.length];
    for (int i = 0; i < r.length; i++) {
      code[i] = r[i] > 0f ? 0.5f : -0.5f;
    }
    return code;
  }

  /** Centered B-bit grid code of the unit residual (per-dim symmetric grid). */
  private static float[] multibitCode(float[] r, int bits) {
    int dim = r.length;
    float[] code = new float[dim];
    double n = norm(r);
    if (n < RaBitQNeutralFactors.EPS_NORM) {
      return code;
    }
    float m = 0f;
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) (r[i] / n);
      m = Math.max(m, Math.abs(v[i]));
    }
    int levels = (1 << bits) - 1;
    float dx = 2f * m / levels;
    for (int i = 0; i < dim; i++) {
      int lvl = Math.max(0, Math.min(levels, Math.round((v[i] + m) / dx)));
      code[i] = -m + dx * lvl;
    }
    return code;
  }

  private static float[] gaussian(Random rnd, int dim, float scale) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rnd.nextGaussian() * scale;
    }
    return v;
  }

  private static double dot(float[] a, float[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++) {
      s += (double) a[i] * b[i];
    }
    return s;
  }

  private static double norm(float[] v) {
    return Math.sqrt(dot(v, v));
  }

  private static void assertRel(double expected, double actual, String msg) {
    double denom = Math.max(1.0, Math.abs(expected));
    assertEquals(expected, actual, REL_TOL * denom, msg);
  }
}
