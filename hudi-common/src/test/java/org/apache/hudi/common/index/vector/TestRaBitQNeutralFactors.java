/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the corrected RaBitQ neutral-factor semantics (RFC-109 v3 §3): factor-version tagging,
 * removal of the invalid {@code min(1, eps1)} cap, the normalized {@code gMin} quality gate, and
 * the exact-zero vs small-nonzero residual split. These are the degenerate cases the previous
 * implementation handled incorrectly.
 */
public class TestRaBitQNeutralFactors {

  private static final RaBitQFactorConfig CFG = RaBitQFactorConfig.defaultsV2();
  private static final int D = 4;

  /** code1 for a residual: +0.5 where r>0 else -0.5. */
  private static float[] signCode(float[] r) {
    float[] c = new float[r.length];
    for (int i = 0; i < r.length; i++) {
      c[i] = r[i] > 0f ? 0.5f : -0.5f;
    }
    return c;
  }

  private static double dot(float[] a, float[] b) {
    double s = 0;
    for (int i = 0; i < a.length; i++) {
      s += (double) a[i] * b[i];
    }
    return s;
  }

  @Test
  void exactZeroResidualUsesZeroError() {
    float[] r = {0f, 0f, 0f, 0f};
    float[] x = {5f, 0f, 0f, 0f};
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, 0.0, 0.0, D, CFG);
    assertEquals(0f, f.residualNorm, 0f);
    assertEquals(0f, f.err1, 0f, "only an exact-zero residual may report ERR_1 = 0");
    assertEquals(0f, f.fRescale1, 0f);
    assertEquals(RaBitQFactorConfig.FACTOR_VERSION_V2, f.factorVersion);
  }

  @Test
  void smallNonzeroResidualDisablesEstimatorWithResidualNormBound() {
    // residual norm 1e-4 is <= epsNRel(1e-3) * ||x||(100) = 0.1 -> small-nonzero tier.
    float[] r = {1.0e-4f, 0f, 0f, 0f};
    float[] x = {100f, 0f, 0f, 0f};
    double ip1 = dot(r, signCode(r));
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, ip1, ip1, D, CFG);
    assertEquals(0f, f.fRescale1, 0f, "small residual must disable the pass-1 estimator");
    assertEquals(1.0e-4f, f.err1, 1e-9f, "ERR_1 must equal the residual norm, not 0");
    assertTrue(f.err1 > 0f, "a nonzero residual must not report zero error");
  }

  @Test
  void largeRelativeErrorDisablesEstimatorInsteadOfClamping() {
    // r=[1,0,0,0]: gHat1 = 0.5 -> eps1 = 1.9*sqrt(0.75/0.75) = 1.9 > eps1Max(1.0).
    // Old code clamped to min(1,eps1)=1 (invalid); v3 disables the estimator.
    float[] r = {1f, 0f, 0f, 0f};
    float[] x = {1f, 0f, 0f, 0f};
    double ip1 = dot(r, signCode(r)); // = 0.5
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, ip1, ip1, D, CFG);
    assertEquals(0f, f.fRescale1, 0f, "eps1 > eps1Max must disable the estimator");
    assertEquals(1.0f, f.err1, 1e-6f, "ERR_1 must be the maximal valid bound n, not the clamped 1*... ");
  }

  @Test
  void weakAlignmentBelowGMinIsGated() {
    // Drive a near-orthogonal code via an explicitly tiny ipResidual1 (gHat1 << gMin).
    float[] r = {1f, 0f, 0f, 0f};
    float[] x = {1f, 0f, 0f, 0f};
    double tinyIp = 1.0e-9; // gHat1 = 1e-9 / (1 * 0.5 * 2) = 1e-9 < gMin(1e-3)
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, tinyIp, tinyIp, D, CFG);
    assertEquals(0f, f.fRescale1, 0f, "gHat1 < gMin must disable the estimator");
    assertEquals(1.0f, f.err1, 1e-6f, "gated estimator uses ERR_1 = residualNorm");
  }

  @Test
  void perfectAlignmentGivesZeroPass1Error() {
    // r = [1,1,1,1]: gHat1 = 1.0 (sign code perfectly aligned) -> eps1 = 0 -> ERR_1 = 0, enabled.
    float[] r = {1f, 1f, 1f, 1f};
    float[] x = {1f, 1f, 1f, 1f};
    double ip1 = dot(r, signCode(r)); // = 2.0
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, ip1, ip1, D, CFG);
    assertEquals(2.0f, f.fRescale1, 1e-6f, "F_RESCALE_1 = n^2/ip = 4/2");
    assertEquals(0f, f.err1, 1e-6f, "perfect alignment yields zero pass-1 error");
  }

  @Test
  void wellConditionedResidualEnablesEstimatorAndReproducesRip() {
    // r = [3,1,1,1]: gHat1 = 0.866 -> eps1 = 0.63 in (0,1); ERR_1 in (0, n).
    float[] r = {3f, 1f, 1f, 1f};
    float[] x = {3f, 1f, 1f, 1f};
    float[] code1 = signCode(r); // all +0.5
    double ip1 = dot(r, code1);   // = 3.0
    RaBitQNeutralFactors.Factors f =
        RaBitQNeutralFactors.compute(r, new float[D], x, ip1, ip1, D, CFG);
    assertTrue(f.fRescale1 != 0f, "well-conditioned residual must keep the estimator enabled");
    // F_RESCALE_1 = n^2 / ipResidual1 = 12 / 3 = 4.0
    assertEquals(4.0f, f.fRescale1, 1e-6f);
    assertTrue(f.err1 > 0f && f.err1 < f.residualNorm,
        "enabled estimator error must be positive and below the maximal bound n");
  }
}
