/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

/**
 * Metric-NEUTRAL factor computation for residual RaBitQ encoding (RFC-109 v3 §3, §16).
 *
 * <p>The stored factors commit to NO metric; L2 / DOT / COSINE are composed at query time by
 * {@link MetricQueryState}. Factor layout (version 2):
 * <pre>
 *   CENTER_RIP     = &lt;c_rot, r&gt;               exact, pass-independent
 *   F_RESCALE_1    = n^2 / ipResidual1          pass-1 rescale
 *   ERR_1          = n * eps1  (or n)           pass-1 absolute-IP error scale
 *   F_RESCALE_EX   = n^2 / ipResidualEx         pass-2 rescale
 *   RESIDUAL_NORM  = n
 *   [ VECTOR_NORM ] = ||x||                     raw-cosine generations only
 * </pre>
 *
 * <p>Corrections over the previous implementation:
 * <ul>
 *   <li><b>No invalid error cap.</b> The old {@code ERR_1 = n * min(1, eps1)} could report a
 *       bound smaller than the true estimation error and falsely prune true neighbours. When the
 *       relative error {@code eps1} exceeds {@link RaBitQFactorConfig#getEps1Max()} the estimator
 *       is disabled and the maximal valid bound {@code ERR_1 = n} is used instead.</li>
 *   <li><b>Normalized quality gate.</b> The absolute {@code |ipResidual| < EPS_IP} gate is replaced
 *       by a normalized alignment gate {@code gHat1 < gMin}.</li>
 *   <li><b>Exact-zero vs small-nonzero residual.</b> Only an exactly-zero residual uses
 *       {@code ERR_1 = 0}; a small nonzero residual (norm {@code <= epsNRel * ||x||}) disables the
 *       estimator with {@code ERR_1 = residualNorm}.</li>
 *   <li><b>dimPadded loops.</b> Norms, center-rip, and alignment use the padded dimension; padded
 *       coordinates are zero in {@code r} and codes so they contribute nothing but keep the code
 *       geometry ({@code ||code1|| = 0.5 * sqrt(dimPadded)}) correct.</li>
 * </ul>
 */
public final class RaBitQNeutralFactors {

  public static final double EPS_NORM = 1.0e-9;

  private RaBitQNeutralFactors() {
  }

  /** Immutable per-vector factor set, in block scalar-array order, tagged with its factor version. */
  public static final class Factors {
    public final int factorVersion;
    public final float centerRip;
    public final float fRescale1;
    public final float err1;
    public final float fRescaleEx;
    public final float residualNorm;
    public final float vectorNorm;     // consumed only for raw-cosine generations

    Factors(int factorVersion, float centerRip, float fRescale1, float err1, float fRescaleEx,
            float residualNorm, float vectorNorm) {
      this.factorVersion = factorVersion;
      this.centerRip = centerRip;
      this.fRescale1 = fRescale1;
      this.err1 = err1;
      this.fRescaleEx = fRescaleEx;
      this.residualNorm = residualNorm;
      this.vectorNorm = vectorNorm;
    }
  }

  /**
   * @param residual r = rotated(x) - rotatedCenter, full precision (zero in padded dims)
   * @param rotatedCenter P * c
   * @param rotatedVector P * x (for ||x||; rotation preserves the norm)
   * @param ipResidual1 &lt;r, code1&gt; accumulated in the encode loop
   * @param ipResidualEx &lt;r, codeEx&gt; accumulated in the encode loop
   * @param dimPadded padded dimension D'; padded coords are zero in r and codes
   * @param config generation factor configuration (thresholds + version)
   */
  public static Factors compute(float[] residual, float[] rotatedCenter, float[] rotatedVector,
                                double ipResidual1, double ipResidualEx, int dimPadded,
                                RaBitQFactorConfig config) {
    double nSq = 0.0;
    double centerRip = 0.0;
    double vSq = 0.0;
    for (int i = 0; i < dimPadded; i++) {
      nSq += (double) residual[i] * residual[i];
      centerRip += (double) rotatedCenter[i] * residual[i];
      vSq += (double) rotatedVector[i] * rotatedVector[i];
    }
    double n = Math.sqrt(nSq);
    float vectorNorm = (float) Math.sqrt(vSq);
    int version = config.getFactorVersion();

    // Residual-norm tiers (RFC-109 v3 §2/§3).
    if (n == 0.0) {
      // Vector coincides with centroid exactly: composition is exact; ERR_1 = 0 is legitimate.
      return new Factors(version, (float) centerRip, 0f, 0f, 0f, 0f, vectorNorm);
    }
    if (n <= config.getEpsNRel() * vectorNorm) {
      // Tiny but nonzero residual: disable the estimator, ERR_1 = residualNorm (maximal valid bound).
      return new Factors(version, (float) centerRip, 0f, (float) n, 0f, (float) n, vectorNorm);
    }

    // Normalized alignment of the residual with the sign code; ||code1|| = 0.5 * sqrt(dimPadded).
    double gHat1 = ipResidual1 / (n * 0.5 * Math.sqrt(dimPadded));

    float fRescale1;
    float err1;
    if (gHat1 < config.getGMin()) {
      // Alignment too weak to trust the estimator (also guards the divide): disable, maximal bound.
      fRescale1 = 0f;
      err1 = (float) n;
    } else {
      double eps1 = config.getKappa() * Math.sqrt(
          Math.max(0.0, 1.0 - gHat1 * gHat1) / (gHat1 * gHat1 * (dimPadded - 1)));
      if (eps1 > config.getEps1Max()) {
        // Relative error exceeds budget: disabling is the ONLY valid choice (never clamp to 1).
        fRescale1 = 0f;
        err1 = (float) n;
      } else {
        fRescale1 = (float) (nSq / ipResidual1);
        err1 = (float) (n * eps1);
      }
    }

    // Pass-2 rescale; relative guard against a degenerate extended inner product (avoids Inf/NaN).
    float fRescaleEx = Math.abs(ipResidualEx) <= EPS_NORM * Math.max(1.0, nSq)
        ? 0f : (float) (nSq / ipResidualEx);

    return new Factors(version, (float) centerRip, fRescale1, err1, fRescaleEx, (float) n, vectorNorm);
  }
}
