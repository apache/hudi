/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import java.io.Serializable;

/**
 * Generation-scoped configuration for RaBitQ neutral-factor computation (RFC-109 §3).
 *
 * <p>Replaces the previously static constants ({@code ERR_KAPPA}, absolute {@code EPS_IP}) with
 * explicit, versioned thresholds. Readers reject factor versions they do not support instead of
 * interpreting persisted factors with current-code constants.
 *
 * <ul>
 *   <li>{@code kappa} — pass-1 error scale (was {@code ERR_KAPPA}).</li>
 *   <li>{@code gMin} — normalized alignment floor: when the sign-code alignment
 *       {@code gHat1 < gMin} the pass-1 estimator is disabled (replaces the absolute
 *       {@code |ipResidual| < EPS_IP} gate).</li>
 *   <li>{@code eps1Max} — maximum permitted relative pass-1 error; above it the estimator is
 *       disabled and the maximal valid bound {@code ERR_1 = residualNorm} is used, instead of the
 *       old (invalid) {@code min(1, eps1)} clamp that could falsely prune true neighbours.</li>
 *   <li>{@code epsNRel} — relative residual-norm floor: a vector whose residual norm is in
 *       {@code (0, epsNRel * vectorNorm]} is treated as coincident-with-centroid; the estimator is
 *       disabled with {@code ERR_1 = residualNorm}. Only an exact-zero residual uses
 *       {@code ERR_1 = 0}.</li>
 * </ul>
 */
public final class RaBitQFactorConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int FACTOR_VERSION = 1;
  public static final double DEFAULT_KAPPA = 1.9;
  public static final double DEFAULT_GMIN = 1.0e-3;
  public static final double DEFAULT_EPS1_MAX = 1.0;
  public static final double DEFAULT_EPS_N_REL = 1.0e-3;

  private final int factorVersion;
  private final double kappa;
  private final double gMin;
  private final double eps1Max;
  private final double epsNRel;

  public RaBitQFactorConfig(int factorVersion, double kappa, double gMin, double eps1Max, double epsNRel) {
    if (factorVersion != FACTOR_VERSION) {
      throw new IllegalArgumentException("Unsupported RaBitQ factor version: " + factorVersion);
    }
    this.factorVersion = factorVersion;
    this.kappa = kappa;
    this.gMin = gMin;
    this.eps1Max = eps1Max;
    this.epsNRel = epsNRel;
  }

  /** The defaults for the first persisted factor format (RFC-109 §3). */
  public static RaBitQFactorConfig defaults() {
    return new RaBitQFactorConfig(
        FACTOR_VERSION, DEFAULT_KAPPA, DEFAULT_GMIN, DEFAULT_EPS1_MAX, DEFAULT_EPS_N_REL);
  }

  public int getFactorVersion() {
    return factorVersion;
  }

  public double getKappa() {
    return kappa;
  }

  public double getGMin() {
    return gMin;
  }

  public double getEps1Max() {
    return eps1Max;
  }

  public double getEpsNRel() {
    return epsNRel;
  }
}
