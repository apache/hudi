/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end recall validation of the corrected math path (RFC-109 §3 + §3), simulating a
 * re-bootstrap: encodes an IVF-residual multibit index with the new {@link RaBitQNeutralFactors}
 * (posting-block format 1), scores queries through the rotate-once {@link MetricQueryState} +
 * {@link RaBitQEncoder#multibitDotTerm} estimator, and measures recall@10 against brute-force exact
 * L2 truth.
 *
 * <p>This is deliberately non-circular: the pass/fail comparison is against exact L2, not against
 * the estimator itself. It confirms the rotate-once query math and the corrected factors do not
 * regress candidate-set recall on SIFT-like clustered data.
 */
public class TestRaBitQResidualRecall {

  private static final int DIM = 128;
  private static final int NUM_CLUSTERS = 50;
  private static final int PER_CLUSTER = 200;
  private static final int N = NUM_CLUSTERS * PER_CLUSTER; // 10k base vectors
  private static final int NUM_QUERIES = 100;
  private static final int K = 10;
  private static final int BITS = 4;
  private static final long SEED = 7L;

  @Test
  public void residualMultibitRecallMeetsFloorOnCorrectedPath() {
    Random rng = new Random(SEED);

    // SIFT-like clustered, non-negative, large-norm data.
    float[][] centers = new float[NUM_CLUSTERS][DIM];
    for (int c = 0; c < NUM_CLUSTERS; c++) {
      for (int d = 0; d < DIM; d++) {
        centers[c][d] = 20f + rng.nextFloat() * 160f;
      }
    }
    float[][] data = new float[N][DIM];
    int[] assign = new int[N];
    int idx = 0;
    for (int c = 0; c < NUM_CLUSTERS; c++) {
      for (int p = 0; p < PER_CLUSTER; p++) {
        float[] v = new float[DIM];
        for (int d = 0; d < DIM; d++) {
          v[d] = Math.max(0f, centers[c][d] + (float) (rng.nextGaussian() * 15.0));
        }
        data[idx] = v;
        assign[idx] = c;
        idx++;
      }
    }

    // Re-bootstrap the residual multibit index with the corrected factors.
    RaBitQEncoder enc = new RaBitQEncoder(DIM, BITS, 42L, false);
    QuantizedVector[] codes = new QuantizedVector[N];
    for (int i = 0; i < N; i++) {
      codes[i] = enc.encodeResidual(data[i], centers[assign[i]]);
    }
    // Confirm the new factor layout is actually being produced.
    assertTrue(codes[0].rescaleFactor != null && codes[0].additiveFactor != null,
        "residual multibit encoding must produce neutral factors");

    double recallSum = 0;
    for (int qi = 0; qi < NUM_QUERIES; qi++) {
      int c = rng.nextInt(NUM_CLUSTERS);
      float[] q = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        q[d] = Math.max(0f, centers[c][d] + (float) (rng.nextGaussian() * 15.0));
      }
      int[] truth = exactTopK(q, data, K);

      // Rotate-once query state; rotate each centroid once (corrected §2 path).
      MetricQueryState state =
          MetricQueryState.create(VectorDistanceMetric.L2, enc::rotateVector, q, false);
      MetricQueryState.ClusterQuery[] cqByCluster = new MetricQueryState.ClusterQuery[NUM_CLUSTERS];
      for (int cc = 0; cc < NUM_CLUSTERS; cc++) {
        cqByCluster[cc] = state.forRotatedCentroid(state.rotateCentroid(centers[cc]));
      }

      float[] approx = new float[N];
      for (int i = 0; i < N; i++) {
        MetricQueryState.ClusterQuery cq = cqByCluster[assign[i]];
        float dotTerm = RaBitQEncoder.multibitDotTerm(
            cq.rotatedQuery, cq.querySum, codes[i].code, codes[i].extendedCode, DIM, BITS);
        double rip = (codes[i].rescaleFactor == null ? 0.0 : codes[i].rescaleFactor) * (double) dotTerm;
        float centerRip = codes[i].additiveFactor == null ? 0f : codes[i].additiveFactor;
        float residualNorm = codes[i].scalar;
        float vectorNorm = codes[i].vectorNorm == null ? Float.NaN : codes[i].vectorNorm;
        approx[i] = (float) state.rankingDistance(rip, centerRip, residualNorm, vectorNorm, cq);
      }
      recallSum += recall(topKByDist(approx, K), truth);
    }
    double recall = recallSum / NUM_QUERIES;
    System.out.printf("[RFC-109] residual multibit recall@%d (B=%d) = %.3f%n", K, BITS, recall);

    // Floor for 4-bit residual multibit on this easy synthetic corpus (all clusters probed).
    assertTrue(recall >= 0.85,
        "corrected-path recall@" + K + " regressed below floor: " + recall);
  }

  private static float l2sq(float[] a, float[] b) {
    float s = 0;
    for (int i = 0; i < a.length; i++) {
      float d = a[i] - b[i];
      s += d * d;
    }
    return s;
  }

  private static int[] exactTopK(float[] q, float[][] data, int k) {
    float[] d = new float[data.length];
    for (int i = 0; i < data.length; i++) {
      d[i] = l2sq(q, data[i]);
    }
    return topKByDist(d, k);
  }

  private static int[] topKByDist(float[] dist, int k) {
    Integer[] order = new Integer[dist.length];
    for (int i = 0; i < dist.length; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (x, y) -> Float.compare(dist[x], dist[y]));
    int[] out = new int[k];
    for (int i = 0; i < k; i++) {
      out[i] = order[i];
    }
    return out;
  }

  private static double recall(int[] got, int[] truth) {
    List<Integer> t = new ArrayList<>();
    for (int x : truth) {
      t.add(x);
    }
    int hit = 0;
    for (int g : got) {
      if (t.contains(g)) {
        hit++;
      }
    }
    return (double) hit / truth.length;
  }
}
