/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the rotate-once query-math contract (RFC-104 v3 §2): the padded query is rotated exactly
 * once, {@code forRotatedCentroid} forms {@code wRot = qRot - cRot} by subtraction (never rotating
 * {@code q - c} per cluster), and exact terms are computed in rotated space with inner-product
 * invariance under an orthogonal rotation.
 */
public class TestMetricQueryStateRotation {

  /** Orthogonal rotation: swap adjacent coordinate pairs (a permutation, so norm/IP preserving). */
  private static final MetricQueryState.QueryRotation SWAP_PAIRS = v -> {
    float[] out = new float[v.length];
    for (int i = 0; i + 1 < v.length; i += 2) {
      out[i] = v[i + 1];
      out[i + 1] = v[i];
    }
    if (v.length % 2 == 1) {
      out[v.length - 1] = v[v.length - 1];
    }
    return out;
  };

  private int rotationCalls;

  @Test
  void queryIsRotatedExactlyOnceRegardlessOfClusterCount() {
    rotationCalls = 0;
    MetricQueryState.QueryRotation counting = v -> {
      rotationCalls++;
      return SWAP_PAIRS.apply(v);
    };
    float[] q = {1f, 2f, 3f, 4f};
    MetricQueryState state =
        MetricQueryState.create(VectorDistanceMetric.L2, counting, q, false);
    // Constructor rotates the query once; probing many clusters must add no further query rotations.
    for (int c = 0; c < 100; c++) {
      state.forRotatedCentroid(new float[] {0f, 0f, 0f, 0f});
    }
    assertEquals(1, rotationCalls, "query must be rotated exactly once");
  }

  @Test
  void rotatedQueryMatchesRotationOfQuery() {
    float[] q = {1f, 2f, 3f, 4f};
    MetricQueryState state =
        MetricQueryState.create(VectorDistanceMetric.L2, SWAP_PAIRS, q, false);
    assertArrayEquals(new float[] {2f, 1f, 4f, 3f}, state.rotatedQuery(), 1e-6f);
  }

  @Test
  void forRotatedCentroidSubtractsInRotatedSpace() {
    float[] q = {1f, 2f, 3f, 4f};
    MetricQueryState state =
        MetricQueryState.create(VectorDistanceMetric.L2, SWAP_PAIRS, q, false);
    float[] rawCentroid = {0.5f, 0.5f, 0.5f, 0.5f};
    float[] cRot = state.rotateCentroid(rawCentroid);
    MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(cRot);
    // wRot = qRot - cRot = [2,1,4,3] - [0.5,0.5,0.5,0.5]
    assertArrayEquals(new float[] {1.5f, 0.5f, 3.5f, 2.5f}, cq.rotatedQuery, 1e-6f);
    assertEquals(1.5f + 0.5f + 3.5f + 2.5f, cq.querySum, 1e-6f);
  }

  @Test
  void innerProductIsInvariantUnderOrthogonalRotation() {
    float[] q = {1f, 2f, 3f, 4f};
    float[] rawCentroid = {0.5f, 0.5f, 0.5f, 0.5f};
    MetricQueryState state =
        MetricQueryState.create(VectorDistanceMetric.DOT_PRODUCT, SWAP_PAIRS, q, false);
    MetricQueryState.ClusterQuery cq = state.forRotatedCentroid(state.rotateCentroid(rawCentroid));
    // <qRot, cRot> must equal raw <q, c> = 0.5*(1+2+3+4) = 5.0 for an orthogonal rotation.
    assertEquals(5.0, cq.terms.qDotC, 1e-6);
  }

  @Test
  void rotatedCentroidDimensionMismatchThrows() {
    float[] q = {1f, 2f, 3f, 4f};
    MetricQueryState state =
        MetricQueryState.create(VectorDistanceMetric.L2, SWAP_PAIRS, q, false);
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> state.forRotatedCentroid(new float[] {0f, 0f}));
  }
}
