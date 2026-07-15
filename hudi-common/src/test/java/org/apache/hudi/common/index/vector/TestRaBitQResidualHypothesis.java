/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.index.vector.RaBitQEncoder.RaBitQQueryState;
import org.apache.hudi.common.index.vector.VectorQuantizer.QuantizedVector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC-104 cheap validation experiment (pure JVM, no Spark / no cluster).
 *
 * <p>Goal: prove that approximate-only RaBitQ recall on <b>L2 / large-norm</b> data
 * (BigANN/SIFT-like) is broken when we quantize the <b>full unit vector</b> (current
 * implementation), and is fixed by quantizing the <b>IVF residual</b>
 * {@code x - centroid[cluster]} instead.
 *
 * <p>The math: {@code L2^2 = ||q||^2 + ||x||^2 - 2*||q||*||x||*cos}. With SIFT-scale
 * norms (~1000) a 1-bit cosine-estimate error of +/-0.1 becomes an L2^2 error of
 * {@code ~2*1000^2*0.1 = 200k}, which dwarfs the true gaps between near neighbors ->
 * random ranking. Residuals are small & zero-centered, so the same 1-bit code resolves
 * them, and {@code ||q_res - x_res|| = ||(q-c) - (x-c)|| = ||q - x||} is recovered
 * accurately.
 *
 * <p>This experiment isolates ENCODING QUALITY: both methods probe ALL clusters
 * (no IVF pruning), so the only difference measured is full-vector vs residual.
 */
public class TestRaBitQResidualHypothesis {

  private static final int DIM = 128;        // SIFT dimension
  private static final int NUM_CLUSTERS = 50;
  private static final int PER_CLUSTER = 200;
  private static final int N = NUM_CLUSTERS * PER_CLUSTER; // 10k base vectors
  private static final int NUM_QUERIES = 100;
  private static final int K = 10;           // recall@10
  private static final long SEED = 7L;

  @Test
  public void residualEncodingFixesL2RecallOnLargeNormData() {
    Random rng = new Random(SEED);

    // ---- 1. Synthetic SIFT-like data: clustered, NON-NEGATIVE, LARGE norms ----
    float[][] centers = new float[NUM_CLUSTERS][DIM];
    for (int c = 0; c < NUM_CLUSTERS; c++) {
      for (int d = 0; d < DIM; d++) {
        centers[c][d] = 20f + rng.nextFloat() * 160f; // [20,180] -> norm ~ 1130
      }
    }
    float[][] data = new float[N][DIM];
    int[] assign = new int[N];
    int idx = 0;
    for (int c = 0; c < NUM_CLUSTERS; c++) {
      for (int p = 0; p < PER_CLUSTER; p++) {
        float[] v = new float[DIM];
        for (int d = 0; d < DIM; d++) {
          v[d] = Math.max(0f, centers[c][d] + (float) (rng.nextGaussian() * 15.0)); // residual ~170
        }
        data[idx] = v;
        assign[idx] = c;
        idx++;
      }
    }

    // ---- 2. Encoders / encodings ----
    RaBitQEncoder enc = new RaBitQEncoder(DIM, 42L, false);

    // FULL: encode each full vector (current production behavior)
    QuantizedVector[] fullCodes = new QuantizedVector[N];
    for (int i = 0; i < N; i++) {
      fullCodes[i] = enc.encode(data[i]);
    }

    // RESIDUAL: encode x - centroid[assign(x)]
    QuantizedVector[] resCodes = new QuantizedVector[N];
    for (int i = 0; i < N; i++) {
      resCodes[i] = enc.encode(sub(data[i], centers[assign[i]]));
    }

    // ---- 3. Queries: held-out points generated the same way (have true neighbors) ----
    double recallFullSym = 0;
    double recallFullAsym = 0;
    double recallResSym = 0;
    double recallResAsym = 0;

    for (int qi = 0; qi < NUM_QUERIES; qi++) {
      int c = rng.nextInt(NUM_CLUSTERS);
      float[] q = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        q[d] = Math.max(0f, centers[c][d] + (float) (rng.nextGaussian() * 15.0));
      }

      // ground truth: exact L2 top-K over all N
      int[] truth = exactTopK(q, data, K);

      // ---- FULL method ----
      RaBitQQueryState qsFull = (RaBitQQueryState) enc.encodeQuery(q);
      float[] distFullSym = new float[N];
      float[] distFullAsym = new float[N];
      for (int i = 0; i < N; i++) {
        float symCos = RaBitQEncoder.symmetricCosine(qsFull.binaryCode, fullCodes[i].code, DIM);
        float asymCos = RaBitQEncoder.asymmetricCosine(qsFull.rotatedQuery, fullCodes[i].code, DIM);
        distFullSym[i] = RaBitQEncoder.reconstructDistance(
            VectorDistanceMetric.L2, symCos, qsFull.queryNorm, fullCodes[i].scalar);
        distFullAsym[i] = RaBitQEncoder.reconstructDistance(
            VectorDistanceMetric.L2, asymCos, qsFull.queryNorm, fullCodes[i].scalar);
      }
      recallFullSym += recall(topKByDist(distFullSym, K), truth);
      recallFullAsym += recall(topKByDist(distFullAsym, K), truth);

      // ---- RESIDUAL method (probe ALL clusters; query residual is per-cluster) ----
      RaBitQQueryState[] qResByCluster = new RaBitQQueryState[NUM_CLUSTERS];
      for (int cc = 0; cc < NUM_CLUSTERS; cc++) {
        qResByCluster[cc] = (RaBitQQueryState) enc.encodeQuery(sub(q, centers[cc]));
      }
      float[] distResSym = new float[N];
      float[] distResAsym = new float[N];
      for (int i = 0; i < N; i++) {
        RaBitQQueryState qsRes = qResByCluster[assign[i]];
        float symCos = RaBitQEncoder.symmetricCosine(qsRes.binaryCode, resCodes[i].code, DIM);
        float asymCos = RaBitQEncoder.asymmetricCosine(qsRes.rotatedQuery, resCodes[i].code, DIM);
        distResSym[i] = RaBitQEncoder.reconstructDistance(
            VectorDistanceMetric.L2, symCos, qsRes.queryNorm, resCodes[i].scalar);
        distResAsym[i] = RaBitQEncoder.reconstructDistance(
            VectorDistanceMetric.L2, asymCos, qsRes.queryNorm, resCodes[i].scalar);
      }
      recallResSym += recall(topKByDist(distResSym, K), truth);
      recallResAsym += recall(topKByDist(distResAsym, K), truth);
    }

    recallFullSym /= NUM_QUERIES;
    recallFullAsym /= NUM_QUERIES;
    recallResSym /= NUM_QUERIES;
    recallResAsym /= NUM_QUERIES;

    System.out.println("================ RFC-104 RESIDUAL HYPOTHESIS ================");
    System.out.printf("Data: N=%d D=%d clusters=%d queries=%d (SIFT-like, L2, non-negative)%n",
        N, DIM, NUM_CLUSTERS, NUM_QUERIES);
    System.out.printf("FULL-vector  recall@%d : symmetric=%.3f  asymmetric=%.3f%n", K, recallFullSym, recallFullAsym);
    System.out.printf("RESIDUAL     recall@%d : symmetric=%.3f  asymmetric=%.3f%n", K, recallResSym, recallResAsym);
    System.out.println("============================================================");

    // The hypothesis: residual encoding dramatically beats full-vector on L2/large-norm.
    assertTrue(recallResSym > recallFullSym + 0.3,
        "Residual symmetric recall should massively beat full-vector; got res=" + recallResSym + " full=" + recallFullSym);
    assertTrue(recallResAsym >= recallResSym,
        "Asymmetric should be >= symmetric for residual; got asym=" + recallResAsym + " sym=" + recallResSym);
  }

  // ---- helpers ----

  private static float[] sub(float[] a, float[] b) {
    float[] out = new float[a.length];
    for (int i = 0; i < a.length; i++) {
      out[i] = a[i] - b[i];
    }
    return out;
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

  /** Indices of the k smallest distances (simple selection; N small). */
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
