/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.index.vector;

import java.nio.ByteBuffer;

/**
 * Byte-lookup-table posting scorer (RFC-109 §3A, fixes 1 and 2).
 *
 * <p><b>Exact float-query semantics.</b> This scorer scores the same float rotated (residual)
 * query used by {@link RaBitQEncoder#dotPackedBinary} and {@link RaBitQEncoder#multibitDotTerm}.
 * It introduces <em>no</em> query quantization (unlike {@link RaBitQPlaneKernel}, which quantizes
 * the query to {@code Bq} planes and therefore changes recall math). It only re-associates the
 * per-dimension sum {@code dot(q, code)} into per-byte partial sums precomputed once per probed
 * cluster, which lets the scan replace:
 * <ul>
 *   <li>the pass-1 per-dimension sign loop ({@code dotSignRow}); and</li>
 *   <li>the pass-2 per-survivor {@code copyBuffer(signRow)} + {@code repackExtendedLevels} +
 *       {@code multibitDotTerm} allocation dance</li>
 * </ul>
 * with table lookups and zero per-survivor allocation. Results match the scalar path up to
 * floating-point re-association (byte grouping), never a semantic (quantization) change.
 *
 * <p><b>LUT layout.</b> {@code lut[bytePos][pattern]} holds the sum of {@code query[bytePos*8 + b]}
 * over the set bits {@code b} of {@code pattern}, with padding dimensions ({@code >= dimension})
 * contributing zero so the byte-grouped sum equals the {@code [0, dimension)} scalar sum. A packed
 * plane dot is then {@code sum(lut[bytePos][planeByte])} over byte positions — {@code codeRowBytes}
 * lookups instead of {@code dimension} branchy bit tests (an ~8x op reduction at any dimension).
 */
public final class RaBitQByteLutScorer {

  private final double[][] lut; // [codeRowBytes][256]
  private final int codeRowBytes;
  private final float querySum;

  private RaBitQByteLutScorer(double[][] lut, int codeRowBytes, float querySum) {
    this.lut = lut;
    this.codeRowBytes = codeRowBytes;
    this.querySum = querySum;
  }

  /**
   * Build the per-cluster LUT from the (residual) rotated query. Called at most once per distinct
   * probed cluster; build cost is {@code codeRowBytes * 256} adds, amortized over the cluster's
   * posting scan.
   *
   * @param rotatedQuery the rotated residual query {@code wRot} (length {@code >= dimension})
   * @param querySum     {@code sum(wRot)}; folded into the pass-1/pass-2 centering terms
   * @param dimension    raw dimension scored (padding dims contribute zero)
   * @param codeRowBytes long-aligned per-plane row width from the block layout
   */
  public static RaBitQByteLutScorer forQuery(float[] rotatedQuery, float querySum,
                                             int dimension, int codeRowBytes) {
    if (rotatedQuery == null || dimension <= 0 || rotatedQuery.length < dimension) {
      throw new IllegalArgumentException("rotatedQuery must contain every scored dimension");
    }
    if (!Float.isFinite(querySum) || codeRowBytes < (dimension + Byte.SIZE - 1) / Byte.SIZE) {
      throw new IllegalArgumentException("querySum and codeRowBytes must match the scored query");
    }
    for (int i = 0; i < dimension; i++) {
      if (!Float.isFinite(rotatedQuery[i])) {
        throw new IllegalArgumentException("rotatedQuery contains a non-finite value at dimension " + i);
      }
    }
    double[][] lut = new double[codeRowBytes][256];
    for (int bytePos = 0; bytePos < codeRowBytes; bytePos++) {
      int baseDim = bytePos << 3;
      double[] bitContribution = new double[8];
      for (int b = 0; b < 8; b++) {
        int dim = baseDim + b;
        bitContribution[b] = dim < dimension ? rotatedQuery[dim] : 0.0;
      }
      double[] table = lut[bytePos];
      for (int pattern = 0; pattern < 256; pattern++) {
        double sum = 0.0;
        // Ascending bit order mirrors dotSignRow's ascending-dimension accumulation.
        for (int b = 0; b < 8; b++) {
          if ((pattern & (1 << b)) != 0) {
            sum += bitContribution[b];
          }
        }
        table[pattern] = sum;
      }
    }
    return new RaBitQByteLutScorer(lut, codeRowBytes, querySum);
  }

  /**
   * Raw packed-plane inner product {@code dot(query, plane)} read directly from a plane buffer at an
   * absolute byte offset. Equivalent to {@link RaBitQEncoder#dotPackedBinary} for the sign plane.
   */
  public double planeDot(ByteBuffer planeBuffer, int offset) {
    if (planeBuffer == null || offset < 0 || offset > planeBuffer.limit() - codeRowBytes) {
      throw new IllegalArgumentException("Plane row exceeds the supplied buffer");
    }
    double sum = 0.0;
    for (int bytePos = 0; bytePos < codeRowBytes; bytePos++) {
      sum += lut[bytePos][planeBuffer.get(offset + bytePos) & 0xFF];
    }
    return sum;
  }

  /**
   * Pass-1 sign-only score {@code dot(query, sign) - 0.5*sumQuery} (== {@code dotSignRow}). Callers
   * that also run pass-2 should keep the {@link #planeDot} sign value and reuse it via
   * {@link #pass1FromDot(double)} and {@link #pass2(double, PostingBlockView, ByteBuffer, int, int, int)}
   * rather than recomputing the sign dot.
   */
  public float pass1(ByteBuffer signBuffer, int signOffset) {
    return pass1FromDot(planeDot(signBuffer, signOffset));
  }

  /** Pass-1 score from an already-computed sign-plane dot (see {@link #planeDot}). */
  public float pass1FromDot(double signDot) {
    return (float) (signDot + querySum * -0.5f);
  }

  /**
   * Pass-2 full multibit dot term, scored directly from the sign plane and extended bit-planes
   * with no repacking and no per-survivor allocation. Bit-plane decomposition of the centered
   * code makes this algebraically identical to {@link RaBitQEncoder#multibitDotTerm}:
   * <pre>
   *   extendedDot = sum_p 2^(exBits-1-p) * dot(query, exPlane_p)
   *   dotTerm     = 2^exBits * signDot + extendedDot + sumQuery * -((2^bits - 1)/2)
   * </pre>
   *
   * @param signDot     the raw sign-plane dot from {@link #planeDot} (reuse the pass-1 value)
   * @param view        the posting block view (for extended-plane offsets)
   * @param exBuffer    the extended-planes buffer ({@link PostingBlockView#exPlanesBuffer()})
   * @param vectorIndex the vector ordinal within the block
   * @param exBits      number of extended planes ({@code bits - 1})
   * @param bits        total RaBitQ bits
   */
  public float pass2(double signDot, PostingBlockView view, ByteBuffer exBuffer,
                     int vectorIndex, int exBits, int bits) {
    if (view == null || exBuffer == null || bits < 1 || bits > 8 || exBits != bits - 1) {
      throw new IllegalArgumentException("Posting view and a consistent 1-8 bit width are required");
    }
    if (view.codeRowBytes() != codeRowBytes || view.numExPlanes() != exBits) {
      throw new IllegalArgumentException("Scorer and posting block layouts do not match");
    }
    view.signPlaneOffset(vectorIndex); // validates the vector index before any early return
    if (exBits <= 0) {
      return (float) (signDot + querySum * -0.5f);
    }
    double extendedDot = 0.0;
    for (int p = 0; p < exBits; p++) {
      extendedDot += (double) (1L << (exBits - 1 - p)) * planeDot(exBuffer, view.exPlaneOffset(vectorIndex, p));
    }
    double cBias = -((double) ((1 << bits) - 1)) / 2.0;
    return (float) (((double) (1 << exBits)) * signDot + extendedDot + querySum * cBias);
  }
}
