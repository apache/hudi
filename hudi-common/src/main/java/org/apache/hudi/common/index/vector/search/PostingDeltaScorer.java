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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.common.index.vector.MetricQueryState;
import org.apache.hudi.common.index.vector.RaBitQEncoder;

import java.nio.ByteBuffer;

/** Scores one live posting delta using the same metric-neutral residual contract as packed rows. */
public final class PostingDeltaScorer {

  private PostingDeltaScorer() {
  }

  public static VectorCandidate score(HoodieVectorIndexPostingDelta delta,
                                      MetricQueryState queryState,
                                      float[] centroid,
                                      int dimension,
                                      int rabitqBits,
                                      int generationId,
                                      int clusterId,
                                      int shardId) {
    CodeRows rows = splitCode(delta.getBinaryCode(), dimension, rabitqBits);
    MetricQueryState.ClusterQuery clusterQuery =
        queryState.forRotatedCentroid(queryState.rotateCentroid(centroid));
    float dotTerm = RaBitQEncoder.multibitDotTerm(
        clusterQuery.rotatedQuery, clusterQuery.querySum,
        rows.sign, rows.extended, dimension, rabitqBits);
    double rip = delta.getFRescaleEx() * (double) dotTerm;
    float vectorNorm = delta.getVectorNorm() == null ? Float.NaN : delta.getVectorNorm();
    double distance = queryState.rankingDistance(
        rip, delta.getFAddEx(), delta.getResidualNorm(), vectorNorm, clusterQuery);
    return new VectorCandidate(
        delta.getRecordKey().toString(), clusterId, shardId, distance,
        new VectorPostingLocator(
            generationId, clusterId, shardId, -1L, -1,
            delta.getPartitionPath().toString(), delta.getFileGroupId().toString(),
            delta.getBaseInstantTime().toString(), delta.getRowPosition()));
  }

  private static CodeRows splitCode(ByteBuffer code, int dimension, int rabitqBits) {
    int signBytes = (dimension + 7) / 8;
    int extendedBytes = (dimension * Math.max(0, rabitqBits - 1) + 7) / 8;
    ByteBuffer source = code.duplicate();
    if (source.remaining() != signBytes + extendedBytes) {
      throw new IllegalArgumentException(
          "Posting delta code length mismatch: expected=" + (signBytes + extendedBytes)
              + ", actual=" + source.remaining());
    }
    byte[] sign = new byte[signBytes];
    byte[] extended = new byte[extendedBytes];
    source.get(sign);
    source.get(extended);
    return new CodeRows(sign, extended);
  }

  private static final class CodeRows {
    private final byte[] sign;
    private final byte[] extended;

    private CodeRows(byte[] sign, byte[] extended) {
      this.sign = sign;
      this.extended = extended;
    }
  }
}
