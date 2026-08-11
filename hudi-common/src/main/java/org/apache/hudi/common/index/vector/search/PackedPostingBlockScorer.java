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

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.common.index.vector.MetricQueryState;
import org.apache.hudi.common.index.vector.PostingBlockView;
import org.apache.hudi.common.index.vector.RaBitQByteLutScorer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Scores one packed posting block without decoding keys or locators for rejected rows. */
public final class PackedPostingBlockScorer {

  private static final Comparator<ScoredOrdinal> WORST_FIRST = Comparator
      .comparingDouble(ScoredOrdinal::getDistance)
      .thenComparingInt(ScoredOrdinal::getOrdinal)
      .reversed();

  private PackedPostingBlockScorer() {
  }

  public static List<VectorCandidate> score(HoodieVectorIndexPostingBlock block,
                                            MetricQueryState queryState,
                                            float[] centroid,
                                            int dimension,
                                            int rabitqBits,
                                            int candidateLimit,
                                            int generationId,
                                            int clusterId,
                                            int shardId,
                                            long blockId) {
    if (candidateLimit <= 0) {
      return new ArrayList<>();
    }
    PostingBlockView view = new PostingBlockView(block);
    int expectedExPlanes = Math.max(0, rabitqBits - 1);
    if (view.numExPlanes() != expectedExPlanes) {
      throw new IllegalArgumentException(
          "Posting block bit-plane mismatch: expected=" + expectedExPlanes
              + ", actual=" + view.numExPlanes());
    }
    MetricQueryState.ClusterQuery clusterQuery =
        queryState.forRotatedCentroid(queryState.rotateCentroid(centroid));
    RaBitQByteLutScorer scorer = RaBitQByteLutScorer.forQuery(
        clusterQuery.rotatedQuery, clusterQuery.querySum, dimension, view.codeRowBytes());
    PriorityQueue<ScoredOrdinal> heap = new PriorityQueue<>(candidateLimit, WORST_FIRST);
    ByteBuffer signPlane = view.signPlaneBuffer();
    ByteBuffer exPlanes = view.exPlanesBuffer();

    for (int ordinal = 0; ordinal < view.numVectors(); ordinal++) {
      double signDot = scorer.planeDot(signPlane, view.signPlaneOffset(ordinal));
      float passOneDot = scorer.pass1FromDot(signDot);
      float centerRip = view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_1, ordinal);
      float residualNorm = view.scalarFactor(PostingBlockView.ScalarFactor.RESIDUAL_NORM, ordinal);
      float vectorNorm = view.vectorNormOrNaN(ordinal);
      double passOneRip = view.scalarFactor(
          PostingBlockView.ScalarFactor.F_RESCALE_1, ordinal) * (double) passOneDot;
      double optimisticDistance = queryState.optimisticRankingDistance(
          passOneRip,
          view.scalarFactor(PostingBlockView.ScalarFactor.ERR_1, ordinal),
          centerRip, residualNorm, vectorNorm, clusterQuery);
      if (heap.size() >= candidateLimit && optimisticDistance > heap.peek().distance) {
        continue;
      }

      float dotTerm = scorer.pass2(
          signDot, view, exPlanes, ordinal, expectedExPlanes, rabitqBits);
      double rip = view.scalarFactor(
          PostingBlockView.ScalarFactor.F_RESCALE_EX, ordinal) * (double) dotTerm;
      double distance = queryState.rankingDistance(
          rip,
          view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_EX, ordinal),
          residualNorm, vectorNorm, clusterQuery);
      offer(heap, new ScoredOrdinal(ordinal, distance), candidateLimit);
    }

    List<ScoredOrdinal> retained = new ArrayList<>(heap);
    retained.sort(Comparator.comparingDouble(ScoredOrdinal::getDistance)
        .thenComparingInt(ScoredOrdinal::getOrdinal));
    List<VectorCandidate> candidates = new ArrayList<>(retained.size());
    for (ScoredOrdinal scored : retained) {
      int ordinal = scored.ordinal;
      PostingBlockView.RowLocator row = view.rowLocator(ordinal);
      candidates.add(new VectorCandidate(
          view.recordKey(ordinal), clusterId, shardId, scored.distance,
          new VectorPostingLocator(
              generationId, clusterId, shardId, blockId, ordinal,
              row.getPartitionPath(), row.getFileGroupId(), row.getInstantTime(),
              row.getRowPosition())));
    }
    return candidates;
  }

  private static void offer(PriorityQueue<ScoredOrdinal> heap,
                            ScoredOrdinal candidate,
                            int candidateLimit) {
    if (heap.size() < candidateLimit) {
      heap.offer(candidate);
    } else if (WORST_FIRST.compare(candidate, heap.peek()) > 0) {
      heap.poll();
      heap.offer(candidate);
    }
  }

  private static final class ScoredOrdinal {
    private final int ordinal;
    private final double distance;

    private ScoredOrdinal(int ordinal, double distance) {
      this.ordinal = ordinal;
      this.distance = distance;
    }

    private int getOrdinal() {
      return ordinal;
    }

    private double getDistance() {
      return distance;
    }
  }
}
