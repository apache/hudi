/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Scans packed vector posting blocks and reduces them to approximate candidates. */
final class VectorPostingBlockScanner {

  private static final Logger LOG = LoggerFactory.getLogger(VectorPostingBlockScanner.class);

  private VectorPostingBlockScanner() {
  }

  static HoodieData<ScoredVectorPostingMatch> scan(HoodieTableMetadata metadataTable,
                                                    String indexPartition,
                                                    int generationId,
                                                    Map<Integer, Integer> clusterShardCounts,
                                                    float[] queryVector,
                                                    int dimension,
                                                    long randomSeed,
                                                    int rabitqBits,
                                                    boolean assumeNormalized,
                                                    VectorDistanceMetric metric,
                                                    float[][] centroids,
                                                    int candidateHeapSize) {
    List<VectorPostingPrefixRawKey> postingPrefixes = VectorIndexMdtSearchUtils.buildPostingPrefixes(generationId, clusterShardCounts);
    if (candidateHeapSize <= 0 || postingPrefixes.isEmpty()) {
      return HoodieListData.eager(Collections.emptyList());
    }
    if (rabitqBits <= 1) {
      throw new IllegalArgumentException("Packed block scanner requires multibit RaBitQ postings.");
    }
    if (centroids == null || centroids.length == 0) {
      throw new IllegalArgumentException("Packed block scanner requires IVF centroids for residual scoring.");
    }

    long setupStartMs = System.currentTimeMillis();
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, rabitqBits, randomSeed, assumeNormalized);
    MetricQueryState metricQueryState = MetricQueryState.create(metric, encoder::rotateVector, queryVector, assumeNormalized);
    int effectiveBits = Math.max(1, rabitqBits);
    LOG.info("[vector_search][stage][scan_posting_blocks_setup] prefixes={} queryDim={} metric={} bits={} assumeNormalized={} candidateHeapSize={} elapsedMs={}",
        postingPrefixes.size(),
        dimension,
        metric,
        effectiveBits,
        assumeNormalized,
        candidateHeapSize,
        System.currentTimeMillis() - setupStartMs);

    List<RawKey> rawKeys = new ArrayList<>(postingPrefixes);
    HoodieData<ScoredVectorPostingMatch> localTopK = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(rawKeys), indexPartition, false)
        .mapPartitions(iterator -> scanBlockPartition(iterator, metricQueryState, centroids, dimension, effectiveBits, candidateHeapSize), true);
    return VectorIndexMdtSearchUtils.selectTopK(
        VectorIndexMdtSearchUtils.overlayByRecordKey(localTopK), candidateHeapSize);
  }

  static Iterator<ScoredVectorPostingMatch> scanBlockPartition(
      Iterator<HoodieRecord<HoodieMetadataPayload>> records,
      MetricQueryState metricQueryState,
      float[][] centroids,
      int dimension,
      int rabitqBits,
      int heapSize) {
    long partitionStartMs = System.currentTimeMillis();
    if (heapSize <= 0) {
      return Collections.emptyIterator();
    }

    VectorCandidateHeap heap = new VectorCandidateHeap(heapSize);
    List<PostingBlockView> blocks = new ArrayList<>();
    List<int[]> blockKeyComponents = new ArrayList<>();
    List<ScoredVectorPostingMatch> deltaMatches = new ArrayList<>();
    Map<String, ScoredVectorPostingMatch> deltaByRecordKey = new HashMap<>();
    Set<String> suppressedPostingKeys = new HashSet<>();
    Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache = new HashMap<>();
    Map<Integer, RaBitQByteLutScorer> lutScorerCache = new HashMap<>();
    int blocksRead = 0;
    int deltasRead = 0;
    long vectorsSeen = 0L;
    long boundSurvivors = 0L;
    // §5A fix 5 (deepened): split the conflated scan wall time. iterNs = time pulling records
    // (HFile block read + Avro decode, lazy in next()); kernelNs = scoring loop (what the LUT
    // touches); gcMs delta = allocation pressure. This isolates whether the straggler is I/O or CPU.
    long iterNs = 0L;
    long kernelNs = 0L;
    long gcStartMs = totalGcMillis();

    while (true) {
      long iterStart = System.nanoTime();
      boolean hasNext = records.hasNext();
      HoodieRecord<HoodieMetadataPayload> record = hasNext ? records.next() : null;
      iterNs += System.nanoTime() - iterStart;
      if (!hasNext) {
        break;
      }
      if (record.getData().isDeleted()) {
        String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
        int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
        String postingKey = postingIdentity(keyComponents, deletedRecordKey);
        if (deletedRecordKey != null && suppressedPostingKeys.add(postingKey)) {
          removeSuppressedDelta(deltaByRecordKey, deletedRecordKey, keyComponents);
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        continue;
      }
      Option<Object> infoOpt = record.getData().getVectorIndexMetadata();
      if (!infoOpt.isPresent()) {
        continue;
      }

      Object info = infoOpt.get();
      if (VectorPostingSerde.isPostingTombstoneInfo(info)) {
        String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
        int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
        String postingKey = postingIdentity(keyComponents, deletedRecordKey);
        if (deletedRecordKey != null && suppressedPostingKeys.add(postingKey)) {
          removeSuppressedDelta(deltaByRecordKey, deletedRecordKey, keyComponents);
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        continue;
      }
      int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
      if (VectorPostingSerde.isPostingBlockInfo(info)) {
        PostingBlockView view = new PostingBlockView(VectorIndexMdtSearchUtils.asPostingBlock(info));
        validatePackedBlock(view, rabitqBits);
        int blockOrdinal = blocks.size();
        blocks.add(view);
        blockKeyComponents.add(keyComponents);
        blocksRead++;
        ByteBuffer signPlane = view.signPlaneBuffer();
        ByteBuffer exPlanes = view.exPlanesBuffer();
        MetricQueryState.ClusterQuery clusterQuery = VectorIndexMdtSearchUtils.clusterQuery(
            metricQueryState,
            centroids,
            clusterQueryCache,
            keyComponents[0]);
        // Byte-LUT scorer built once per probed cluster (RFC-109 v3 §5A fixes 1-2): preserves
        // exact float-query semantics, replaces the per-dimension sign loop and the per-survivor
        // repack/allocate pass-2 path with table lookups and zero scoring-loop allocation.
        RaBitQByteLutScorer scorer = lutScorer(
            lutScorerCache, clusterQuery, keyComponents[0], dimension, view.codeRowBytes());
        int exBits = rabitqBits - 1;
        long kernelStart = System.nanoTime();
        for (int vectorIndex = 0; vectorIndex < view.numVectors(); vectorIndex++) {
          vectorsSeen++;
          int signOffset = view.signPlaneOffset(vectorIndex);
          double signDot = scorer.planeDot(signPlane, signOffset);
          float dotTerm1 = scorer.pass1FromDot(signDot);
          float centerRip = view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_1, vectorIndex);
          float residualNorm = view.scalarFactor(PostingBlockView.ScalarFactor.RESIDUAL_NORM, vectorIndex);
          float vectorNorm = view.vectorNormOrNaN(vectorIndex);
          double rip1 = view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_1, vectorIndex) * (double) dotTerm1;
          float bound = (float) metricQueryState.optimisticRankingDistance(
              rip1,
              view.scalarFactor(PostingBlockView.ScalarFactor.ERR_1, vectorIndex),
              centerRip,
              residualNorm,
              vectorNorm,
              clusterQuery);
          if (!heap.wouldAdmit(bound)) {
            continue;
          }

          boundSurvivors++;
          // Pass 2 scores the extended planes in place from the same LUT; no signRow copy,
          // no repackExtendedLevels, no per-survivor byte[] allocation.
          float dotTerm = scorer.pass2(signDot, view, exPlanes, vectorIndex, exBits, rabitqBits);
          double rip = view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_EX, vectorIndex) * (double) dotTerm;
          float approxDistance = (float) metricQueryState.rankingDistance(
              rip,
              view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_EX, vectorIndex),
              residualNorm,
              vectorNorm,
              clusterQuery);
          heap.offer(packBlockKey(blockOrdinal, vectorIndex), approxDistance);
        }
        kernelNs += System.nanoTime() - kernelStart;
      } else if (VectorPostingSerde.isPostingDeltaInfo(info) && VectorPostingSerde.getDeltaBinaryCode(info) != null) {
        deltasRead++;
        ScoredVectorPostingMatch scoredDelta = scoreDeltaMatch(
            info,
            keyComponents,
            metricQueryState,
            centroids,
            clusterQueryCache,
            dimension,
            rabitqBits);
        if (suppressedPostingKeys.contains(postingIdentity(keyComponents, scoredDelta.getRecordKey()))) {
          continue;
        }
        boolean newOverlayKey = !deltaByRecordKey.containsKey(scoredDelta.getRecordKey());
        deltaByRecordKey.merge(scoredDelta.getRecordKey(), scoredDelta, VectorIndexMdtSearchUtils::chooseOverlayCandidate);
        if (newOverlayKey) {
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        if (heap.wouldAdmit(scoredDelta.getApproxDistance())) {
          int deltaOrdinal = deltaMatches.size();
          deltaMatches.add(scoredDelta);
          heap.offer(packDeltaKey(deltaOrdinal), scoredDelta.getApproxDistance());
        }
      } else {
        throw new IllegalArgumentException("Unexpected vector posting metadata record: " + info.getClass().getName());
      }
    }

    List<ScoredVectorPostingMatch> scored = new ArrayList<>(heap.size());
    Set<String> emittedDeltaRecordKeys = new HashSet<>();
    for (VectorCandidateHeap.Entry entry : heap.entriesBestFirst()) {
      long key = entry.key;
      if (isDeltaKey(key)) {
        ScoredVectorPostingMatch delta = deltaMatches.get(unpackDeltaOrdinal(key));
        int[] keyComponents = {delta.getClusterId(), delta.getShardId()};
        if (!suppressedPostingKeys.contains(postingIdentity(keyComponents, delta.getRecordKey()))
            && delta == deltaByRecordKey.get(delta.getRecordKey())) {
          scored.add(delta);
          emittedDeltaRecordKeys.add(delta.getRecordKey());
        }
        continue;
      }
      int blockOrdinal = unpackBlockOrdinal(key);
      int vectorIndex = unpackVectorIndex(key);
      PostingBlockView view = blocks.get(blockOrdinal);
      int[] keyComponents = blockKeyComponents.get(blockOrdinal);
      String recordKey = view.recordKey(vectorIndex);
      if (suppressedPostingKeys.contains(postingIdentity(keyComponents, recordKey))) {
        continue;
      }
      ScoredVectorPostingMatch overlayDelta = deltaByRecordKey.get(recordKey);
      if (overlayDelta != null) {
        if (emittedDeltaRecordKeys.add(recordKey)) {
          scored.add(overlayDelta);
        }
        continue;
      }
      PostingBlockView.RowLocator rowLocator = view.rowLocator(vectorIndex);
      VectorPostingMatch match = new VectorPostingMatch(
          recordKey,
          keyComponents[0],
          keyComponents[1],
          rowLocator.getFileGroupId(),
          rowLocator.getPartitionPath(),
          rowLocator.getInstantTime(),
          rowLocator.getRowPosition(),
          VectorPostingSerde.copyBuffer(view.signPlaneRow(vectorIndex)),
          VectorPostingSerde.packExtendedLevels(view, vectorIndex),
          view.scalarFactor(PostingBlockView.ScalarFactor.RESIDUAL_NORM, vectorIndex),
          view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_EX, vectorIndex),
          view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_EX, vectorIndex),
          view.hasVectorNorm() ? view.scalarFactor(PostingBlockView.ScalarFactor.VECTOR_NORM, vectorIndex) : null);
      scored.add(new ScoredVectorPostingMatch(match, entry.score, match.toLocation().orElse(null)));
    }
    long partitionElapsedMs = System.currentTimeMillis() - partitionStartMs;
    // RFC-109 v3 §5A fix 5: task-normalized throughput so the next investigation needs no log
    // archaeology. vectorsPerMs exposes per-partition scan speed; survivorsPerVector exposes
    // pass-1 selectivity (skew shows up as low vectorsPerMs on a high-vectors partition).
    double vectorsPerMs = partitionElapsedMs > 0 ? (double) vectorsSeen / partitionElapsedMs : (double) vectorsSeen;
    double survivorsPerVector = vectorsSeen > 0 ? (double) boundSurvivors / vectorsSeen : 0.0;
    long iterMs = iterNs / 1_000_000L;
    long kernelMs = kernelNs / 1_000_000L;
    long gcMs = totalGcMillis() - gcStartMs;
    LOG.info("[vector_search][stage][scan_posting_blocks] blocks={} deltas={} vectors={} boundSurvivors={} kept={} clusterQueries={} "
            + "vectorsPerMs={} survivorsPerVector={} iterMs={} kernelMs={} gcMs={} elapsedMs={}",
        blocksRead,
        deltasRead,
        vectorsSeen,
        boundSurvivors,
        scored.size(),
        clusterQueryCache.size(),
        String.format("%.1f", vectorsPerMs),
        String.format("%.4f", survivorsPerVector),
        iterMs,
        kernelMs,
        gcMs,
        partitionElapsedMs);
    return scored.iterator();
  }

  private static void validatePackedBlock(PostingBlockView view, int rabitqBits) {
    int expectedExPlanes = Math.max(0, rabitqBits - 1);
    if (view.numExPlanes() != expectedExPlanes) {
      throw new IllegalArgumentException(
          "Posting block bit-plane mismatch: expected exPlanes=" + expectedExPlanes
              + ", actual=" + view.numExPlanes());
    }
  }

  private static RaBitQByteLutScorer lutScorer(
      Map<Integer, RaBitQByteLutScorer> lutScorerCache,
      MetricQueryState.ClusterQuery clusterQuery,
      int clusterId,
      int dimension,
      int codeRowBytes) {
    return lutScorerCache.computeIfAbsent(clusterId, id ->
        RaBitQByteLutScorer.forQuery(clusterQuery.rotatedQuery, clusterQuery.querySum, dimension, codeRowBytes));
  }

  /** Cumulative JVM GC time (ms) across all collectors; delta over a scan isolates allocation stalls. */
  private static long totalGcMillis() {
    long total = 0L;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long t = bean.getCollectionTime();
      if (t > 0) {
        total += t;
      }
    }
    return total;
  }

  private static ScoredVectorPostingMatch scoreDeltaMatch(Object info,
                                                            int[] keyComponents,
                                                            MetricQueryState metricQueryState,
                                                            float[][] centroids,
                                                            Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache,
                                                            int dimension,
                                                            int rabitqBits) {
    CodeRows codeRows = splitDeltaCode(VectorPostingSerde.getDeltaBinaryCode(info), dimension, rabitqBits);
    MetricQueryState.ClusterQuery clusterQuery = VectorIndexMdtSearchUtils.clusterQuery(
        metricQueryState,
        centroids,
        clusterQueryCache,
        keyComponents[0]);
    float dotTerm = RaBitQEncoder.multibitDotTerm(
        clusterQuery.rotatedQuery,
        clusterQuery.querySum,
        codeRows.sign,
        codeRows.extended,
        dimension,
        rabitqBits);
    VectorPostingMatch match = VectorPostingMatch.delta(
        VectorPostingSerde.getDeltaRecordKey(info),
        keyComponents[0],
        keyComponents[1],
        VectorPostingSerde.getDeltaFileGroupId(info),
        VectorPostingSerde.getDeltaPartitionPath(info),
        VectorPostingSerde.getDeltaBaseInstantTime(info),
        VectorPostingSerde.getDeltaRowPosition(info),
        codeRows.sign,
        codeRows.extended,
        VectorPostingSerde.getDeltaResidualNorm(info),
        VectorPostingSerde.getDeltaFAddEx(info),
        VectorPostingSerde.getDeltaFRescaleEx(info),
        VectorPostingSerde.getDeltaVectorNormOrNull(info));
    double rip = VectorPostingSerde.getDeltaFRescaleEx(info) * (double) dotTerm;
    float approxDistance = (float) metricQueryState.rankingDistance(
        rip,
        VectorPostingSerde.getDeltaFAddEx(info),
        VectorPostingSerde.getDeltaResidualNorm(info),
        VectorPostingSerde.getDeltaVectorNormOrNaN(info),
        clusterQuery);
    return new ScoredVectorPostingMatch(match, approxDistance, match.toLocation().orElse(null));
  }

  private static CodeRows splitDeltaCode(ByteBuffer codeBuffer, int dimension, int rabitqBits) {
    int signBytes = (dimension + 7) / 8;
    int extendedBytes = (dimension * Math.max(0, rabitqBits - 1) + 7) / 8;
    int expectedBytes = signBytes + extendedBytes;
    ByteBuffer duplicate = codeBuffer.duplicate();
    if (duplicate.remaining() != expectedBytes) {
      throw new IllegalArgumentException(
          "Posting delta code length mismatch: expected=" + expectedBytes
              + ", actual=" + duplicate.remaining());
    }
    byte[] sign = new byte[signBytes];
    byte[] extended = new byte[extendedBytes];
    duplicate.get(sign);
    duplicate.get(extended);
    return new CodeRows(sign, extended);
  }

  static byte[] repackExtendedLevels(PostingBlockView view,
                                     int vectorIndex,
                                     int dimension,
                                     byte[] scratch) {
    int exBits = view.numExPlanes();
    if (exBits <= 0) {
      return new byte[0];
    }
    int packedBytes = (dimension * exBits + 7) / 8;
    Arrays.fill(scratch, 0, packedBytes, (byte) 0);
    ByteBuffer exPlanes = view.exPlanesBuffer();
    int bitOffset = 0;
    for (int dim = 0; dim < dimension; dim++) {
      for (int bit = 0; bit < exBits; bit++) {
        int plane = exBits - 1 - bit;
        int planeOffset = view.exPlaneOffset(vectorIndex, plane) + (dim >> 3);
        if ((exPlanes.get(planeOffset) & (1 << (dim & 7))) != 0) {
          int absoluteBit = bitOffset + bit;
          scratch[absoluteBit >> 3] |= (byte) (1 << (absoluteBit & 7));
        }
      }
      bitOffset += exBits;
    }
    return Arrays.copyOf(scratch, packedBytes);
  }

  private static long packBlockKey(int blockOrdinal, int vectorIndex) {
    return ((long) blockOrdinal << Integer.SIZE) | (vectorIndex & 0xFFFFFFFFL);
  }

  private static long packDeltaKey(int deltaOrdinal) {
    return Long.MIN_VALUE | (deltaOrdinal & 0x7FFFFFFFL);
  }

  private static boolean isDeltaKey(long key) {
    return key < 0;
  }

  private static int unpackDeltaOrdinal(long key) {
    return (int) (key & 0x7FFFFFFFL);
  }

  private static int unpackBlockOrdinal(long key) {
    return (int) (key >>> Integer.SIZE);
  }

  private static int unpackVectorIndex(long key) {
    return (int) key;
  }

  private static String postingIdentity(int[] keyComponents, String recordKey) {
    return keyComponents[0] + "\u0000" + keyComponents[1] + "\u0000" + recordKey;
  }

  private static void removeSuppressedDelta(
      Map<String, ScoredVectorPostingMatch> deltaByRecordKey,
      String recordKey,
      int[] keyComponents) {
    ScoredVectorPostingMatch delta = deltaByRecordKey.get(recordKey);
    if (delta != null
        && delta.getClusterId() == keyComponents[0]
        && delta.getShardId() == keyComponents[1]) {
      deltaByRecordKey.remove(recordKey);
    }
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
