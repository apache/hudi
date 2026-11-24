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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises posting reads, scoring, overlay and candidate reduction with the same encoded block. */
class TestVectorPostingPipelineParity {

  private static final String PARTITION = "vector_index_embedding_idx";
  private static final int DIMENSION = 16;
  private static final int BITS = 4;
  private static final long SEED = 31L;

  @Test
  void packedScannerAgreesWithMaterializedPostingPipeline() {
    float[][] vectors = {
        {1f, 2f, 3f, 4f, 2f, -1f, 0f, 3f, -2f, 1f, 0f, 1f, 2f, 0f, -1f, 4f},
        {-2f, 1f, 0f, 1f, 3f, 2f, -1f, 0f, 1f, -3f, 4f, 0f, -1f, 2f, 0f, 1f},
        {2f, -1f, 1f, 0f, 0f, 4f, 2f, 1f, -3f, 0f, 1f, -1f, 3f, 2f, 1f, 0f}
    };
    float[][] centroids = {new float[DIMENSION]};
    float[] query = {1f, 2f, 2f, 3f, 1f, 0f, 0f, 3f, -1f, 1f, 0f, 2f, 2f, 0f, 0f, 3f};
    RaBitQEncoder encoder = new RaBitQEncoder(DIMENSION, BITS, SEED, false);
    int rowBytes = ((DIMENSION + 63) / 64) * Long.BYTES;
    PostingBlockBuilder builder = new PostingBlockBuilder(rowBytes, BITS - 1);
    for (int i = 0; i < vectors.length; i++) {
      QuantizedVector encoded = encoder.encodeResidual(vectors[i], centroids[0]);
      builder.addRow("rk-" + i,
          VectorIndexBootstrapUtils.padToRow(encoded.code, rowBytes),
          VectorIndexBootstrapUtils.splitExPlanes(encoded.extendedCode, BITS - 1, DIMENSION, rowBytes),
          encoded.additiveFactor1, encoded.rescaleFactor1, encoded.error1,
          encoded.additiveFactor, encoded.rescaleFactor, encoded.scalar,
          "fg-1", "001", "p=1", i);
    }
    HoodieRecord<HoodieMetadataPayload> block = HoodieMetadataPayload.createVectorIndexPostingBlockRecord(
        1, 0, 0, 0, builder.build(), PARTITION);
    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getRecordsByKeyPrefixes(any(), eq(PARTITION), eq(false)))
        .thenAnswer(invocation -> {
          List<? extends RawKey> prefixes = ((HoodieListData<? extends RawKey>) invocation.getArgument(0)).collectAsList();
          assertEquals(1, prefixes.size());
          return HoodieListData.eager(Collections.singletonList(block));
        });
    Map<Integer, Integer> shards = Collections.singletonMap(0, 1);
    for (int heapSize : new int[] {1, 2, vectors.length}) {
      List<ScoredVectorPostingMatch> packed = VectorIndexMdtSearchUtils.scanPostingCandidates(
          metadata, PARTITION, 1, shards, query, DIMENSION, SEED, BITS, false,
          VectorDistanceMetric.L2, false, true, centroids, heapSize).collectAsList();
      List<ScoredVectorPostingMatch> materialized = VectorIndexMdtSearchUtils.selectTopK(
          VectorIndexMdtSearchUtils.overlayByRecordKey(VectorIndexMdtSearchUtils.scoreConfiguredPostings(
              VectorIndexMdtSearchUtils.readPostingMatches(metadata, PARTITION, 1, shards, false),
              query, DIMENSION, SEED, BITS, false, VectorDistanceMetric.L2, false, true, centroids)),
          heapSize).collectAsList();

      assertEquals(heapSize, packed.size());
      assertEquals(heapSize, materialized.size());
      Map<String, ScoredVectorPostingMatch> byKey = materialized.stream()
          .collect(Collectors.toMap(ScoredVectorPostingMatch::getRecordKey, match -> match));
      for (ScoredVectorPostingMatch match : packed) {
        assertTrue(byKey.containsKey(match.getRecordKey()));
        assertEquals(byKey.get(match.getRecordKey()).getApproxDistance(), match.getApproxDistance(), 1e-3f);
        assertEquals(byKey.get(match.getRecordKey()).getPartitionPath(), match.getPartitionPath());
        assertEquals(byKey.get(match.getRecordKey()).getRowPosition(), match.getRowPosition());
      }
    }
  }
}
