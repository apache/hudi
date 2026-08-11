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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.index.vector.PostingBlockBuilder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorStalePolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestMdtVectorPostingScanner {

  @Test
  void scansBlocksDeltasAndLogicalTombstonesAcrossClusters() {
    String indexPartition = "vector-index";
    HoodieVectorIndexPostingBlock oldBlock = new PostingBlockBuilder(Long.BYTES, 0)
        .addRow("moved", new byte[Long.BYTES], new byte[0],
            0f, 1f, 0f, 0f, 1f, 1f, "old-file", "001", "p", 3)
        .build();
    HoodieRecord<HoodieMetadataPayload> block =
        HoodieMetadataPayload.createVectorIndexPostingBlockRecord(
            1, 0, 0, 7, oldBlock, indexPartition);
    HoodieRecord<HoodieMetadataPayload> tombstone =
        HoodieMetadataPayload.createVectorIndexPostingDeleteRecord(
            1, "moved", 0, 0, "002", indexPartition);
    HoodieRecord<HoodieMetadataPayload> delta =
        HoodieMetadataPayload.createVectorIndexPostingRecord(
            1, "moved", 1, 0, "new-file", "p", "002",
            new byte[] {1}, new byte[0], 1f, 0f, 1f,
            0f, 1f, 0f, null, -1L, 2L, indexPartition);

    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getRecordsByKeyPrefixes(any(), eq(indexPartition), eq(false)))
        .thenReturn(HoodieListData.eager(Arrays.asList(block, tombstone, delta)));
    MdtVectorPostingScanner scanner = new MdtVectorPostingScanner(
        metadata, indexPartition,
        new float[][] {{0f, 0f}, {1f, 1f}}, Collections.emptyMap(),
        2, 1, 17L, false);

    VectorPostingScanResult result = scanner.scan(plan(), null, 4);
    List<VectorCandidate> resolved = VectorCandidateOverlay.resolvePostingKeys(
        result.getPackedCandidates(), result.getDeltaCandidates(),
        result.getTombstonedPostingKeys(), 4, 1);

    assertEquals(1, result.getPackedCandidates().size());
    assertEquals(1, result.getDeltaCandidates().size());
    assertEquals(1, result.getTombstonedPostingKeys().size());
    assertEquals(1, resolved.size());
    assertEquals(1, resolved.get(0).getClusterId());
    assertEquals("new-file", resolved.get(0).getPostingLocator().getFileId());
    assertEquals(-1L, resolved.get(0).getPostingLocator().getRowPosition());
  }

  private static VectorSearchPlan plan() {
    VectorSearchBudget budget = new VectorSearchBudget(
        5000, 4, 1, 4, 10, 1,
        VectorExecutionMode.LOCAL, 10, DeadlinePolicy.FAIL);
    VectorSearchRequest request = new VectorSearchRequest(
        "embedding", new float[] {0f, 0f}, VectorDistanceMetric.L2, 1, 2, 1, true,
        VectorStalePolicy.FAIL, "002", budget);
    return new VectorSearchPlan(
        request,
        new VectorSearchSnapshot(
            "002", new VectorIndexSnapshot(1, 1, 1, "rot-v1", "quant-v1", "002")),
        new VectorExecutionDecision(
            VectorExecutionMode.LOCAL, VectorExecutionMode.LOCAL, 4, 10, "test"));
  }
}
