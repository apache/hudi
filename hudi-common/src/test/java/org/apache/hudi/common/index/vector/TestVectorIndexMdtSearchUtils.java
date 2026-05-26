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

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestVectorIndexMdtSearchUtils {

  @Test
  void testBuildPostingPrefixesExpandsShardRangesInClusterOrder() {
    Map<Integer, Integer> shardCounts = new HashMap<>();
    shardCounts.put(5, 2);
    shardCounts.put(1, 1);

    List<VectorPostingPrefixRawKey> prefixes = VectorIndexMdtSearchUtils.buildPostingPrefixes(7, shardCounts);

    assertEquals(3, prefixes.size());
    assertEquals("P|00000007|00000001|0000|", prefixes.get(0).encode());
    assertEquals("P|00000007|00000005|0000|", prefixes.get(1).encode());
    assertEquals("P|00000007|00000005|0001|", prefixes.get(2).encode());
  }

  @Test
  void testReadClusterShardCountsUsesClusterManifestRows() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    String indexPartition = "vector_index_embedding_idx";
    List<HoodieRecord<HoodieMetadataPayload>> clusterRecords = Arrays.asList(
        HoodieMetadataPayload.createVectorIndexClusterManifestRecord("7", 3, 4, Collections.singletonList("fg-1"), 10L, 123L, indexPartition),
        HoodieMetadataPayload.createVectorIndexClusterManifestRecord("7", 9, 2, Collections.singletonList("fg-2"), 20L, 123L, indexPartition));
    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(indexPartition), eq(true)))
        .thenReturn(HoodieListData.eager(clusterRecords));

    Map<Integer, Integer> shardCounts = VectorIndexMdtSearchUtils.readClusterShardCounts(
        metadataTable, indexPartition, 7, Arrays.asList(3, 9));

    assertEquals(4, shardCounts.get(3));
    assertEquals(2, shardCounts.get(9));
  }

  @Test
  void testReadPostingMatchesExtractsRecordKeysFromPostingRows() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    String indexPartition = "vector_index_embedding_idx";
    HoodieRecord<HoodieMetadataPayload> postingRecord = HoodieMetadataPayload.createVectorIndexPostingRecord(
        "7", "doc_42", 3, 1, "fg-9", "p=2026", new byte[] {0x0F}, 2.0f, 123L, indexPartition);
    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(indexPartition), eq(false)))
        .thenReturn(HoodieListData.eager(Collections.singletonList(postingRecord)));

    HoodieListData<VectorIndexMdtSearchUtils.PostingMatch> matches =
        (HoodieListData<VectorIndexMdtSearchUtils.PostingMatch>) VectorIndexMdtSearchUtils.readPostingMatches(
            metadataTable,
            indexPartition,
            7,
            Collections.singletonMap(3, 2),
            false);

    List<VectorIndexMdtSearchUtils.PostingMatch> results = matches.collectAsList();
    assertEquals(1, results.size());
    assertEquals("doc_42", results.get(0).getRecordKey());
    assertEquals(1, results.get(0).getShardId());
    assertEquals("fg-9", results.get(0).getFileGroupId());
    assertNotNull(results.get(0).getBinaryCode());
  }

  @Test
  void testScoreAttachLocationAndTopK() {
    RaBitQEncoder encoder = new RaBitQEncoder(8, 42L, false);
    VectorQuantizer.QuantizedVector near = encoder.encode(new float[] {1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f});
    VectorQuantizer.QuantizedVector far = encoder.encode(new float[] {-1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f});

    HoodieListData<VectorIndexMdtSearchUtils.PostingMatch> postings = HoodieListData.eager(Arrays.asList(
        new VectorIndexMdtSearchUtils.PostingMatch("rk-near", 0, 0, "fg-1", "p1", near.code, near.scalar),
        new VectorIndexMdtSearchUtils.PostingMatch("rk-far", 0, 0, "fg-2", "p1", far.code, far.scalar)));

    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    when(metadataTable.readRecordIndexLocationsWithKeys(any())).thenReturn(HoodieListPairData.eager(Arrays.asList(
        Pair.of("rk-near", new HoodieRecordGlobalLocation("p1", "001", "fg-1")),
        Pair.of("rk-far", new HoodieRecordGlobalLocation("p1", "001", "fg-2")))));

    List<VectorIndexMdtSearchUtils.ScoredPostingMatch> top = VectorIndexMdtSearchUtils.selectTopK(
            VectorIndexMdtSearchUtils.attachRecordLocations(
                metadataTable,
                VectorIndexMdtSearchUtils.scorePostingMatches(postings, new float[] {1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}, 8, 42L, false)),
            1)
        .collectAsList();

    assertEquals(1, top.size());
    assertEquals("rk-near", top.get(0).getRecordKey());
    assertTrue(top.get(0).getApproxDistance() <= 1.0e-6f);
    assertNotNull(top.get(0).getLocation());
    assertEquals("fg-1", top.get(0).getLocation().getFileId());
  }

  @Test
  void testPostingRecordKeyExtractionHelper() {
    String postingKey = HoodieTableMetadataUtil.getVectorIndexPostingKey(7, 3, 2, "doc_987654");
    assertEquals("doc_987654", HoodieTableMetadataUtil.getVectorIndexPostingRecordKey(postingKey));
    assertEquals(null, HoodieTableMetadataUtil.getVectorIndexPostingRecordKey("not-a-posting-key"));
  }
}
