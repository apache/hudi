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

import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexClusterStats;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexBootstrapUtils {

  @Test
  void testBootstrapProducesCentroidAndPostingRecords() {
    Map<String, String> options = new HashMap<>();
    options.put(VectorIndexOptions.DIMENSION, "2");
    options.put(VectorIndexOptions.NUM_CLUSTERS, "2");
    options.put(VectorIndexOptions.MAX_ITER, "10");
    options.put(VectorIndexOptions.METRIC, "l2");
    options.put(VectorIndexOptions.RABITQ_BITS, "4");
    options.put(VectorIndexOptions.RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER, "1");

    List<VectorIndexBootstrapUtils.VectorDocument> documents = Arrays.asList(
        document("rk-1", new float[] {0.0f, 0.0f}, "fg-1", 0L),
        document("rk-2", new float[] {0.1f, 0.2f}, "fg-1", 1L),
        document("rk-3", new float[] {10.0f, 10.0f}, "fg-2", 0L),
        document("rk-4", new float[] {9.8f, 10.2f}, "fg-2", 1L)
    );

    VectorIndexBootstrapUtils.BootstrapResult result =
        VectorIndexBootstrapUtils.buildBootstrapResult(
            documents, "vector_index_demo", options, HoodieSchema.Vector.VectorElementType.FLOAT);

    assertEquals(2, result.getCentroids().length);
    assertEquals(4, result.getAssignments().size());
    assertEquals(7, result.getRecords().size());

    assertEquals(result.getAssignments().get("rk-1"), result.getAssignments().get("rk-2"));
    assertEquals(result.getAssignments().get("rk-3"), result.getAssignments().get("rk-4"));
    assertTrue(!result.getAssignments().get("rk-1").equals(result.getAssignments().get("rk-3")));

    HoodieMetadataPayload centroidPayload = result.getRecords().stream()
        .map(record -> (HoodieMetadataPayload) record.getData())
        .filter(payload -> payload.getVectorIndexMetadata().isPresent()
            && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexCentroids)
        .findFirst()
        .orElse(null);
    assertNotNull(centroidPayload);
    assertTrue(centroidPayload.getVectorIndexMetadata().isPresent());
    HoodieVectorIndexCentroids centroids =
        (HoodieVectorIndexCentroids) centroidPayload.getVectorIndexMetadata().get();
    assertNotNull(centroids.getCentroidBytes());
    assertEquals(2 * 2 * Float.BYTES, centroids.getCentroidBytes().remaining());

    HoodieVectorIndexManifest manifest = result.getRecords().stream()
        .map(record -> (HoodieMetadataPayload) record.getData())
        .filter(payload -> payload.getVectorIndexMetadata().isPresent()
            && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexManifest)
        .map(payload -> (HoodieVectorIndexManifest) payload.getVectorIndexMetadata().get())
        .findFirst()
        .orElseThrow(AssertionError::new);
    assertEquals(2, manifest.getDim());
    assertEquals(4, manifest.getBitsTotal());
    assertEquals(3, manifest.getNumExPlanes());
    assertTrue(!manifest.getResidualEncoding());
    assertEquals(4096, manifest.getVectorsPerBlock());

    List<HoodieVectorIndexPostingBlock> postingBlocks = result.getRecords().stream()
        .map(record -> (HoodieMetadataPayload) record.getData())
        .filter(payload -> payload.getVectorIndexMetadata().isPresent()
            && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexPostingBlock)
        .map(payload -> (HoodieVectorIndexPostingBlock) payload.getVectorIndexMetadata().get())
        .collect(Collectors.toList());
    assertEquals(2, postingBlocks.size());
    int packedRows = postingBlocks.stream().mapToInt(HoodieVectorIndexPostingBlock::getNumVectors).sum();
    assertEquals(4, packedRows);

    PostingBlockView firstBlock = new PostingBlockView(postingBlocks.get(0));
    assertEquals(3, firstBlock.numExPlanes());
    assertNotNull(firstBlock.recordKey(0));
    assertNotNull(firstBlock.rowLocator(0).getFileGroupId());

    long liveCount = result.getRecords().stream()
        .map(record -> (HoodieMetadataPayload) record.getData())
        .filter(payload -> payload.getVectorIndexMetadata().isPresent()
            && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexClusterStats)
        .map(payload -> (HoodieVectorIndexClusterStats) payload.getVectorIndexMetadata().get())
        .mapToLong(HoodieVectorIndexClusterStats::getLiveCount)
        .sum();
    assertEquals(4L, liveCount);

    VectorIndexPruner pruner = new VectorIndexPruner(result.getCentroids(), clusterToFileGroups(result.getAssignments()), VectorDistanceMetric.L2);
    assertArrayEquals(new int[] {result.getAssignments().get("rk-1")}, pruner.findTopClusters(new float[] {0.05f, 0.05f}, 1));
  }

  @Test
  void testBootstrapSerializesDoubleCentroidsUsingDoubleWidth() {
    Map<String, String> options = new HashMap<>();
    options.put(VectorIndexOptions.DIMENSION, "2");
    options.put(VectorIndexOptions.NUM_CLUSTERS, "1");
    options.put(VectorIndexOptions.MAX_ITER, "2");
    options.put(VectorIndexOptions.METRIC, "l2");
    options.put(VectorIndexOptions.RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER, "1");

    List<VectorIndexBootstrapUtils.VectorDocument> documents = Arrays.asList(
        document("rk-1", new float[] {1.0f, 2.0f}, "fg-1", 0L),
        document("rk-2", new float[] {3.0f, 4.0f}, "fg-1", 1L)
    );

    VectorIndexBootstrapUtils.BootstrapResult result =
        VectorIndexBootstrapUtils.buildBootstrapResult(
            documents, "vector_index_demo", options, HoodieSchema.Vector.VectorElementType.DOUBLE);
    HoodieMetadataPayload centroidPayload = result.getRecords().stream()
        .map(record -> (HoodieMetadataPayload) record.getData())
        .filter(payload -> payload.getVectorIndexMetadata().isPresent()
            && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexCentroids)
        .findFirst()
        .orElseThrow(AssertionError::new);
    ByteBuffer centroidBytes = ((HoodieVectorIndexCentroids) centroidPayload.getVectorIndexMetadata().get()).getCentroidBytes();

    assertEquals(2 * Double.BYTES, centroidBytes.remaining());
  }

  @Test
  void testBootstrapBlockIdsAreDenseAcrossGroupBoundaries() {
    Map<String, String> options = new HashMap<>();
    options.put(VectorIndexOptions.DIMENSION, "2");
    options.put(VectorIndexOptions.NUM_CLUSTERS, "3");
    options.put(VectorIndexOptions.MAX_ITER, "1");
    options.put(VectorIndexOptions.METRIC, "l2");
    options.put(VectorIndexOptions.RABITQ_BITS, "4");
    options.put(VectorIndexOptions.RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER, "1");
    options.put(VectorIndexOptions.RABITQ_POSTING_TARGET_BLOCK_BYTES, "1");

    List<VectorIndexBootstrapUtils.VectorDocument> documents = new ArrayList<>();
    addClusterDocs(documents, "a", new float[] {0.0f, 0.0f}, 171);
    addClusterDocs(documents, "b", new float[] {1000.0f, 0.0f}, 171);
    addClusterDocs(documents, "c", new float[] {0.0f, 1000.0f}, 1);
    addClusterDocs(documents, "a-tail", new float[] {0.25f, 0.0f}, 85);
    addClusterDocs(documents, "b-tail", new float[] {1000.25f, 0.0f}, 86);

    VectorIndexBootstrapUtils.BootstrapResult result =
        VectorIndexBootstrapUtils.buildBootstrapResult(
            documents, "vector_index_demo", 17, options, HoodieSchema.Vector.VectorElementType.FLOAT);

    Map<Integer, Set<Long>> blockIdsByCluster = new HashMap<>();
    List<Integer> blockSizes = new ArrayList<>();
    result.getRecords().forEach(record -> {
      HoodieMetadataPayload payload = (HoodieMetadataPayload) record.getData();
      if (payload.getVectorIndexMetadata().isPresent()
          && payload.getVectorIndexMetadata().get() instanceof HoodieVectorIndexPostingBlock) {
        byte[] key = VectorIndexMetadataKey.decode(record.getRecordKey());
        int clusterId = readInt(key, 5);
        long blockId = Integer.toUnsignedLong(readInt(key, 11));
        blockIdsByCluster.computeIfAbsent(clusterId, ignored -> new HashSet<>()).add(blockId);
        blockSizes.add(((HoodieVectorIndexPostingBlock) payload.getVectorIndexMetadata().get()).getNumVectors());
      }
    });

    assertEquals(3, blockIdsByCluster.size());
    List<Integer> blocksPerCluster = blockIdsByCluster.values().stream()
        .map(Set::size)
        .sorted()
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(1, 1, 2), blocksPerCluster);
    for (Set<Long> blockIds : blockIdsByCluster.values()) {
      for (long expected = 0; expected < blockIds.size(); expected++) {
        assertTrue(blockIds.contains(expected));
      }
    }
    Collections.sort(blockSizes);
    assertEquals(Arrays.asList(1, 1, 256, 256), blockSizes);
  }

  @Test
  void testMaterializeRaBitQColumnsOnCreateOptionDefaultsToFalse() {
    Map<String, String> options = new HashMap<>();
    assertTrue(!VectorIndexOptions.shouldMaterializeRaBitQColumnsOnCreate(options));

    options.put(VectorIndexOptions.RABITQ_MATERIALIZE_ON_CREATE, "true");
    assertTrue(VectorIndexOptions.shouldMaterializeRaBitQColumnsOnCreate(options));
  }

  @Test
  void testRaBitQStorageDefaultsToMdtLookupAndEnablesMdtPostings() {
    Map<String, String> options = new HashMap<>();
    assertEquals("mdt_lookup", VectorIndexOptions.getRaBitQStorage(options));
    assertTrue(VectorIndexOptions.shouldStoreRaBitQCodesInMdt(options));
    assertEquals(2, VectorIndexOptions.getRaBitQBits(options));

    options.put(VectorIndexOptions.RABITQ_STORAGE, "hidden_columns");
    assertTrue(!VectorIndexOptions.shouldStoreRaBitQCodesInMdt(options));
  }

  private static Map<Integer, java.util.Set<String>> clusterToFileGroups(Map<String, Integer> assignments) {
    Map<Integer, java.util.Set<String>> clusterMap = new HashMap<>();
    assignments.forEach((recordKey, clusterId) ->
        clusterMap.computeIfAbsent(clusterId, ignored -> new java.util.HashSet<>()).add("fg-" + recordKey));
    return clusterMap;
  }

  private static VectorIndexBootstrapUtils.VectorDocument document(String recordKey, float[] vector, String fileGroupId, long rowPosition) {
    return new VectorIndexBootstrapUtils.VectorDocument(recordKey, vector, fileGroupId, "p=1", "001", rowPosition);
  }

  private static void addClusterDocs(List<VectorIndexBootstrapUtils.VectorDocument> documents,
                                     String prefix,
                                     float[] center,
                                     int count) {
    for (int i = 0; i < count; i++) {
      documents.add(document(prefix + "-" + i, new float[] {center[0] + (i * 0.001f), center[1]}, "fg-" + prefix, i));
    }
  }

  private static int readInt(byte[] bytes, int offset) {
    return (Byte.toUnsignedInt(bytes[offset]) << 24)
        | (Byte.toUnsignedInt(bytes[offset + 1]) << 16)
        | (Byte.toUnsignedInt(bytes[offset + 2]) << 8)
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }
}
