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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchema.Vector.VectorElementType;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestVectorIndexMetadataCache {

  private static final String INDEX_PARTITION = "vector_index_embedding_idx";

  @Test
  void testLoadCachesAllMetadataInOneCall() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(4, VectorElementType.FLOAT);

    // Build mock records: centroids + manifest + quantizer + cluster manifests
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();

    // Centroids: 2 centroids of dimension 4
    ByteBuffer centroidBytes = ByteBuffer.allocate(2 * 4 * 4).order(ByteOrder.LITTLE_ENDIAN);
    centroidBytes.putFloat(0.1f).putFloat(0.2f).putFloat(0.3f).putFloat(0.4f);
    centroidBytes.putFloat(0.5f).putFloat(0.6f).putFloat(0.7f).putFloat(0.8f);
    centroidBytes.flip();
    allRecords.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));

    // Manifest: generation 7
    allRecords.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "7", "IVF_RABITQ", 1, 42L, false, 123L, INDEX_PARTITION));

    // Quantizer
    allRecords.add(HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        "IVF_RABITQ", 1, 42L, false, INDEX_PARTITION));

    // Cluster manifests
    allRecords.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "7", 0, 2, Arrays.asList("fg-1", "fg-2"), 50L, 123L, INDEX_PARTITION));
    allRecords.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "7", 1, 3, Arrays.asList("fg-3", "fg-4", "fg-5"), 100L, 123L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(allRecords));

    // Act
    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    // Assert
    assertNotNull(cache);
    assertEquals(2, cache.numClusters());
    assertEquals(7, cache.getGenerationId());
    assertEquals(42L, cache.getQuantizerSeed());
    assertEquals(4, cache.getDimension());
    assertFalse(cache.isAssumeNormalized());
    assertEquals("001", cache.getValidForInstant());

    // Verify centroids deserialized correctly
    float[][] centroids = cache.getCentroids();
    assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f, 0.4f}, centroids[0], 1e-6f);
    assertArrayEquals(new float[]{0.5f, 0.6f, 0.7f, 0.8f}, centroids[1], 1e-6f);

    // Verify cluster manifests
    Map<Integer, VectorIndexMetadataCache.ClusterManifest> manifests = cache.getClusterManifests();
    assertEquals(2, manifests.size());
    assertEquals(2, manifests.get(0).getShardCount());
    assertEquals(new HashSet<>(Arrays.asList("fg-1", "fg-2")), manifests.get(0).getFileGroupIds());
    assertEquals(50L, manifests.get(0).getVectorCount());
    assertEquals(3, manifests.get(1).getShardCount());
    assertEquals(new HashSet<>(Arrays.asList("fg-3", "fg-4", "fg-5")), manifests.get(1).getFileGroupIds());
    assertEquals(100L, manifests.get(1).getVectorCount());

    // Verify ONE MDT call was made
    verify(metadataTable, times(1)).getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true));
  }

  @Test
  void testStalenessCheck() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(4, VectorElementType.FLOAT);

    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    ByteBuffer centroidBytes = ByteBuffer.allocate(4 * 4).order(ByteOrder.LITTLE_ENDIAN);
    centroidBytes.putFloat(1f).putFloat(2f).putFloat(3f).putFloat(4f);
    centroidBytes.flip();
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "1", "IVF_RABITQ", 1, 42L, false, 100L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    assertNotNull(cache);
    assertFalse(cache.isStaleFor("001"));
    assertTrue(cache.isStaleFor("002"));
    assertTrue(cache.isStaleFor(null));
  }

  @Test
  void testFindTopClusters() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(2, VectorElementType.FLOAT);

    // 3 centroids in 2D
    ByteBuffer centroidBytes = ByteBuffer.allocate(3 * 2 * 4).order(ByteOrder.LITTLE_ENDIAN);
    centroidBytes.putFloat(0f).putFloat(0f);   // cluster 0: origin
    centroidBytes.putFloat(5f).putFloat(0f);   // cluster 1: (5,0)
    centroidBytes.putFloat(9f).putFloat(0f);   // cluster 2: (9,0)
    centroidBytes.flip();

    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "1", "IVF_RABITQ", 1, 42L, false, 100L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    assertNotNull(cache);

    // Query near (6,0) should probe clusters 1 and 2
    int[] topClusters = cache.findTopClusters(new float[]{6f, 0f}, 2, VectorDistanceMetric.L2);
    assertEquals(2, topClusters.length);
    assertEquals(1, topClusters[0]);
    assertEquals(2, topClusters[1]);
  }

  @Test
  void testGetShardCounts() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(2, VectorElementType.FLOAT);

    ByteBuffer centroidBytes = ByteBuffer.allocate(2 * 2 * 4).order(ByteOrder.LITTLE_ENDIAN);
    centroidBytes.putFloat(0f).putFloat(0f);
    centroidBytes.putFloat(1f).putFloat(1f);
    centroidBytes.flip();

    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "5", "IVF_RABITQ", 1, 42L, false, 100L, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "5", 0, 4, Arrays.asList("fg-1"), 100L, 123L, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "5", 1, 2, Arrays.asList("fg-2"), 50L, 123L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    Map<Integer, Integer> shardCounts = cache.getShardCounts(new int[]{0, 1});
    assertEquals(4, shardCounts.get(0));
    assertEquals(2, shardCounts.get(1));
  }

  @Test
  void testGetFileGroupsForClusters() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(2, VectorElementType.FLOAT);

    ByteBuffer centroidBytes = ByteBuffer.allocate(3 * 2 * 4).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 6; i++) {
      centroidBytes.putFloat(i);
    }
    centroidBytes.flip();

    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "1", "IVF_RABITQ", 1, 42L, false, 100L, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "1", 0, 1, Arrays.asList("fg-1", "fg-2"), 10L, 100L, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "1", 1, 1, Arrays.asList("fg-3"), 20L, 100L, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "1", 2, 1, Arrays.asList("fg-4", "fg-5"), 30L, 100L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    // Query clusters 0 and 2 → should get fg-1, fg-2, fg-4, fg-5
    Set<String> fileGroups = cache.getFileGroupsForClusters(new int[]{0, 2}, null);
    assertEquals(new HashSet<>(Arrays.asList("fg-1", "fg-2", "fg-4", "fg-5")), fileGroups);
  }

  @Test
  void testDeserializeCentroidsFloat() {
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(3, VectorElementType.FLOAT);
    ByteBuffer buf = ByteBuffer.allocate(2 * 3 * 4).order(ByteOrder.LITTLE_ENDIAN);
    buf.putFloat(1.0f).putFloat(2.0f).putFloat(3.0f);
    buf.putFloat(4.0f).putFloat(5.0f).putFloat(6.0f);
    buf.flip();

    float[][] centroids = VectorIndexMetadataCache.deserializeCentroids(buf, vectorSchema);
    assertEquals(2, centroids.length);
    assertArrayEquals(new float[]{1f, 2f, 3f}, centroids[0], 1e-6f);
    assertArrayEquals(new float[]{4f, 5f, 6f}, centroids[1], 1e-6f);
  }

  @Test
  void testLoadReturnsNullWhenCentroidsMissing() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(4, VectorElementType.FLOAT);

    // Only manifest, no centroids
    List<HoodieRecord<HoodieMetadataPayload>> records = Collections.singletonList(
        HoodieMetadataPayload.createVectorIndexManifestRecord(
            "7", "IVF_RABITQ", 1, 42L, false, 123L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    assertNull(cache);
  }

  @Test
  void testGetShardCountsDefaultsToOneForMissingClusters() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(2, VectorElementType.FLOAT);

    ByteBuffer centroidBytes = ByteBuffer.allocate(2 * 2 * 4).order(ByteOrder.LITTLE_ENDIAN);
    centroidBytes.putFloat(0f).putFloat(0f);
    centroidBytes.putFloat(1f).putFloat(1f);
    centroidBytes.flip();

    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(centroidBytes, INDEX_PARTITION));
    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        "1", "IVF_RABITQ", 1, 42L, false, 100L, INDEX_PARTITION));
    // Only cluster 0 has a manifest
    records.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        "1", 0, 4, Arrays.asList("fg-1"), 100L, 123L, INDEX_PARTITION));

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenReturn(HoodieListData.eager(records));

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable, INDEX_PARTITION, vectorSchema, "001");

    // Cluster 0 has 4 shards, cluster 1 is missing → defaults to 1
    Map<Integer, Integer> shardCounts = cache.getShardCounts(new int[]{0, 1});
    assertEquals(4, shardCounts.get(0));
    assertEquals(1, shardCounts.get(1));
  }
}
