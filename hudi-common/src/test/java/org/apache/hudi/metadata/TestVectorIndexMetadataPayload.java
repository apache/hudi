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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieVectorIndexActiveManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexClusterStats;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.avro.model.HoodieVectorIndexSourceInstantMarker;
import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexMetadataPayload {

  @Test
  void testPostingRecordCarriesCanonicalLookupMetadata() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexPostingRecord(
        7,
        "rk-1",
        3,
        1,
        "file-group-1",
        "dt=2026-04-01",
        "20260603120000",
        new byte[] {0x01, 0x02},
        1.5f,
        123456789L,
        "vector_index_demo");

    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    HoodieVectorIndexPostingDelta delta =
        (HoodieVectorIndexPostingDelta) record.getData().getVectorIndexMetadata().get();
    assertEquals("rk-1", delta.getRecordKey());
    assertEquals(3, VectorIndexMetadataKey.postingClusterId(record.getRecordKey()));
    assertEquals(1, VectorIndexMetadataKey.postingShard(record.getRecordKey()));
    assertEquals("file-group-1", delta.getFileGroupId());
    assertEquals("dt=2026-04-01", delta.getPartitionPath());
    assertEquals("20260603120000", delta.getBaseInstantTime());
  }

  @Test
  void testActiveManifestCarriesReaderVisibleGeneration() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
        2, "vector_index_demo");

    HoodieVectorIndexActiveManifest manifest =
        (HoodieVectorIndexActiveManifest) record.getData().getVectorIndexMetadata().get();
    assertEquals(VectorIndexMetadataKey.activeManifest(), record.getRecordKey());
    assertEquals(1, manifest.getIndexVersion());
    assertEquals(2, manifest.getActiveGeneration());
  }

  @Test
  void testEpochFreeCentroidRecordUsesGenerationAndChunkKey() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexCentroidsRecord(
        2,
        7,
        ByteBuffer.wrap(new byte[] {1, 0, 0, 0}),
        ByteBuffer.wrap(new byte[] {2, 3}),
        ByteBuffer.wrap(new byte[] {4, 5}),
        "vector_index_demo");

    HoodieVectorIndexCentroids centroids =
        (HoodieVectorIndexCentroids) record.getData().getVectorIndexMetadata().get();
    assertEquals(VectorIndexMetadataKey.centroids(2, 7), record.getRecordKey());
    assertEquals(2, centroids.getCentroidBytes().remaining());
    assertNull(HoodieVectorIndexCentroids.getClassSchema().getField("centroidEpoch"));
  }

  @Test
  void testManifestCarriesVerifiedContiguousFrontierWithoutEpoch() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexManifestRecord(
        2, "build-2", "BUILDING", 128, 128, 16, 2, 1, 64, 1, 8,
        "COSINE", true, true, "embedding", 524288, 2048,
        1, 2, 1.9, 1.0e-3, 1.0, 1.0e-3, 4, "sha256:centroids",
        4096, 1024, "20260724000000", "20260724010101", 123L, "vector_index_demo");

    HoodieVectorIndexManifest manifest =
        (HoodieVectorIndexManifest) record.getData().getVectorIndexMetadata().get();
    assertEquals("20260724000000", manifest.getBootstrapInstant());
    assertEquals("20260724010101", manifest.getVerifiedFrontier());
    assertEquals(8, manifest.getFileGroupCount());
    assertEquals(1, manifest.getBlockFormatVersion());
    assertEquals(2, manifest.getFactorVersion());
    assertEquals(1.9, manifest.getKappa());
    assertEquals(1.0e-3, manifest.getGMin());
    assertEquals(1.0, manifest.getEps1Max());
    assertEquals(1.0e-3, manifest.getEpsNRel());
    assertEquals(4, manifest.getCentroidChunkCount());
    assertEquals("sha256:centroids", manifest.getCentroidChecksum());
    assertEquals("BUILDING", manifest.getState().toString());
    assertNull(HoodieVectorIndexManifest.getClassSchema().getField("centroidEpoch"));
  }

  @Test
  void testClusterManifestPersistsRoutingFields() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
        2, 9, 2, Arrays.asList("fg-a", "fg-b"), 17L, 123L, "vector_index_demo");

    HoodieVectorIndexClusterStats stats =
        (HoodieVectorIndexClusterStats) record.getData().getVectorIndexMetadata().get();
    assertEquals(0, stats.getRoutingVersion());
    assertEquals(2, stats.getShardCount());
    assertEquals(Arrays.asList("fg-a", "fg-b"), stats.getFileGroupIds());
    assertEquals(17L, stats.getLiveCount());
    assertNull(HoodieVectorIndexClusterStats.getClassSchema().getField("centroidEpoch"));
  }

  @Test
  void testSourceInstantMarkerCarriesSourceIdentity() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexSourceInstantMarkerRecord(
        2, "20260724010101", "vector_index_demo");

    HoodieVectorIndexSourceInstantMarker marker =
        (HoodieVectorIndexSourceInstantMarker) record.getData().getVectorIndexMetadata().get();
    assertEquals(VectorIndexMetadataKey.sourceInstantMarker(2, "20260724010101"), record.getRecordKey());
    assertEquals("20260724010101", marker.getDataInstant().toString());
  }

  @Test
  void testQuantizerMetadataRecordCarriesRaBitQConfig() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        "IVF_RABITQ",
        96,
        42L,
        true,
        "vector_index_demo");

    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    HoodieVectorIndexQuantizer quantizer =
        (HoodieVectorIndexQuantizer) record.getData().getVectorIndexMetadata().get();
    assertEquals("IVF_RABITQ", quantizer.getQuantizerType());
    assertEquals(42L, quantizer.getRandomSeed());
  }
}
