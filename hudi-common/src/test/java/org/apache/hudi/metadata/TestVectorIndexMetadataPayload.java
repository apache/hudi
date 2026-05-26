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

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexMetadataPayload {

  @Test
  void testAssignmentRecordCarriesFileGroupMetadata() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexAssignmentRecord(
        "rk-1",
        7,
        "file-group-1",
        "dt=2026-04-01",
        "vector_index_demo");

    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    assertEquals(7, record.getData().getVectorIndexMetadata().get().getClusterId());
    assertEquals("file-group-1", record.getData().getVectorIndexMetadata().get().getFileGroupId());
    assertEquals("dt=2026-04-01", record.getData().getVectorIndexMetadata().get().getPartitionPath());
  }

  @Test
  void testFgMappingRecordUsesForwardMapKey() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexFgMappingRecord(
        3,
        "dt=2026-04-01",
        Arrays.asList("fg-1", "fg-2"),
        12L,
        123456789L,
        "vector_index_demo");

    assertTrue(HoodieTableMetadataUtil.isVectorIndexFgMappingKey(record.getRecordKey()));
    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    assertEquals(3, record.getData().getVectorIndexMetadata().get().getClusterId());
    assertEquals(Arrays.asList("fg-1", "fg-2"), record.getData().getVectorIndexMetadata().get().getFileGroupIds());
    assertEquals(12L, record.getData().getVectorIndexMetadata().get().getVectorCount());
    assertEquals(123456789L, record.getData().getVectorIndexMetadata().get().getLastUpdatedTs());
  }

  @Test
  void testQuantizerMetadataRecordCarriesRaBitQConfig() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        "IVF_RABITQ",
        96,
        42L,
        true,
        "vector_index_demo");

    assertEquals(HoodieTableMetadataUtil.VECTOR_INDEX_QUANTIZER_KEY, record.getRecordKey());
    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    assertEquals("IVF_RABITQ", record.getData().getVectorIndexMetadata().get().getQuantizerType());
    assertEquals(96, record.getData().getVectorIndexMetadata().get().getQuantizedCodeBytes());
    assertEquals(42L, record.getData().getVectorIndexMetadata().get().getRandomSeed());
    assertTrue(record.getData().getVectorIndexMetadata().get().getAssumeNormalized());
  }
}
