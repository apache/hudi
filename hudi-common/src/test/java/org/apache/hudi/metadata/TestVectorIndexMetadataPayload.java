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

import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
