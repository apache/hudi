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

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestVectorIndexMetadataLoader {

  @Test
  void loadsPointerManifestChunksAndQuantizer() {
    String partition = "vector-index";
    HoodieRecord<HoodieMetadataPayload> pointer =
        HoodieMetadataPayload.createVectorIndexActiveManifestRecord(2, partition);
    HoodieRecord<HoodieMetadataPayload> manifest = HoodieMetadataPayload.createVectorIndexManifestRecord(
        2, "build-2", "ACTIVE", 2, 2, 8, 1, 0, 2,
        1, ByteBuffer.allocate(2 * Float.BYTES), ByteBuffer.allocate(2 * Integer.BYTES), 1.1f, 1, 8,
        "L2", false, true, "embedding", 524288, 128,
        1, 1, 1.9, 1.0e-3, 1.0, 1.0e-3, 2, "checksum",
        4096, 1024, "002", 123L, partition);
    ByteBuffer ids = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0).putInt(1);
    ids.flip();
    ByteBuffer values = ByteBuffer.allocate(4 * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN)
        .putFloat(0f).putFloat(0f).putFloat(1f).putFloat(1f);
    values.flip();
    HoodieRecord<HoodieMetadataPayload> centroids =
        HoodieMetadataPayload.createVectorIndexCentroidsRecord(
            2, 0, ids, values, ByteBuffer.allocate(2 * Float.BYTES), partition);
    HoodieRecord<HoodieMetadataPayload> quantizer =
        HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
            2, 0, "RABITQ", 17L, null, partition);

    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getRecordsByKeyPrefixes(any(), eq(partition), eq(true))).thenAnswer(invocation -> {
      List<? extends RawKey> keys = invocation.<org.apache.hudi.common.data.HoodieData<? extends RawKey>>getArgument(0)
          .collectAsList();
      String prefix = keys.get(0).encode();
      if (prefix.equals(VectorIndexMetadataKey.activeManifest())) {
        return HoodieListData.eager(Arrays.asList(pointer));
      } else if (prefix.equals(VectorIndexMetadataKey.manifest(2))) {
        return HoodieListData.eager(Arrays.asList(manifest));
      } else if (prefix.equals(VectorIndexMetadataKey.centroidsPrefix(2))) {
        return HoodieListData.eager(Arrays.asList(centroids));
      }
      return HoodieListData.eager(Arrays.asList(quantizer));
    });

    LoadedVectorIndexMetadata loaded = VectorIndexMetadataLoader.load(metadata, partition);

    assertEquals(2, loaded.getSnapshot().getGenerationId());
    assertEquals("002", loaded.getSnapshot().getLastContiguousSourceInstant());
    assertEquals(17L, loaded.getRandomSeed());
    assertEquals(1, loaded.getShardCounts().get(0));
    assertEquals(1f, loaded.getCentroids()[1][0]);
  }
}
