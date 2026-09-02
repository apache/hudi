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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestVectorIndexMetadataCacheRouting {

  private static final String INDEX_PARTITION = "vector_index_embedding_idx";

  @Test
  void queryCacheLoadUsesActivePointerAndExactGenerationKeys() {
    int generation = 7;
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    List<List<String>> observedKeys = new ArrayList<>();
    List<List<HoodieRecord<HoodieMetadataPayload>>> responses = Arrays.asList(
        Collections.singletonList(HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
            generation, INDEX_PARTITION)),
        Collections.singletonList(manifestRecord(generation)),
        Arrays.asList(centroidsRecord(generation),
            HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
                generation, 0, "IVF_RABITQ", 42L, null, INDEX_PARTITION)));
    AtomicInteger call = new AtomicInteger();

    when(metadataTable.getRecordsByKeyPrefixes(any(), eq(INDEX_PARTITION), eq(true)))
        .thenAnswer(invocation -> {
          HoodieData<? extends RawKey> keys = invocation.getArgument(0);
          observedKeys.add(keys.map(RawKey::encode).collectAsList());
          return HoodieListData.eager(responses.get(call.getAndIncrement()));
        });

    VectorIndexMetadataCache cache = VectorIndexMetadataCache.load(
        metadataTable,
        INDEX_PARTITION,
        HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.FLOAT),
        "001",
        false);

    assertNotNull(cache);
    assertEquals(generation, cache.getGenerationId());
    assertEquals(2, cache.numClusters());
    assertEquals(Arrays.asList(
        Collections.singletonList(VectorIndexMetadataKey.activeManifest()),
        Collections.singletonList(VectorIndexMetadataKey.manifest(generation)),
        Arrays.asList(
            VectorIndexMetadataKey.centroids(generation, 0),
            VectorIndexMetadataKey.quantizer(generation, 0))), observedKeys);
  }

  private static HoodieRecord<HoodieMetadataPayload> centroidsRecord(int generation) {
    ByteBuffer bytes = ByteBuffer.allocate(4 * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    bytes.putFloat(0.0f).putFloat(1.0f).putFloat(2.0f).putFloat(3.0f).flip();
    return HoodieMetadataPayload.createVectorIndexCentroidsRecord(
        generation, 0, ByteBuffer.allocate(0), bytes, ByteBuffer.allocate(0), INDEX_PARTITION);
  }

  private static HoodieRecord<HoodieMetadataPayload> manifestRecord(int generation) {
    return HoodieMetadataPayload.createVectorIndexManifestRecord(
        generation, String.valueOf(generation), "ACTIVE",
        2, 2, 1, 1, 0, 2,
        1, ByteBuffer.allocate(0), ByteBuffer.allocate(0), 1.0f, "routing",
        1, 2, "L2", false, false, "embedding",
        524288, 1024, 1, 1, 1, "rotation",
        0.0, 0.0, 0.0, 0.0,
        1, "centroids", 1, 1, "000", "000", 1L, INDEX_PARTITION);
  }
}
