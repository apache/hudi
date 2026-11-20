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

package org.apache.hudi.utilities.unstructured.embedding.audio;

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.ChunkIdScheme;
import org.apache.hudi.utilities.unstructured.embedding.prep.SequentialChunkIdScheme;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A video's speech and frame chunk ids against the embedding table's record key: both are dense from 0, and the key columns
 * {@link EmbeddingTableSchema#KEY_COLUMNS} (source version, modality, chunk_id) keep frame k and speech chunk k apart.
 */
class TestAudioChunkIds {

  @Test
  void framesAndSpeechOfOneSourceNumberedFromZeroNeverShareARecordKey() {
    String sha = "0123456789abcdef0123456789abcdef";
    int n = 50;
    ChunkIdScheme scheme = new SequentialChunkIdScheme();
    Set<List<Object>> frames = new HashSet<>();
    Set<List<Object>> speech = new HashSet<>();
    for (long k = 0; k < n; k++) {
      frames.add(key(sha, EmbeddingTableSchema.MODALITY_IMAGE, k));
      speech.add(key(sha, EmbeddingTableSchema.MODALITY_TEXT, scheme.chunkId(k)));
    }
    assertEquals(n, frames.size());
    assertEquals(n, speech.size());
    Set<List<Object>> all = new HashSet<>(frames);
    all.addAll(speech);
    assertEquals(2 * n, all.size(), "frame k and speech chunk k of one source have distinct record keys");
    assertEquals(key(sha, EmbeddingTableSchema.MODALITY_TEXT, 0L), key(sha, EmbeddingTableSchema.MODALITY_TEXT, scheme.chunkId(0)));
  }

  /** One row's values of {@link EmbeddingTableSchema#KEY_COLUMNS}, for one source version. */
  private static List<Object> key(String sha, String modality, long chunkId) {
    assertEquals(5, EmbeddingTableSchema.KEY_COLUMNS.size());
    return Arrays.asList(sha, 1000L, 16L, modality, chunkId);
  }
}
