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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link Chunk} and {@link SequentialChunkIdScheme}: the unit a preprocessor yields and how its id is assigned. */
class TestChunk {

  @Test
  void builderCopiesTheAuxMap() {
    Map<String, String> aux = new HashMap<>();
    aux.put("page", "1");
    Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text("hello").tokenCount(1).aux(aux).build();
    aux.put("page", "2");

    assertEquals(EmbeddingTableSchema.MODALITY_TEXT, chunk.getModality());
    assertEquals("hello", chunk.getText());
    assertEquals(1, chunk.getTokenCount());
    assertEquals(Collections.singletonMap("page", "1"), chunk.getAux());
    Map<String, String> chunkAux = chunk.getAux();
    assertThrows(UnsupportedOperationException.class, () -> chunkAux.put("page", "3"));
  }

  @Test
  void constructorCopiesTheAuxMap() {
    Map<String, String> aux = new HashMap<>();
    aux.put("page", "1");
    Chunk chunk = new Chunk(EmbeddingTableSchema.MODALITY_TEXT, "hello", 1, aux);
    aux.put("page", "9");

    assertEquals(Collections.singletonMap("page", "1"), chunk.getAux());
    Map<String, String> chunkAux = chunk.getAux();
    assertThrows(UnsupportedOperationException.class, () -> chunkAux.put("page", "3"));
  }

  @Test
  void optionalFieldsDefaultToEmpty() {
    Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).aux(null).build();

    assertNull(chunk.getText());
    assertNull(chunk.getTokenCount());
    assertTrue(chunk.getAux().isEmpty());
    assertTrue(new Chunk(EmbeddingTableSchema.MODALITY_TEXT, null, null, null).getAux().isEmpty());
  }

  @Test
  void builderNeedsAModality() {
    assertThrows(NullPointerException.class, () -> Chunk.builder(null));
  }

  @Test
  void sequentialChunkIdsAreTheOrdinals() {
    SequentialChunkIdScheme scheme = new SequentialChunkIdScheme();

    assertEquals(0L, scheme.chunkId(0L));
    assertEquals(41L, scheme.chunkId(41L));
  }
}
