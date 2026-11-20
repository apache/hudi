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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the dummy endpoint's output contract independently of its vector helper. */
class TestDummyEmbeddingClient {

  @ParameterizedTest
  @ValueSource(ints = {1, 4, 17})
  void returnsFiniteUnitVectorsAtTheConfiguredDimension(int dimension) {
    try (DummyEmbeddingClient client = client(dimension)) {
      List<EmbeddingResult> results = client.embed(Arrays.asList(input("a", 0), input("a", 1), input("b", 0)));
      assertEquals(3, results.size());
      for (EmbeddingResult result : results) {
        assertEquals(EmbeddingResult.Status.OK, result.getStatus());
        assertEquals(dimension, result.getVector().length);
        double squaredNorm = 0;
        for (float value : result.getVector()) {
          assertTrue(Float.isFinite(value));
          squaredNorm += (double) value * value;
        }
        assertEquals(1.0, squaredNorm, 1e-6, "the dummy produces unit vectors");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {4, 17})
  void preservesInputOrderAndRepeatsAcrossClientInstances(int dimension) {
    EmbeddingInput first = input("a", 0);
    EmbeddingInput second = input("a", 1);
    EmbeddingInput third = input("b", 0);
    try (DummyEmbeddingClient client = client(dimension); DummyEmbeddingClient other = client(dimension)) {
      float[] firstAlone = other.embed(Collections.singletonList(first)).get(0).getVector();
      float[] secondAlone = other.embed(Collections.singletonList(second)).get(0).getVector();
      float[] thirdAlone = other.embed(Collections.singletonList(third)).get(0).getVector();
      assertFalse(Arrays.equals(firstAlone, secondAlone), "chunk identity changes the vector");
      assertFalse(Arrays.equals(firstAlone, thirdAlone), "file identity changes the vector");
      List<EmbeddingResult> reordered = client.embed(Arrays.asList(third, first, second, first));
      assertEquals(4, reordered.size());
      assertArrayEquals(thirdAlone, reordered.get(0).getVector());
      assertArrayEquals(firstAlone, reordered.get(1).getVector());
      assertArrayEquals(secondAlone, reordered.get(2).getVector());
      assertArrayEquals(firstAlone, reordered.get(3).getVector());
    }
  }

  private static DummyEmbeddingClient client(int dimension) {
    DummyEmbeddingClient client = new DummyEmbeddingClient();
    client.init(new EmbeddingModelSpec("m", "emb_m", dimension, null, "dummy", "m"), new TypedProperties());
    return client;
  }

  private static EmbeddingInput input(String sourcePathSha, long chunkId) {
    return new EmbeddingInput(sourcePathSha, chunkId, EmbeddingTableSchema.MODALITY_TEXT, "prepared chunk");
  }
}
