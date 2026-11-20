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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieListData;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestVectorPostingScorer {

  private static final long SEED = 17L;

  /**
   * Posting-block sign rows are padded to a long-aligned codeRowBytes, but the single-bit scorer
   * requires exactly ceil(dimension / 8) bytes. Scoring a padded row must match the unpadded code.
   */
  @ParameterizedTest
  @ValueSource(ints = {64, 100, 130})
  void singleBitScoringAcceptsLongAlignedSignRows(int dimension) {
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, 1, SEED, false);
    float[] vector = randomVector(dimension, 1);
    float[] query = randomVector(dimension, 2);
    QuantizedVector encoded = encoder.encode(vector);
    int codeRowBytes = ((dimension + 63) / 64) * Long.BYTES;
    byte[] paddedSignRow = Arrays.copyOf(encoded.getCode(), codeRowBytes);

    float expected = encoder.estimateDistance(
        encoder.encodeQuery(query), encoded, VectorDistanceMetric.L2, false);
    List<ScoredVectorPostingMatch> scored = VectorPostingScorer.score(
        HoodieListData.eager(Collections.singletonList(new VectorPostingMatch(
            "key", 0, 0, "fg-0", "p", "001", 0L, paddedSignRow, encoded.getScalar()))),
        query, dimension, SEED, 1, false, VectorDistanceMetric.L2, false, false, null)
        .collectAsList();

    assertEquals(1, scored.size());
    assertEquals(expected, scored.get(0).getApproxDistance(), 1e-6f);
  }

  private static float[] randomVector(int dimension, long seed) {
    Random random = new Random(seed);
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random.nextFloat() * 2f - 1f;
    }
    return vector;
  }
}
