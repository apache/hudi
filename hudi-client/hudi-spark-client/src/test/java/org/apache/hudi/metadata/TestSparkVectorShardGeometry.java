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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TestSparkVectorShardGeometry {

  @Test
  void unequalClusterPopulationsShareOnePostingShardGeometry() {
    Map<Integer, Long> populations = new HashMap<>();
    populations.put(0, 3L);
    populations.put(1, 250L);
    int shardCount = SparkVectorIndexBootstrap.generationShardCount(populations, 100, 64);

    assertEquals(3, shardCount);
    String key = "";
    for (int i = 0; i < 100 && SparkVectorIndexPostingWriter.computeShardId(key, shardCount) == 0; i++) {
      key = "uneven-cluster-posting-" + i;
    }
    assertNotEquals(SparkVectorIndexPostingWriter.computeShardId(key, 1),
        SparkVectorIndexPostingWriter.computeShardId(key, shardCount),
        "Fixture must detect silently using the smaller cluster's local shard count");
  }

  @Test
  void emptyPopulationStillHasOneShard() {
    assertEquals(1, SparkVectorIndexBootstrap.generationShardCount(
        new HashMap<>(), 100, 64));
  }
}
