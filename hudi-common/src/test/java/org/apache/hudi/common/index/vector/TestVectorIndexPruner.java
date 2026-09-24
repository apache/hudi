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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorIndexPruner {

  private static final float[][] CENTROIDS = {
      {1f, 1f},
      {1f, -1f},
      {-1f, 1f},
      {-1f, -1f}
  };

  @Test
  void selectsClosestClustersInDistanceOrder() {
    VectorIndexPruner pruner = new VectorIndexPruner(CENTROIDS, VectorDistanceMetric.L2);

    assertArrayEquals(new int[] {1, 0}, pruner.findTopClusters(new float[] {0.9f, -0.9f}, 2));
  }

  @Test
  void capsProbesToAvailableClusters() {
    VectorIndexPruner pruner = new VectorIndexPruner(CENTROIDS, VectorDistanceMetric.L2);

    assertEquals(CENTROIDS.length, pruner.findTopClusters(new float[] {0f, 0f}, 100).length);
  }

  @Test
  void breaksDistanceTiesByClusterId() {
    VectorIndexPruner pruner = new VectorIndexPruner(CENTROIDS, VectorDistanceMetric.L2);

    assertArrayEquals(new int[] {0, 1, 2, 3}, pruner.findTopClusters(new float[] {0f, 0f}, 4));
  }

  @Test
  void returnsNoClustersWhenIndexIsEmpty() {
    VectorIndexPruner pruner = new VectorIndexPruner(new float[0][0], VectorDistanceMetric.L2);

    assertArrayEquals(new int[0], pruner.findTopClusters(new float[] {1f}, 1));
    assertEquals(0, pruner.numClusters());
  }

  @Test
  void rejectsNonPositiveProbeCount() {
    VectorIndexPruner pruner = new VectorIndexPruner(CENTROIDS, VectorDistanceMetric.L2);

    assertThrows(IllegalArgumentException.class,
        () -> pruner.findTopClusters(new float[] {0f, 0f}, 0));
  }
}
