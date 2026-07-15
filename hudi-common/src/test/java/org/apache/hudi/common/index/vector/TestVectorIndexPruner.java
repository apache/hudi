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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link VectorIndexPruner}.
 */
class TestVectorIndexPruner {

  /** 4 centroids at the corners of a 2D unit square. */
  private static final float[][] CENTROIDS = {
      {1f, 1f},   // cluster 0
      {1f, -1f},  // cluster 1
      {-1f, 1f},  // cluster 2
      {-1f, -1f}  // cluster 3
  };

  private VectorIndexPruner pruner(Map<Integer, Set<String>> clusterMap) {
    return new VectorIndexPruner(CENTROIDS, clusterMap, VectorDistanceMetric.L2);
  }

  @Test
  void probeTopOneReturnsCorrectFileGroup() {
    Map<Integer, Set<String>> map = new HashMap<>();
    map.put(0, new HashSet<>(Collections.singletonList("fg-A")));
    map.put(1, new HashSet<>(Collections.singletonList("fg-B")));
    map.put(2, new HashSet<>(Collections.singletonList("fg-C")));
    map.put(3, new HashSet<>(Collections.singletonList("fg-D")));

    VectorIndexPruner p = pruner(map);
    // Query near cluster 0 (1,1)
    Set<String> fgs = p.probe(new float[]{0.9f, 0.9f}, 1);
    assertEquals(Collections.singleton("fg-A"), fgs);
  }

  @Test
  void probeTwoReturnsUnionOfFileGroups() {
    Map<Integer, Set<String>> map = new HashMap<>();
    map.put(0, new HashSet<>(Arrays.asList("fg-A", "fg-E")));
    map.put(1, new HashSet<>(Collections.singletonList("fg-B")));
    map.put(2, new HashSet<>(Collections.singletonList("fg-C")));
    map.put(3, new HashSet<>(Collections.singletonList("fg-D")));

    VectorIndexPruner p = pruner(map);
    // Query near cluster 1 (1,-1), second nearest is cluster 0 (1,1)
    Set<String> fgs = p.probe(new float[]{0.9f, -0.9f}, 2);
    assertTrue(fgs.contains("fg-A"));
    assertTrue(fgs.contains("fg-B"));
    assertTrue(fgs.contains("fg-E"));
    assertEquals(3, fgs.size());
  }

  @Test
  void probeWithNoMatchingClustersInMapReturnsEmpty() {
    Map<Integer, Set<String>> map = new HashMap<>();
    // cluster 0 has no file groups registered
    VectorIndexPruner p = pruner(map);
    Set<String> fgs = p.probe(new float[]{0.9f, 0.9f}, 1);
    assertTrue(fgs.isEmpty());
  }

  @Test
  void probeCapsProbesToAvailableClusters() {
    Map<Integer, Set<String>> map = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      map.put(i, new HashSet<>(Collections.singletonList("fg-" + i)));
    }
    VectorIndexPruner p = pruner(map);
    // Request more probes than clusters
    Set<String> fgs = p.probe(new float[]{0f, 0f}, 100);
    assertEquals(4, fgs.size());
  }

  @Test
  void emptyCentroidsReturnsEmpty() {
    VectorIndexPruner p = new VectorIndexPruner(
        new float[0][0], Collections.emptyMap(), VectorDistanceMetric.L2);
    assertTrue(p.probe(new float[]{1f}, 1).isEmpty());
  }

  @Test
  void buildClusterMapFiltersPartitions() {
    List<Object[]> assignments = Arrays.asList(
        new Object[]{0, "fg-A", "p=2024"},
        new Object[]{1, "fg-B", "p=2023"},
        new Object[]{0, "fg-C", "p=2023"}
    );
    Set<String> filter = Collections.singleton("p=2024");
    Map<Integer, Set<String>> map =
        VectorIndexPruner.buildClusterMap(assignments, filter);
    assertEquals(1, map.size());
    assertTrue(map.get(0).contains("fg-A"));
    assertFalse(map.containsKey(1));
  }

  @Test
  void buildClusterMapNullFilterIncludesAll() {
    List<Object[]> assignments = Arrays.asList(
        new Object[]{0, "fg-A", "p=2024"},
        new Object[]{1, "fg-B", "p=2023"}
    );
    Map<Integer, Set<String>> map =
        VectorIndexPruner.buildClusterMap(assignments, null);
    assertEquals(2, map.size());
  }
}