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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single starter test for vector search pruning: synthetic embeddings, no external data.
 *
 * <p>Story: IVF-style index with three cluster centroids on the X axis in 3-D. Each cluster
 * maps to one Hudi file group. A query embedding is placed closest to the middle cluster;
 * {@link VectorIndexPruner#probe(float[], int)} with {@code nProbes = 1} should return only
 * that cluster's file group — the same idea as probing nearest centroids before scanning Parquet.
 *
 * <p>Run: {@code mvn test -pl hudi-common -Dtest=SampleVectorSearchTest}
 */
class SampleVectorSearchTest {

  @Test
  void sampleQueryVectorSelectsSingleNearestClusterFileGroup() {
    // Three centroids like three "regions" of embedding space (e.g. could be 384-D in production).
    float[][] centroids = {
        {0f, 0f, 0f},
        {10f, 0f, 0f},
        {20f, 0f, 0f}
    };

    Map<Integer, Set<String>> clusterToFileGroups = new HashMap<>();
    clusterToFileGroups.put(0, Collections.singleton("fg-west"));
    clusterToFileGroups.put(1, Collections.singleton("fg-mid"));
    clusterToFileGroups.put(2, Collections.singleton("fg-east"));

    VectorIndexPruner pruner =
        new VectorIndexPruner(centroids, clusterToFileGroups, VectorDistanceMetric.L2);

    // Query sits next to centroid 1 (L2 nearest is cluster 1).
    float[] query = {9f, 0f, 0f};

    Set<String> toScan = pruner.probe(query, 1);

    assertEquals(1, toScan.size());
    assertTrue(toScan.contains("fg-mid"));

    // With 2 probes, clusters 1 and 0 (or 1 and 2) contribute — union includes multiple FGs.
    Set<String> toScan2 = pruner.probe(query, 2);
    assertEquals(2, toScan2.size());
    assertTrue(toScan2.contains("fg-mid"));
    assertTrue(toScan2.contains("fg-west"));
  }
}
