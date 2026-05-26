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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexBootstrapUtils {

  @Test
  void testBootstrapProducesCentroidAndAssignmentRecords() {
    Map<String, String> options = new HashMap<>();
    options.put(VectorIndexOptions.DIMENSION, "2");
    options.put(VectorIndexOptions.NUM_CLUSTERS, "2");
    options.put(VectorIndexOptions.MAX_ITER, "10");
    options.put(VectorIndexOptions.METRIC, "l2");

    List<VectorIndexBootstrapUtils.VectorDocument> documents = Arrays.asList(
        new VectorIndexBootstrapUtils.VectorDocument("rk-1", new double[] {0.0d, 0.0d}),
        new VectorIndexBootstrapUtils.VectorDocument("rk-2", new double[] {0.1d, 0.2d}),
        new VectorIndexBootstrapUtils.VectorDocument("rk-3", new double[] {10.0d, 10.0d}),
        new VectorIndexBootstrapUtils.VectorDocument("rk-4", new double[] {9.8d, 10.2d})
    );

    VectorIndexBootstrapUtils.BootstrapResult result =
        VectorIndexBootstrapUtils.buildBootstrapResult(
            documents, "vector_index_demo", options, HoodieSchema.Vector.VectorElementType.FLOAT);

    assertEquals(2, result.getCentroids().length);
    assertEquals(4, result.getAssignments().size());
    assertEquals(5, result.getRecords().size());

    assertEquals(result.getAssignments().get("rk-1"), result.getAssignments().get("rk-2"));
    assertEquals(result.getAssignments().get("rk-3"), result.getAssignments().get("rk-4"));
    assertTrue(!result.getAssignments().get("rk-1").equals(result.getAssignments().get("rk-3")));

    Map<String, HoodieMetadataPayload> recordsByKey = result.getRecords().stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, record -> (HoodieMetadataPayload) record.getData()));

    HoodieMetadataPayload centroidPayload = recordsByKey.get(HoodieTableMetadataUtil.VECTOR_INDEX_CENTROIDS_KEY);
    assertNotNull(centroidPayload);
    assertTrue(centroidPayload.getVectorIndexMetadata().isPresent());
    assertNotNull(centroidPayload.getVectorIndexMetadata().get().getCentroidBytes());
    assertEquals(2 * 2 * Float.BYTES, centroidPayload.getVectorIndexMetadata().get().getCentroidBytes().remaining());

    VectorIndexPruner pruner = new VectorIndexPruner(result.getCentroids(), clusterToFileGroups(result.getAssignments()), VectorDistanceMetric.L2);
    assertArrayEquals(new int[] {result.getAssignments().get("rk-1")}, pruner.findTopClusters(new float[] {0.05f, 0.05f}, 1));
  }

  @Test
  void testBootstrapSerializesDoubleCentroidsUsingDoubleWidth() {
    Map<String, String> options = new HashMap<>();
    options.put(VectorIndexOptions.DIMENSION, "2");
    options.put(VectorIndexOptions.NUM_CLUSTERS, "1");
    options.put(VectorIndexOptions.MAX_ITER, "2");
    options.put(VectorIndexOptions.METRIC, "l2");

    List<VectorIndexBootstrapUtils.VectorDocument> documents = Arrays.asList(
        new VectorIndexBootstrapUtils.VectorDocument("rk-1", new double[] {1.0d, 2.0d}),
        new VectorIndexBootstrapUtils.VectorDocument("rk-2", new double[] {3.0d, 4.0d})
    );

    VectorIndexBootstrapUtils.BootstrapResult result =
        VectorIndexBootstrapUtils.buildBootstrapResult(
            documents, "vector_index_demo", options, HoodieSchema.Vector.VectorElementType.DOUBLE);
    HoodieMetadataPayload centroidPayload = (HoodieMetadataPayload) result.getRecords().get(0).getData();
    ByteBuffer centroidBytes = centroidPayload.getVectorIndexMetadata().get().getCentroidBytes();

    assertEquals(2 * Double.BYTES, centroidBytes.remaining());
  }

  @Test
  void testMaterializeRaBitQColumnsOnCreateOptionDefaultsToFalse() {
    Map<String, String> options = new HashMap<>();
    assertTrue(!VectorIndexOptions.shouldMaterializeRaBitQColumnsOnCreate(options));

    options.put(VectorIndexOptions.RABITQ_MATERIALIZE_ON_CREATE, "true");
    assertTrue(VectorIndexOptions.shouldMaterializeRaBitQColumnsOnCreate(options));
  }

  private static Map<Integer, java.util.Set<String>> clusterToFileGroups(Map<String, Integer> assignments) {
    Map<Integer, java.util.Set<String>> clusterMap = new HashMap<>();
    assignments.forEach((recordKey, clusterId) ->
        clusterMap.computeIfAbsent(clusterId, ignored -> new java.util.HashSet<>()).add("fg-" + recordKey));
    return clusterMap;
  }
}
