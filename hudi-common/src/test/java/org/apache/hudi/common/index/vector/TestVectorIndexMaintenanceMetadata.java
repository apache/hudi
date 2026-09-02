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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.index.vector.VectorIndexMaintenanceMetadata.AFFECTED_CLUSTERS_KEY;
import static org.apache.hudi.common.index.vector.VectorIndexMaintenanceMetadata.OPERATION_KEY;
import static org.apache.hudi.common.index.vector.VectorIndexMaintenanceMetadata.TRIGGER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexMaintenanceMetadata {

  @Test
  void testCreateProducesDeterministicRoundTripMetadata() {
    Map<String, String> metadata = VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.MERGE,
        VectorIndexMaintenanceMetadata.Trigger.MERGE_FLOOR,
        Arrays.asList(9, 2, 9, 4));

    assertEquals("MERGE", metadata.get(OPERATION_KEY));
    assertEquals("MERGE_FLOOR", metadata.get(TRIGGER_KEY));
    assertEquals("2,4,9", metadata.get(AFFECTED_CLUSTERS_KEY));
    assertEquals(VectorIndexMaintenanceMetadata.Operation.MERGE,
        VectorIndexMaintenanceMetadata.operation(metadata));
    assertEquals(VectorIndexMaintenanceMetadata.Trigger.MERGE_FLOOR,
        VectorIndexMaintenanceMetadata.trigger(metadata));
    assertEquals(Arrays.asList(2, 4, 9),
        Arrays.asList(VectorIndexMaintenanceMetadata.affectedClusterIds(metadata).toArray()));
    assertThrows(UnsupportedOperationException.class,
        () -> metadata.put(OPERATION_KEY, "REBUILD"));
  }

  @Test
  void testOperationSpecificClusterRules() {
    assertThrows(IllegalArgumentException.class, () -> VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.SPLIT,
        VectorIndexMaintenanceMetadata.Trigger.SPLIT_LIMIT,
        Collections.emptyList()));
    assertThrows(IllegalArgumentException.class, () -> VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.MERGE,
        VectorIndexMaintenanceMetadata.Trigger.MERGE_FLOOR,
        Collections.singletonList(1)));
    assertThrows(IllegalArgumentException.class, () -> VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.REBUILD,
        VectorIndexMaintenanceMetadata.Trigger.SPLIT_LIMIT,
        Collections.emptyList()));
    assertThrows(IllegalArgumentException.class, () -> VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.COMPACT,
        VectorIndexMaintenanceMetadata.Trigger.DELTA_PRESSURE,
        Collections.singletonList(-1)));
  }

  @Test
  void testMalformedPersistedMetadataIsRejected() {
    Map<String, String> metadata = new HashMap<>();
    assertFalse(VectorIndexMaintenanceMetadata.hasMaintenanceOperation(metadata));

    metadata.put(OPERATION_KEY, "SPLIT");
    assertTrue(VectorIndexMaintenanceMetadata.hasMaintenanceOperation(metadata));
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexMaintenanceMetadata.validate(metadata));

    metadata.put(TRIGGER_KEY, "SPLIT_LIMIT");
    metadata.put(AFFECTED_CLUSTERS_KEY, "1,");
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexMaintenanceMetadata.validate(metadata));

    metadata.put(AFFECTED_CLUSTERS_KEY, "not-a-cluster");
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexMaintenanceMetadata.validate(metadata));
  }

  @Test
  void testRebuildDoesNotPreallocateGenerationMetadata() {
    Map<String, String> metadata = VectorIndexMaintenanceMetadata.create(
        VectorIndexMaintenanceMetadata.Operation.REBUILD,
        VectorIndexMaintenanceMetadata.Trigger.MANUAL,
        Collections.emptyList());

    assertFalse(metadata.keySet().stream().anyMatch(key -> key.contains("generation")));
    assertTrue(VectorIndexMaintenanceMetadata.affectedClusterIds(metadata).isEmpty());
  }
}
