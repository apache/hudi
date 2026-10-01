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

import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorRoutingModelRestore {

  @Test
  void restoredTwoLevelAssignmentDiffersFromFlatNearestLeaf() {
    // At the coarse level x=0 selects cell 0; leaf 1 is nearer but belongs to cell 1.
    Object model = TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
        new float[][] {{0.0f, 0.0f}, {10.0f, 0.0f}},
        new float[][] {{100.0f, 0.0f}, {11.0f, 0.0f}},
        new int[] {0, 1, 2});

    assertEquals(0, TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
        model, new float[] {0.0f, 0.0f}, 1.1f));
    assertEquals(1, TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
        model, new float[] {10.0f, 0.0f}, 1.1f));
  }

  @Test
  void serializedRoutingGeometryRestoresAndChangesDigestWhenAssignmentChanges() {
    float[][] coarse = {{0.0f, 0.0f}, {10.0f, 0.0f}};
    float[][] leaves = {{100.0f, 0.0f}, {11.0f, 0.0f}};
    int[] offsets = {0, 1, 2};
    ByteBuffer coarseBytes = VectorRoutingArtifacts.serializeFloatMatrix(coarse);
    ByteBuffer offsetBytes = VectorRoutingArtifacts.serializeIntArray(offsets);
    Object restored = VectorRoutingArtifacts.restore(VectorRoutingArtifacts.ROUTING_VERSION,
        coarseBytes, offsetBytes, 1.1f, 2, leaves);

    assertEquals(0, TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
        restored, new float[] {0.0f, 0.0f}, 1.1f));
    assertEquals(coarseBytes.position(), 0, "Restoration must not consume persisted bytes");
    assertEquals(offsetBytes.position(), 0, "Restoration must not consume persisted bytes");
    String digest = VectorRoutingArtifacts.digest(coarse, leaves, offsets, 1.1f);
    assertEquals(64, digest.length());
    assertEquals(digest, VectorRoutingArtifacts.digest(coarse, leaves, offsets, 1.1f));
    assertNotEquals(digest, VectorRoutingArtifacts.digest(coarse, leaves, offsets, 1.2f));
    assertNotEquals(digest, VectorRoutingArtifacts.digest(coarse, leaves, new int[] {0, 2, 2}, 1.1f));
    assertNotEquals(digest, VectorRoutingArtifacts.digest(coarse,
        new float[][] {{100.0f, 0.0f}, {12.0f, 0.0f}}, offsets, 1.1f));
  }

  @Test
  void rejectsInvalidSerializedRoutingGeometry() {
    float[][] leaves = {{1.0f}, {11.0f}};
    ByteBuffer validOffsets = VectorRoutingArtifacts.serializeIntArray(new int[] {0, 1, 2});
    assertThrows(HoodieMetadataException.class, () -> VectorRoutingArtifacts.restore(
        VectorRoutingArtifacts.ROUTING_VERSION + 1,
        VectorRoutingArtifacts.serializeFloatMatrix(new float[][] {{0.0f}, {10.0f}}),
        validOffsets, 1.1f, 1, leaves));
    assertThrows(HoodieMetadataException.class, () -> VectorRoutingArtifacts.restore(
        VectorRoutingArtifacts.ROUTING_VERSION, ByteBuffer.wrap(new byte[] {1}),
        validOffsets, 1.1f, 1, leaves));
    assertThrows(HoodieMetadataException.class, () -> VectorRoutingArtifacts.restore(
        VectorRoutingArtifacts.ROUTING_VERSION,
        VectorRoutingArtifacts.serializeFloatMatrix(new float[][] {{0.0f}, {10.0f}}),
        VectorRoutingArtifacts.serializeIntArray(new int[] {0, 2, 1}), 1.1f, 1, leaves));
  }

  @Test
  void rejectsInvalidPersistedLeafOffsets() {
    assertThrows(IllegalArgumentException.class, () ->
        TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
            new float[][] {{0.0f}, {10.0f}},
            new float[][] {{1.0f}, {11.0f}},
            new int[] {0, 2, 1}));
  }
}
