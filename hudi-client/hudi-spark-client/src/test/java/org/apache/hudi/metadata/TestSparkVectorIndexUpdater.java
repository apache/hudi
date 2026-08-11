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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.avro.model.HoodieVectorIndexTombstone;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchema.Vector.VectorElementType.FLOAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkVectorIndexUpdater {

  private static final int GENERATION = 3;
  private static final String INSTANT = "20260805120000";
  private static final String INDEX_PARTITION = "vector_index_embedding";
  private static final SparkVectorIndexUpdater.Artifacts ARTIFACTS =
      new SparkVectorIndexUpdater.Artifacts(
          new float[][] {{0.0f, 0.0f}},
          TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
              new float[][] {{0.0f, 0.0f}},
              new float[][] {{0.0f, 0.0f}},
              new int[] {0, 1}),
          1.1f,
          1,
          VectorDistanceMetric.L2, 2, 1, 42L, false, false);

  @Test
  void testInsertEmitsPostingWithPersistedRowPosition() {
    SparkVectorIndexBootstrap.VectorRow inserted = row(
        "id-1", "p1", "file-1", "001", floats(1.0f, 2.0f), 37L);

    List<HoodieRecord> records = classify(Collections.emptyMap(), rows(inserted));

    assertEquals(1, records.size());
    HoodieVectorIndexPostingDelta posting = posting(records.get(0));
    assertEquals("id-1", posting.getRecordKey());
    assertEquals("file-1", posting.getFileGroupId());
    assertEquals("p1", posting.getPartitionPath());
    assertEquals("001", posting.getBaseInstantTime());
    assertEquals(37L, posting.getRowPosition());
  }

  @Test
  void testDeleteEmitsOnlyOldPostingTombstone() {
    SparkVectorIndexBootstrap.VectorRow deleted = row(
        "id-1", "p1", "file-1", "001", floats(1.0f, 2.0f), 5L);

    List<HoodieRecord> records = classify(rows(deleted), Collections.emptyMap());

    assertEquals(1, records.size());
    HoodieVectorIndexTombstone tombstone = assertInstanceOf(
        HoodieVectorIndexTombstone.class, metadata(records.get(0)));
    assertEquals(INSTANT, tombstone.getDeleteInstant());
  }

  @Test
  void testBytewiseVectorChangeWithinClusterEmitsReplacementPosting() {
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-1", "p1", "file-1", "001", floats(0.0f, 1.0f), 5L);
    // +0.0f and -0.0f compare numerically equal but have distinct stored bytes.
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-1", "p1", "file-1", "001", floats(-0.0f, 1.0f), 6L);

    List<HoodieRecord> records = classify(rows(previous), rows(current));

    assertEquals(1, records.size());
    HoodieVectorIndexPostingDelta posting = posting(records.get(0));
    assertEquals(6L, posting.getRowPosition());
  }

  @Test
  void testVectorChangeAcrossClustersTombstonesOldPostingAndAddsNewPosting() {
    float[][] centroids = {{0.0f, 0.0f}, {10.0f, 10.0f}};
    Object routingModel = TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
        centroids, centroids, new int[] {0, 1, 2});
    SparkVectorIndexUpdater.Artifacts artifacts = new SparkVectorIndexUpdater.Artifacts(
        centroids, routingModel, 1.1f, 1,
        VectorDistanceMetric.L2, 2, 1, 42L, false, false);
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-moving", "p1", "file-1", "001", floats(0.0f, 0.0f), 5L);
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-moving", "p1", "file-1", "001", floats(10.0f, 10.0f), 6L);

    List<HoodieRecord> records = classify(rows(previous), rows(current), artifacts);

    assertEquals(2, records.size());
    assertEquals(
        VectorIndexMetadataKey.postingDelta(GENERATION, 0, 0, "id-moving"),
        records.get(0).getRecordKey());
    assertInstanceOf(HoodieVectorIndexTombstone.class, metadata(records.get(0)));
    assertEquals(
        VectorIndexMetadataKey.postingDelta(GENERATION, 1, 0, "id-moving"),
        records.get(1).getRecordKey());
    HoodieVectorIndexPostingDelta posting = posting(records.get(1));
    assertEquals(6L, posting.getRowPosition());
  }

  @Test
  void testLocatorOnlyRewriteEmitsNewPostingWithoutTombstone() {
    byte[] vector = floats(1.0f, 2.0f);
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-1", "p1", "file-1", "001", vector, 5L);
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-1", "p2", "file-2", "002", vector.clone(), 41L);

    List<HoodieRecord> records = classify(rows(previous), rows(current));

    assertEquals(1, records.size());
    HoodieVectorIndexPostingDelta posting = posting(records.get(0));
    assertEquals("p2", posting.getPartitionPath());
    assertEquals("file-2", posting.getFileGroupId());
    assertEquals("002", posting.getBaseInstantTime());
    assertEquals(41L, posting.getRowPosition());
  }

  @Test
  void testUpdaterUsesPersistedTwoLevelPlacementInsteadOfFlatArgmin() {
    float[][] leaves = {{100.0f, 0.0f}, {11.0f, 0.0f}};
    Object routingModel = TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
        new float[][] {{0.0f, 0.0f}, {10.0f, 0.0f}}, leaves, new int[] {0, 1, 2});
    SparkVectorIndexUpdater.Artifacts artifacts = new SparkVectorIndexUpdater.Artifacts(
        leaves, routingModel, 1.1f, 1,
        VectorDistanceMetric.L2, 2, 1, 42L, false, false);
    SparkVectorIndexBootstrap.VectorRow deleted = row(
        "id-boundary", "p1", "file-1", "001", floats(0.0f, 0.0f), 5L);

    List<HoodieRecord> records = classify(rows(deleted), Collections.emptyMap(), artifacts);

    assertEquals(1, records.size());
    assertEquals(
        VectorIndexMetadataKey.postingDelta(GENERATION, 0, 0, "id-boundary"),
        records.get(0).getRecordKey());
  }

  @Test
  void testValidToInvalidVectorEmitsTombstone() {
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-transition", "p1", "file-1", "001", floats(1.0f, 2.0f), 5L);
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-transition", "p1", "file-1", "001", floats(Float.NaN, 2.0f), 6L);

    List<HoodieRecord> records = classify(rows(previous), rows(current));

    assertEquals(1, records.size());
    assertInstanceOf(HoodieVectorIndexTombstone.class, metadata(records.get(0)));
  }

  @Test
  void testInvalidToValidVectorEmitsPosting() {
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-transition", "p1", "file-1", "001", new byte[] {1, 2}, 5L);
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-transition", "p1", "file-1", "001", floats(1.0f, 2.0f), 6L);

    List<HoodieRecord> records = classify(rows(previous), rows(current));

    assertEquals(1, records.size());
    assertEquals(6L, posting(records.get(0)).getRowPosition());
  }

  @Test
  void testLogBackedCurrentRowPreservesKeyLookupLocator() {
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-log", "p1", "file-1", "001", floats(1.0f, 2.0f), -1L);

    List<HoodieRecord> records = classify(Collections.emptyMap(), rows(current));

    assertEquals(1, records.size());
    assertEquals(-1L, posting(records.get(0)).getRowPosition());
  }

  @Test
  void testInvalidCurrentVectorIsNotIndexed() {
    SparkVectorIndexBootstrap.VectorRow invalid = row(
        "id-invalid", "p1", "file-1", "001", floats(Float.NaN, 1.0f), 5L);

    assertTrue(classify(Collections.emptyMap(), rows(invalid)).isEmpty());
  }

  @Test
  void testInvalidPreviousVectorSkipsDerivedTombstone() {
    SparkVectorIndexBootstrap.VectorRow invalid = row(
        "id-invalid", "p1", "file-1", "001", new byte[] {1, 2}, 5L);

    assertTrue(classify(rows(invalid), Collections.emptyMap()).isEmpty());
  }

  @Test
  void testExactNoOpEmitsNothing() {
    SparkVectorIndexBootstrap.VectorRow previous = row(
        "id-1", "p1", "file-1", "001", floats(1.0f, 2.0f), 5L);
    SparkVectorIndexBootstrap.VectorRow current = row(
        "id-1", "p1", "file-1", "001", previous.vectorBytes.clone(), 5L);

    assertTrue(classify(rows(previous), rows(current)).isEmpty());
  }

  private static List<HoodieRecord> classify(
      Map<String, SparkVectorIndexBootstrap.VectorRow> previous,
      Map<String, SparkVectorIndexBootstrap.VectorRow> current) {
    return classify(previous, current, ARTIFACTS);
  }

  private static List<HoodieRecord> classify(
      Map<String, SparkVectorIndexBootstrap.VectorRow> previous,
      Map<String, SparkVectorIndexBootstrap.VectorRow> current,
      SparkVectorIndexUpdater.Artifacts artifacts) {
    return SparkVectorIndexUpdater.classifyPostingRecords(
        new SparkVectorIndexUpdater.FileGroupRows(previous, current),
        artifacts, FLOAT, GENERATION, INSTANT, INDEX_PARTITION);
  }

  private static Map<String, SparkVectorIndexBootstrap.VectorRow> rows(
      SparkVectorIndexBootstrap.VectorRow... values) {
    Map<String, SparkVectorIndexBootstrap.VectorRow> rows = new HashMap<>();
    for (SparkVectorIndexBootstrap.VectorRow value : values) {
      rows.put(value.recordKey, value);
    }
    return rows;
  }

  private static SparkVectorIndexBootstrap.VectorRow row(
      String key, String partition, String fileId, String baseInstant,
      byte[] vector, long rowPosition) {
    return new SparkVectorIndexBootstrap.VectorRow(
        key, partition, fileId, baseInstant, vector, rowPosition);
  }

  private static byte[] floats(float... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES)
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  private static Object metadata(HoodieRecord record) {
    HoodieMetadataPayload payload = (HoodieMetadataPayload) record.getData();
    return payload.getVectorIndexMetadata().get();
  }

  private static HoodieVectorIndexPostingDelta posting(HoodieRecord record) {
    return assertInstanceOf(HoodieVectorIndexPostingDelta.class, metadata(record));
  }
}
