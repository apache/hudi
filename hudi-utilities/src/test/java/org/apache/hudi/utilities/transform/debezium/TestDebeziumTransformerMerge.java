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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end functional tests for the Debezium transformers: raw change-event envelopes are run
 * through the transformer, written to a Hudi table with the matching {@code *DebeziumAvroPayload},
 * and read back to assert that insert / update / out-of-order-update / delete merge correctly.
 *
 * <p>This exercises the full transform -> write -> read -> merge path (using the payload's
 * ordering and delete semantics), which the transformer unit tests do not cover.
 */
class TestDebeziumTransformerMerge extends SparkClientFunctionalTestHarness {

  private Dataset<Row> jsonToDataset(String... jsonDocs) {
    return spark().read().json(jsc().parallelize(Arrays.asList(jsonDocs), 1));
  }

  /** Transform the raw envelopes (flat layout) and upsert them into the Debezium table at {@code path}. */
  private void upsert(Transformer transformer, String[] envelopes, String orderingFields,
                      String payloadClass, String path, SaveMode mode) {
    upsert(transformer, new TypedProperties(), envelopes, orderingFields, payloadClass, path, mode);
  }

  /** Transform the raw envelopes with the given transformer props (e.g. nested layout) and upsert them. */
  private void upsert(Transformer transformer, TypedProperties transformerProps, String[] envelopes,
                      String orderingFields, String payloadClass, String path, SaveMode mode) {
    Dataset<Row> transformed =
        transformer.apply(jsc(), spark(), jsonToDataset(envelopes), transformerProps);
    transformed.write().format("hudi")
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id")
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), orderingFields)
        .option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), payloadClass)
        .option(HoodieWriteConfig.TBL_NAME.key(), "dbz_merge_test")
        .option(HoodieMetadataConfig.ENABLE.key(), "false")
        .mode(mode)
        .save(path);
  }

  /** Read the snapshot back as an id -> value map, so deletes drop out and updates overwrite. */
  private Map<Long, String> readIdToValue(String path, String valueCol) {
    List<Row> rows = spark().read().format("hudi").load(path).select("id", valueCol).collectAsList();
    Map<Long, String> result = new HashMap<>();
    for (Row row : rows) {
      result.put(((Number) row.getAs("id")).longValue(), row.getAs(valueCol));
    }
    return result;
  }

  // --- Postgres envelope: op, id, name (the mutable value), lsn (the ordering field) ---
  private static String pg(String op, long id, String name, long lsn) {
    String row = "{\"id\":" + id + ",\"name\":\"" + name + "\"}";
    return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000500,"
        + "\"before\":" + row + ",\"after\":" + row + ","
        + "\"source\":{\"name\":\"pgdb\",\"ts_ms\":1700000000000,\"schema\":\"public\","
        + "\"txId\":" + (500 + id) + ",\"lsn\":" + lsn + ",\"xmin\":" + (9000 + id) + "}}";
  }

  // --- MySQL envelope: op, id, title (the mutable value), pos (the ordering field within one binlog) ---
  private static String mysql(String op, long id, String title, long pos) {
    String row = "{\"id\":" + id + ",\"title\":\"" + title + "\"}";
    return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000500,"
        + "\"before\":" + row + ",\"after\":" + row + ","
        + "\"source\":{\"name\":\"mysqldb\",\"ts_ms\":1700000000000,"
        + "\"file\":\"mysql-bin.000001\",\"pos\":" + pos + ",\"row\":0}}";
  }

  @Test
  void postgresInsertUpdateOutOfOrderAndDeleteMerge() {
    String path = basePath() + "/postgres";
    String ordering = DebeziumConstants.FLATTENED_LSN_COL_NAME;
    String payload = PostgresDebeziumAvroPayload.class.getName();
    PostgresDebeziumTransformer transformer = new PostgresDebeziumTransformer();

    // 1. Insert two rows (op=c).
    upsert(transformer, new String[] {pg("c", 1, "alice", 100), pg("c", 2, "bob", 100)},
        ordering, payload, path, SaveMode.Overwrite);

    // 2. Update id=1 with a higher LSN -> should win.
    upsert(transformer, new String[] {pg("u", 1, "alice_v2", 200)}, ordering, payload, path, SaveMode.Append);

    // 3. Out-of-order update id=1 with a lower LSN -> must be ignored (LSN 50 < 200).
    upsert(transformer, new String[] {pg("u", 1, "stale", 50)}, ordering, payload, path, SaveMode.Append);

    Map<Long, String> afterUpdates = readIdToValue(path, "name");
    assertEquals(2, afterUpdates.size());
    assertEquals("alice_v2", afterUpdates.get(1L), "higher-LSN update wins; stale lower-LSN update ignored");
    assertEquals("bob", afterUpdates.get(2L));

    // 4. Delete id=2 (op=d) with the highest LSN -> row removed.
    upsert(transformer, new String[] {pg("d", 2, "bob", 300)}, ordering, payload, path, SaveMode.Append);

    Map<Long, String> afterDelete = readIdToValue(path, "name");
    assertEquals(1, afterDelete.size(), "delete removed id=2");
    assertEquals("alice_v2", afterDelete.get(1L));
    assertFalse(afterDelete.containsKey(2L), "id=2 should be deleted");
  }

  @Test
  void mysqlInsertUpdateOutOfOrderAndDeleteMerge() {
    String path = basePath() + "/mysql";
    String ordering = DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME;
    String payload = MySqlDebeziumAvroPayload.class.getName();
    MysqlDebeziumTransformer transformer = new MysqlDebeziumTransformer();

    // 1. Insert two rows (op=c).
    upsert(transformer, new String[] {mysql("c", 1, "t1", 100), mysql("c", 2, "t2", 100)},
        ordering, payload, path, SaveMode.Overwrite);

    // 2. Update id=1 with a higher binlog position -> should win.
    upsert(transformer, new String[] {mysql("u", 1, "t1_v2", 200)}, ordering, payload, path, SaveMode.Append);

    // 3. Out-of-order update id=1 with a lower position -> must be ignored (pos 50 < 200).
    upsert(transformer, new String[] {mysql("u", 1, "stale", 50)}, ordering, payload, path, SaveMode.Append);

    Map<Long, String> afterUpdates = readIdToValue(path, "title");
    assertEquals(2, afterUpdates.size());
    assertEquals("t1_v2", afterUpdates.get(1L), "higher-position update wins; stale lower-position update ignored");
    assertEquals("t2", afterUpdates.get(2L));

    // 4. Delete id=2 (op=d) with the highest position -> row removed.
    upsert(transformer, new String[] {mysql("d", 2, "t2", 300)}, ordering, payload, path, SaveMode.Append);

    Map<Long, String> afterDelete = readIdToValue(path, "title");
    assertEquals(1, afterDelete.size(), "delete removed id=2");
    assertEquals("t1_v2", afterDelete.get(1L));
    assertFalse(afterDelete.containsKey(2L), "id=2 should be deleted");
  }

  @Test
  void mysqlNestedMetadataMergeKeepsOrderingCorrect() {
    // With nested metadata enabled, the non-ordering columns move under _debezium_metadata but the
    // binlog file/pos ordering columns stay at root, so ordering + delete must still merge correctly.
    String path = basePath() + "/mysql_nested";
    String ordering = DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME;
    String payload = MySqlDebeziumAvroPayload.class.getName();
    MysqlDebeziumTransformer transformer = new MysqlDebeziumTransformer();
    TypedProperties nested = new TypedProperties();
    nested.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "true");

    upsert(transformer, nested, new String[] {mysql("c", 1, "t1", 100), mysql("c", 2, "t2", 100)},
        ordering, payload, path, SaveMode.Overwrite);
    upsert(transformer, nested, new String[] {mysql("u", 1, "t1_v2", 200)}, ordering, payload, path, SaveMode.Append);
    upsert(transformer, nested, new String[] {mysql("u", 1, "stale", 50)}, ordering, payload, path, SaveMode.Append);

    // The nested layout is actually persisted (proving nesting was in effect), yet ordering held.
    assertTrue(Arrays.asList(spark().read().format("hudi").load(path).columns())
        .contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "_debezium_metadata struct persisted");
    Map<Long, String> afterUpdates = readIdToValue(path, "title");
    assertEquals(2, afterUpdates.size());
    assertEquals("t1_v2", afterUpdates.get(1L), "higher-position update wins under nested layout; stale ignored");
    assertEquals("t2", afterUpdates.get(2L));

    upsert(transformer, nested, new String[] {mysql("d", 2, "t2", 300)}, ordering, payload, path, SaveMode.Append);
    Map<Long, String> afterDelete = readIdToValue(path, "title");
    assertEquals(1, afterDelete.size(), "delete removed id=2 under nested layout");
    assertEquals("t1_v2", afterDelete.get(1L));
    assertFalse(afterDelete.containsKey(2L), "id=2 should be deleted");
  }
}
