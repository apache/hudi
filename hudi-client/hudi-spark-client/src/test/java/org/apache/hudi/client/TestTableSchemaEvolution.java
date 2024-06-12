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

package org.apache.hudi.client;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_1;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.EXTRA_TYPE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.FARE_NESTED_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.MAP_TYPE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TIP_NESTED_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA_PREFIX;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTableSchemaEvolution extends HoodieClientTestBase {

  private final String initCommitTime = "000";
  private HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
  private HoodieTestDataGenerator dataGenEvolved = new HoodieTestDataGenerator();
  private HoodieTestDataGenerator dataGenDevolved = new HoodieTestDataGenerator();

  public static final String EXTRA_FIELD_SCHEMA =
      "{\"name\": \"new_field\", \"type\": \"boolean\", \"default\": false},";
  public static final String EXTRA_FIELD_WITHOUT_DEFAULT_SCHEMA =
      "{\"name\": \"new_field_without_default\", \"type\": \"boolean\"},";
  public static final String EXTRA_FIELD_NULLABLE_SCHEMA =
      ",{\"name\": \"new_field_without_default\", \"type\": [\"boolean\", \"null\"]}";

  // TRIP_EXAMPLE_SCHEMA with a new_field added
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
      + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;

  // TRIP_EXAMPLE_SCHEMA with tip field removed
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_DROPPED = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
      + FARE_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;

  @Test
  public void testSchemaCompatibilityBasic() {
    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA, false),
        "Same schema is compatible");

    String reorderedSchema = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + TIP_NESTED_SCHEMA + FARE_NESTED_SCHEMA
        + MAP_TYPE_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, reorderedSchema, false),
        "Reordered fields are compatible");
    assertTrue(isSchemaCompatible(reorderedSchema, TRIP_EXAMPLE_SCHEMA, false),
        "Reordered fields are compatible");

    String renamedSchema = TRIP_EXAMPLE_SCHEMA.replace("tip_history", "tip_future");

    assertFalse(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, renamedSchema, false),
        "Renaming fields is essentially: dropping old field, created a new one");
    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, renamedSchema, true),
        "Renaming fields is essentially: dropping old field, created a new one");

    String renamedRecordSchema = TRIP_EXAMPLE_SCHEMA.replace("triprec", "triprec_renamed");
    assertFalse(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, renamedRecordSchema, false),
        "Renamed record name is not compatible");

    String swappedFieldSchema = TRIP_SCHEMA_PREFIX + MAP_TYPE_SCHEMA.replace("city_to_state", "fare")
        + FARE_NESTED_SCHEMA.replace("fare", "city_to_state") + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertFalse(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, swappedFieldSchema, false),
        "Swapped fields are not compatible");

    String typeChangeSchemaDisallowed = TRIP_SCHEMA_PREFIX + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA
        + TIP_NESTED_SCHEMA.replace("string", "boolean") + TRIP_SCHEMA_SUFFIX;
    assertFalse(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, typeChangeSchemaDisallowed, false),
        "Incompatible field type change is not allowed");

    // Array of allowed schema field type transitions
    String[][] allowedFieldChanges = {
        {"string", "bytes"}, {"bytes", "string"},
        {"int", "long"}, {"int", "float"}, {"long", "float"},
        {"int", "double"}, {"float", "double"}, {"long", "double"}};
    for (String[] fieldChange : allowedFieldChanges) {
      String fromSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA.replace("string", fieldChange[0]) + TRIP_SCHEMA_SUFFIX;
      String toSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA.replace("string", fieldChange[1]) + TRIP_SCHEMA_SUFFIX;
      assertTrue(isSchemaCompatible(fromSchema, toSchema, false),
          "Compatible field type change is not allowed");
      if (!fieldChange[0].equals("byte") && fieldChange[1].equals("byte")) {
        assertFalse(isSchemaCompatible(toSchema, fromSchema, false),
            "Incompatible field type change is allowed");
      }
    }

    // Names and aliases should match
    String fromSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;
    String toSchema = TRIP_SCHEMA_PREFIX.replace("triprec", "new_triprec") + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertFalse(isSchemaCompatible(fromSchema, toSchema, false), "Field names should match");
    assertFalse(isSchemaCompatible(toSchema, fromSchema, false), "Field names should match");


    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED, false),
        "Added field with default is compatible (Evolved Schema)");

    String multipleAddedFieldSchema = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA
        + TIP_NESTED_SCHEMA + EXTRA_FIELD_SCHEMA + EXTRA_FIELD_SCHEMA.replace("new_field", "new_new_field")
        + TRIP_SCHEMA_SUFFIX;
    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, multipleAddedFieldSchema, false),
        "Multiple added fields with defaults are compatible");

    assertFalse(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
            + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_FIELD_WITHOUT_DEFAULT_SCHEMA + TRIP_SCHEMA_SUFFIX, false),
        "Added field without default and not nullable is not compatible (Evolved Schema)");

    assertTrue(isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
            + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX + EXTRA_FIELD_NULLABLE_SCHEMA, false),
        "Added nullable field is compatible (Evolved Schema)");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMORTable(boolean shouldAllowDroppedColumns) throws Exception {
    tableType = HoodieTableType.MERGE_ON_READ;

    // Create the table
    HoodieTableMetaClient.withPropertyBuilder()
        .fromMetaClient(metaClient)
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTimelineLayoutVersion(VERSION_1)
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());

    HoodieWriteConfig hoodieWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA, shouldAllowDroppedColumns);
    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Initial inserts with TRIP_EXAMPLE_SCHEMA
    int numRecords = 10;
    insertFirstBatch(hoodieWriteConfig, client, "001", initCommitTime,
                     numRecords, SparkRDDWriteClient::insert, false, false, numRecords);
    checkLatestDeltaCommit("001");

    // Compact once so we can incrementally read later
    assertTrue(client.scheduleCompactionAtInstant("002", Option.empty()));
    client.compact("002");

    // Updates with same schema is allowed
    final int numUpdateRecords = 5;
    updateBatch(hoodieWriteConfig, client, "003", "002", Option.empty(),
                initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, 0, 0, 0);
    checkLatestDeltaCommit("003");
    checkReadRecords("000", numRecords);

    // Delete with same schema is allowed
    final int numDeleteRecords = 2;
    numRecords -= numDeleteRecords;
    deleteBatch(hoodieWriteConfig, client, "004", "003", initCommitTime, numDeleteRecords, false, false, 0, 0);
    checkLatestDeltaCommit("004");
    checkReadRecords("000", numRecords);

    // Insert with evolved schema (column dropped) is allowed
    HoodieWriteConfig hoodieDevolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_DROPPED, shouldAllowDroppedColumns);
    client = getHoodieWriteClient(hoodieDevolvedWriteConfig);
    final List<HoodieRecord> failedRecords = generateInsertsWithSchema("005", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_DROPPED);
    // We cannot use insertBatch directly here because we want to insert records
    // with a evolved schema and insertBatch inserts records using the TRIP_EXAMPLE_SCHEMA.
    try {
      writeBatch(client, "005", "004", Option.empty(), "003", numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, false, numRecords, 2 * numRecords, 5, false);
      assertTrue(shouldAllowDroppedColumns);
    } catch (HoodieInsertException e) {
      assertFalse(shouldAllowDroppedColumns);
      return;
    }

    // Update with evolved schema (column dropped) might be allowed depending on config set.
    updateBatch(hoodieDevolvedWriteConfig, client, "006", "005", Option.empty(),
                initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, numUpdateRecords, 2 * numRecords, 0);

    // Insert with an evolved scheme is allowed
    HoodieWriteConfig hoodieEvolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED, shouldAllowDroppedColumns);
    client = getHoodieWriteClient(hoodieEvolvedWriteConfig);

    // We cannot use insertBatch directly here because we want to insert records
    // with an evolved schema and insertBatch inserts records using the TRIP_EXAMPLE_SCHEMA.
    final List<HoodieRecord> evolvedRecords = generateInsertsWithSchema("007", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED);
    writeBatch(client, "007", "006", Option.empty(), initCommitTime, numRecords,
        (String s, Integer a) -> evolvedRecords, SparkRDDWriteClient::insert, false, numRecords, 3 * numRecords, 7, false);

    // new commit
    checkLatestDeltaCommit("007");
    checkReadRecords("000", 3 * numRecords);

    // Updates with evolved schema is allowed
    final List<HoodieRecord> updateRecords = generateUpdatesWithSchema("008", numUpdateRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED);
    writeBatch(client, "008", "007", Option.empty(), initCommitTime,
        numUpdateRecords, (String s, Integer a) -> updateRecords, SparkRDDWriteClient::upsert, false, numRecords, 4 * numRecords, 8, false);
    // new commit
    checkLatestDeltaCommit("008");
    checkReadRecords("000", 4 * numRecords);

    // Now try updating w/ the original schema (should succeed)
    client = getHoodieWriteClient(hoodieWriteConfig);
    try {
      updateBatch(hoodieWriteConfig, client, "009", "008", Option.empty(),
          initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, numUpdateRecords, 4 * numRecords, 9);
      assertTrue(shouldAllowDroppedColumns);
    } catch (HoodieUpsertException e) {
      assertFalse(shouldAllowDroppedColumns);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCopyOnWriteTable(boolean shouldAllowDroppedColumns) throws Exception {
    // Create the table
    HoodieTableMetaClient.withPropertyBuilder()
        .fromMetaClient(metaClient)
        .setTimelineLayoutVersion(VERSION_1)
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());

    HoodieWriteConfig hoodieWriteConfig = getWriteConfigBuilder(TRIP_EXAMPLE_SCHEMA)
        .withRollbackUsingMarkers(false)
        .withAllowAutoEvolutionColumnDrop(shouldAllowDroppedColumns)
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Initial inserts with TRIP_EXAMPLE_SCHEMA
    int numRecords = 10;
    insertFirstBatch(hoodieWriteConfig, client, "001", initCommitTime,
                     numRecords, SparkRDDWriteClient::insert, false, true, numRecords);
    checkReadRecords("000", numRecords);

    // Updates with same schema is allowed
    final int numUpdateRecords = 5;
    updateBatch(hoodieWriteConfig, client, "002", "001", Option.empty(),
                initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
                numUpdateRecords, numRecords, 2);
    checkReadRecords("000", numRecords);

    // Delete with same schema is allowed
    final int numDeleteRecords = 2;
    numRecords -= numDeleteRecords;
    deleteBatch(hoodieWriteConfig, client, "003", "002", initCommitTime, numDeleteRecords, false, true, 0, numRecords);
    checkReadRecords("000", numRecords);

    // Inserting records w/ new evolved schema (w/ tip column dropped)
    HoodieWriteConfig hoodieDevolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_DROPPED, shouldAllowDroppedColumns);
    client = getHoodieWriteClient(hoodieDevolvedWriteConfig);
    final List<HoodieRecord> failedRecords = generateInsertsWithSchema("004", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_DROPPED);
    try {
      writeBatch(client, "004", "003", Option.empty(), "003", numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, true, numRecords, numRecords * 2, 1, false);
      assertTrue(shouldAllowDroppedColumns);
    } catch (HoodieInsertException e) {
      assertFalse(shouldAllowDroppedColumns);
      return;
    }

    // Updating records w/ new evolved schema
    updateBatch(hoodieDevolvedWriteConfig, client, "005", "004", Option.empty(),
                initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
                numUpdateRecords, 2 * numRecords, 5);

    // Inserting with evolved schema is allowed
    HoodieWriteConfig hoodieEvolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED, shouldAllowDroppedColumns);
    client = getHoodieWriteClient(hoodieEvolvedWriteConfig);
    final List<HoodieRecord> evolvedRecords = generateInsertsWithSchema("006", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED);
    // We cannot use insertBatch directly here because we want to insert records
    // with a evolved schema.
    writeBatch(client, "006", "005", Option.empty(), initCommitTime, numRecords,
        (String s, Integer a) -> evolvedRecords, SparkRDDWriteClient::insert, true, numRecords, 3 * numRecords, 6, false);

    // new commit
    HoodieTimeline curTimeline = metaClient.reloadActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
    assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("006"));
    checkReadRecords("000", 3 * numRecords);

    // Updating with evolved schema is allowed
    final List<HoodieRecord> updateRecords = generateUpdatesWithSchema("007", numUpdateRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED);
    writeBatch(client, "007", "006", Option.empty(), initCommitTime,
        numUpdateRecords, (String s, Integer a) -> updateRecords, SparkRDDWriteClient::upsert, true, numUpdateRecords, 3 * numRecords, 7, false);
    checkReadRecords("000", 3 * numRecords);

    // Now try updating w/ the original schema (should succeed)
    client = getHoodieWriteClient(hoodieWriteConfig);
    try {
      updateBatch(hoodieWriteConfig, client, "008", "007", Option.empty(),
          initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
          numUpdateRecords, 3 * numRecords, 8);
      assertTrue(shouldAllowDroppedColumns);
    } catch (HoodieUpsertException e) {
      assertFalse(shouldAllowDroppedColumns);
    }
  }

  private void checkReadRecords(String instantTime, int numExpectedRecords) throws IOException {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      HoodieTimeline timeline = metaClient.reloadActiveTimeline().getCommitAndReplaceTimeline();
      assertEquals(numExpectedRecords, HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of(instantTime)));
    } else {
      // TODO: This code fails to read records under the following conditions:
      // 1. No parquet files yet (i.e. no compaction done yet)
      // 2. Log file but no base file with the same FileID
      /*
      FileStatus[] allFiles = HoodieTestUtils.listAllDataAndLogFilesInPath(metaClient.getFs(), basePath);
      HoodieTimeline timeline = metaClient.reloadActiveTimeline().getCommitsTimeline();
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline, allFiles);
      List<String> dataFiles = fsView.getLatestBaseFiles().map(hf -> hf.getPath()).collect(Collectors.toList());

      Configuration conf = new Configuration();
      String absTableName = "hoodie." + metaClient.getTableConfig().getTableName();
      conf.set(absTableName + ".consume.mode", "INCREMENTAL");
      conf.set(absTableName + ".consume.start.timestamp", instantTime);
      conf.set(absTableName + ".consume.max.commits", "-1");
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath, conf);
      assertEquals(recordsRead.size(), numExpectedRecords);
      */
    }
  }

  private void checkLatestDeltaCommit(String instantTime) {
    HoodieTimeline timeline = metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, timeline.lastInstant().get().getAction());
    assertEquals(instantTime, timeline.lastInstant().get().getTimestamp());
  }

  private List<HoodieRecord> generateInsertsWithSchema(String commitTime, int numRecords, String schemaStr) {
    HoodieTestDataGenerator gen = schemaStr.equals(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED) ? dataGenEvolved : dataGenDevolved;
    List<HoodieRecord> records = gen.generateInserts(commitTime, numRecords);
    return convertToSchema(records, schemaStr);
  }

  private List<HoodieRecord> generateUpdatesWithSchema(String commitTime, int numRecords, String schemaStr) {
    HoodieTestDataGenerator gen = schemaStr.equals(TRIP_EXAMPLE_SCHEMA_EVOLVED_COL_ADDED) ? dataGenEvolved : dataGenDevolved;
    List<HoodieRecord> records = gen.generateUniqueUpdates(commitTime, numRecords);
    return convertToSchema(records, schemaStr);
  }

  private List<HoodieRecord> convertToSchema(List<HoodieRecord> records, String schemaStr) {
    Schema newSchema = new Schema.Parser().parse(schemaStr);
    return records.stream().map(r -> {
      HoodieKey key = r.getKey();
      GenericRecord payload;
      try {
        payload = (GenericRecord) ((HoodieAvroRecord) r).getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA).get();
        GenericRecord newPayload = HoodieAvroUtils.rewriteRecord(payload, newSchema);
        return new HoodieAvroRecord(key, new RawTripTestPayload(newPayload.toString(), key.getRecordKey(), key.getPartitionPath(), schemaStr));
      } catch (IOException e) {
        throw new RuntimeException("Conversion to new schema failed");
      }
    }).collect(Collectors.toList());
  }

  private HoodieWriteConfig getWriteConfig(String schema, boolean shouldAllowDroppedColumns) {
    return getWriteConfigBuilder(schema).withAllowAutoEvolutionColumnDrop(shouldAllowDroppedColumns).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(String schema) {
    return getConfigBuilder(schema)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.INMEMORY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withAvroSchemaValidate(true);
  }

  private static boolean isSchemaCompatible(String oldSchema, String newSchema, boolean shouldAllowDroppedColumns) {
    return AvroSchemaUtils.isSchemaCompatible(new Schema.Parser().parse(oldSchema), new Schema.Parser().parse(newSchema), shouldAllowDroppedColumns);
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
