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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
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
import static org.junit.jupiter.api.Assertions.fail;

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
  public static final String TRIP_EXAMPLE_SCHEMA_EVOLVED = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
      + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;

  // TRIP_EXAMPLE_SCHEMA with tip field removed
  public static final String TRIP_EXAMPLE_SCHEMA_DEVOLVED = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
      + FARE_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;

  @Test
  public void testSchemaCompatibilityBasic() throws Exception {
    assertTrue(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA),
        "Same schema is compatible");

    String reorderedSchema = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + TIP_NESTED_SCHEMA + FARE_NESTED_SCHEMA
        + MAP_TYPE_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertTrue(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, reorderedSchema),
        "Reordered fields are compatible");
    assertTrue(TableSchemaResolver.isSchemaCompatible(reorderedSchema, TRIP_EXAMPLE_SCHEMA),
        "Reordered fields are compatible");

    String renamedSchema = TRIP_EXAMPLE_SCHEMA.replace("tip_history", "tip_future");
    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, renamedSchema),
        "Renamed fields are not compatible");

    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA_DEVOLVED),
        "Deleted single field is not compatible");
    String deletedMultipleFieldSchema = TRIP_SCHEMA_PREFIX + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, deletedMultipleFieldSchema),
        "Deleted multiple fields are not compatible");

    String renamedRecordSchema = TRIP_EXAMPLE_SCHEMA.replace("triprec", "triprec_renamed");
    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, renamedRecordSchema),
        "Renamed record name is not compatible");

    String swappedFieldSchema = TRIP_SCHEMA_PREFIX + MAP_TYPE_SCHEMA.replace("city_to_state", "fare")
        + FARE_NESTED_SCHEMA.replace("fare", "city_to_state") + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, swappedFieldSchema),
        "Swapped fields are not compatible");

    String typeChangeSchemaDisallowed = TRIP_SCHEMA_PREFIX + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA
        + TIP_NESTED_SCHEMA.replace("string", "boolean") + TRIP_SCHEMA_SUFFIX;
    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, typeChangeSchemaDisallowed),
        "Incompatible field type change is not allowed");

    // Array of allowed schema field type transitions
    String[][] allowedFieldChanges = {
        {"string", "bytes"}, {"bytes", "string"},
        {"int", "long"}, {"int", "float"}, {"long", "float"},
        {"int", "double"}, {"float", "double"}, {"long", "double"}};
    for (String[] fieldChange : allowedFieldChanges) {
      String fromSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA.replace("string", fieldChange[0]) + TRIP_SCHEMA_SUFFIX;
      String toSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA.replace("string", fieldChange[1]) + TRIP_SCHEMA_SUFFIX;
      assertTrue(TableSchemaResolver.isSchemaCompatible(fromSchema, toSchema),
          "Compatible field type change is not allowed");
      if (!fieldChange[0].equals("byte") && fieldChange[1].equals("byte")) {
        assertFalse(TableSchemaResolver.isSchemaCompatible(toSchema, fromSchema),
            "Incompatible field type change is allowed");
      }
    }

    // Names and aliases should match
    String fromSchema = TRIP_SCHEMA_PREFIX + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;
    String toSchema = TRIP_SCHEMA_PREFIX.replace("triprec", "new_triprec") + EXTRA_FIELD_SCHEMA + TRIP_SCHEMA_SUFFIX;
    assertFalse(TableSchemaResolver.isSchemaCompatible(fromSchema, toSchema), "Field names should match");
    assertFalse(TableSchemaResolver.isSchemaCompatible(toSchema, fromSchema), "Field names should match");


    assertTrue(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA_EVOLVED),
        "Added field with default is compatible (Evolved Schema)");

    String multipleAddedFieldSchema = TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA + FARE_NESTED_SCHEMA
        + TIP_NESTED_SCHEMA + EXTRA_FIELD_SCHEMA + EXTRA_FIELD_SCHEMA.replace("new_field", "new_new_field")
        + TRIP_SCHEMA_SUFFIX;
    assertTrue(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA, multipleAddedFieldSchema),
        "Multiple added fields with defaults are compatible");

    assertFalse(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA,
        TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
            + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + EXTRA_FIELD_WITHOUT_DEFAULT_SCHEMA + TRIP_SCHEMA_SUFFIX),
        "Added field without default and not nullable is not compatible (Evolved Schema)");

    assertTrue(TableSchemaResolver.isSchemaCompatible(TRIP_EXAMPLE_SCHEMA,
        TRIP_SCHEMA_PREFIX + EXTRA_TYPE_SCHEMA + MAP_TYPE_SCHEMA
            + FARE_NESTED_SCHEMA + TIP_NESTED_SCHEMA + TRIP_SCHEMA_SUFFIX + EXTRA_FIELD_NULLABLE_SCHEMA),
        "Added nullable field is compatible (Evolved Schema)");
  }

  @Test
  public void testMORTable() throws Exception {
    tableType = HoodieTableType.MERGE_ON_READ;

    // Create the table
    HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTableType(HoodieTableType.MERGE_ON_READ)
      .setTimelineLayoutVersion(VERSION_1)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());

    HoodieWriteConfig hoodieWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA);
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
    deleteBatch(hoodieWriteConfig, client, "004", "003", initCommitTime, numDeleteRecords,
        SparkRDDWriteClient::delete, false, false, 0, 0);
    checkLatestDeltaCommit("004");
    checkReadRecords("000", numRecords);

    // Insert with evolved schema is not allowed
    HoodieWriteConfig hoodieDevolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_DEVOLVED);
    client = getHoodieWriteClient(hoodieDevolvedWriteConfig);
    final List<HoodieRecord> failedRecords = generateInsertsWithSchema("004", numRecords, TRIP_EXAMPLE_SCHEMA_DEVOLVED);
    try {
      // We cannot use insertBatch directly here because we want to insert records
      // with a devolved schema and insertBatch inserts records using the TRIP_EXAMPLE_SCHEMA.
      writeBatch(client, "005", "004", Option.empty(), "003", numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, false, 0, 0, 0, false);
      fail("Insert with devolved scheme should fail");
    } catch (HoodieInsertException ex) {
      // no new commit
      checkLatestDeltaCommit("004");
      checkReadRecords("000", numRecords);
      client.rollback("005");
    }

    // Update with devolved schema is also not allowed
    try {
      updateBatch(hoodieDevolvedWriteConfig, client, "005", "004", Option.empty(),
                  initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, 0, 0, 0);
      fail("Update with devolved scheme should fail");
    } catch (HoodieUpsertException ex) {
      // no new commit
      checkLatestDeltaCommit("004");
      checkReadRecords("000", numRecords);
      client.rollback("005");
    }

    // Insert with an evolved scheme is allowed
    HoodieWriteConfig hoodieEvolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED);
    client = getHoodieWriteClient(hoodieEvolvedWriteConfig);

    // We cannot use insertBatch directly here because we want to insert records
    // with an evolved schema and insertBatch inserts records using the TRIP_EXAMPLE_SCHEMA.
    final List<HoodieRecord> evolvedRecords = generateInsertsWithSchema("005", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED);
    writeBatch(client, "005", "004", Option.empty(), initCommitTime, numRecords,
        (String s, Integer a) -> evolvedRecords, SparkRDDWriteClient::insert, false, 0, 0, 0, false);

    // new commit
    checkLatestDeltaCommit("005");
    checkReadRecords("000", 2 * numRecords);

    // Updates with evolved schema is allowed
    final List<HoodieRecord> updateRecords = generateUpdatesWithSchema("006", numUpdateRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED);
    writeBatch(client, "006", "005", Option.empty(), initCommitTime,
        numUpdateRecords, (String s, Integer a) -> updateRecords, SparkRDDWriteClient::upsert, false, 0, 0, 0, false);
    // new commit
    checkLatestDeltaCommit("006");
    checkReadRecords("000", 2 * numRecords);

    // Now even the original schema cannot be used for updates as it is devolved in relation to the
    // current schema of the dataset.
    client = getHoodieWriteClient(hoodieWriteConfig);
    try {
      updateBatch(hoodieWriteConfig, client, "007", "006", Option.empty(),
                  initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, 0, 0, 0);
      fail("Update with original scheme should fail");
    } catch (HoodieUpsertException ex) {
      // no new commit
      checkLatestDeltaCommit("006");
      checkReadRecords("000", 2 * numRecords);
      client.rollback("007");
    }

    // Now even the original schema cannot be used for inserts as it is devolved in relation to the
    // current schema of the dataset.
    try {
      // We are not using insertBatch directly here because insertion of these
      // records will fail and we dont want to keep these records within HoodieTestDataGenerator as we
      // will be testing updates later.
      failedRecords.clear();
      failedRecords.addAll(dataGen.generateInserts("007", numRecords));
      writeBatch(client, "007", "006", Option.empty(), initCommitTime, numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, true, numRecords, numRecords, 1, false);
      fail("Insert with original scheme should fail");
    } catch (HoodieInsertException ex) {
      // no new commit
      checkLatestDeltaCommit("006");
      checkReadRecords("000", 2 * numRecords);
      client.rollback("007");

      // Remove the inserts from the in-memory state of HoodieTestDataGenerator
      // as these records were never inserted in the dataset. This is required so
      // that future calls to updateBatch or deleteBatch do not generate updates
      // or deletes for records which do not even exist.
      for (HoodieRecord record : failedRecords) {
        assertTrue(dataGen.deleteExistingKeyIfPresent(record.getKey()));
      }
    }

    // Rollback to the original schema
    client.restoreToInstant("004", hoodieWriteConfig.isMetadataTableEnabled());
    checkLatestDeltaCommit("004");

    // Updates with original schema are now allowed
    client = getHoodieWriteClient(hoodieWriteConfig);
    updateBatch(hoodieWriteConfig, client, "008", "004", Option.empty(),
                initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, false, 0, 0, 0);
    // new commit
    checkLatestDeltaCommit("008");
    checkReadRecords("000", 2 * numRecords);

    // Insert with original schema is allowed now
    insertBatch(hoodieWriteConfig, client, "009", "008", numRecords, SparkRDDWriteClient::insert,
        false, false, 0, 0, 0, Option.empty());
    checkLatestDeltaCommit("009");
    checkReadRecords("000", 3 * numRecords);
  }

  @Test
  public void testCopyOnWriteTable() throws Exception {
    // Create the table
    HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTimelineLayoutVersion(VERSION_1)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());

    HoodieWriteConfig hoodieWriteConfig = getWriteConfigBuilder(TRIP_EXAMPLE_SCHEMA).withRollbackUsingMarkers(false).build();
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
    deleteBatch(hoodieWriteConfig, client, "003", "002", initCommitTime, numDeleteRecords,
        SparkRDDWriteClient::delete, false, true, 0, numRecords);
    checkReadRecords("000", numRecords);

    // Insert with devolved schema is not allowed
    HoodieWriteConfig hoodieDevolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_DEVOLVED);
    client = getHoodieWriteClient(hoodieDevolvedWriteConfig);
    final List<HoodieRecord> failedRecords = generateInsertsWithSchema("004", numRecords, TRIP_EXAMPLE_SCHEMA_DEVOLVED);
    try {
      // We cannot use insertBatch directly here because we want to insert records
      // with a devolved schema.
      writeBatch(client, "004", "003", Option.empty(), "003", numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, true, numRecords, numRecords, 1, false);
      fail("Insert with devolved scheme should fail");
    } catch (HoodieInsertException ex) {
      // no new commit
      HoodieTimeline curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
      assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("003"));
      client.rollback("004");
    }

    // Update with devolved schema is not allowed
    try {
      updateBatch(hoodieDevolvedWriteConfig, client, "004", "003", Option.empty(),
                  initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
                  numUpdateRecords, 2 * numRecords, 5);
      fail("Update with devolved scheme should fail");
    } catch (HoodieUpsertException ex) {
      // no new commit
      HoodieTimeline curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
      assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("003"));
      client.rollback("004");
    }

    // Insert with evolved scheme is allowed
    HoodieWriteConfig hoodieEvolvedWriteConfig = getWriteConfig(TRIP_EXAMPLE_SCHEMA_EVOLVED);
    client = getHoodieWriteClient(hoodieEvolvedWriteConfig);
    final List<HoodieRecord> evolvedRecords = generateInsertsWithSchema("004", numRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED);
    // We cannot use insertBatch directly here because we want to insert records
    // with a evolved schema.
    writeBatch(client, "004", "003", Option.empty(), initCommitTime, numRecords,
        (String s, Integer a) -> evolvedRecords, SparkRDDWriteClient::insert, true, numRecords, 2 * numRecords, 4, false);
    // new commit
    HoodieTimeline curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
    assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("004"));
    checkReadRecords("000", 2 * numRecords);

    // Updates with evolved schema is allowed
    final List<HoodieRecord> updateRecords = generateUpdatesWithSchema("005", numUpdateRecords, TRIP_EXAMPLE_SCHEMA_EVOLVED);
    writeBatch(client, "005", "004", Option.empty(), initCommitTime,
        numUpdateRecords, (String s, Integer a) -> updateRecords, SparkRDDWriteClient::upsert, true, numUpdateRecords, 2 * numRecords, 5, false);
    checkReadRecords("000", 2 * numRecords);

    // Now even the original schema cannot be used for updates as it is devolved
    // in relation to the current schema of the dataset.
    client = getHoodieWriteClient(hoodieWriteConfig);
    try {
      updateBatch(hoodieWriteConfig, client, "006", "005", Option.empty(),
                  initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
                  numUpdateRecords, numRecords, 2);
      fail("Update with original scheme should fail");
    } catch (HoodieUpsertException ex) {
      // no new commit
      curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
      assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("005"));
      client.rollback("006");
    }

    // Now even the original schema cannot be used for inserts as it is devolved
    // in relation to the current schema of the dataset.
    try {
      // We are not using insertBatch directly here because insertion of these
      // records will fail and we dont want to keep these records within
      // HoodieTestDataGenerator.
      failedRecords.clear();
      failedRecords.addAll(dataGen.generateInserts("006", numRecords));
      writeBatch(client, "006", "005", Option.empty(), initCommitTime, numRecords,
          (String s, Integer a) -> failedRecords, SparkRDDWriteClient::insert, true, numRecords, numRecords, 1, false);
      fail("Insert with original scheme should fail");
    } catch (HoodieInsertException ex) {
      // no new commit
      curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
      assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("005"));
      client.rollback("006");

      // Remove the inserts from the in-memory state of HoodieTestDataGenerator
      // as these records were never inserted in the dataset. This is required so
      // that future calls to updateBatch or deleteBatch do not generate updates
      // or deletes for records which do not even exist.
      for (HoodieRecord record : failedRecords) {
        assertTrue(dataGen.deleteExistingKeyIfPresent(record.getKey()));
      }
    }

    // Revert to the older commit and ensure that the original schema can now
    // be used for inserts and inserts.
    client.restoreToInstant("003", hoodieWriteConfig.isMetadataTableEnabled());
    curTimeline = metaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants();
    assertTrue(curTimeline.lastInstant().get().getTimestamp().equals("003"));
    checkReadRecords("000", numRecords);

    // Insert with original schema is allowed now
    insertBatch(hoodieWriteConfig, client, "007", "003", numRecords, SparkRDDWriteClient::insert,
        false, true, numRecords, 2 * numRecords, 1, Option.empty());
    checkReadRecords("000", 2 * numRecords);

    // Update with original schema is allowed now
    updateBatch(hoodieWriteConfig, client, "008", "007", Option.empty(),
        initCommitTime, numUpdateRecords, SparkRDDWriteClient::upsert, false, true,
        numUpdateRecords, 2 * numRecords, 5);
    checkReadRecords("000", 2 * numRecords);
  }

  private void checkReadRecords(String instantTime, int numExpectedRecords) throws IOException {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      HoodieTimeline timeline = metaClient.reloadActiveTimeline().getCommitTimeline();
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
    assertTrue(timeline.lastInstant().get().getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));
    assertTrue(timeline.lastInstant().get().getTimestamp().equals(instantTime));
  }

  private List<HoodieRecord> generateInsertsWithSchema(String commitTime, int numRecords, String schemaStr) {
    HoodieTestDataGenerator gen = schemaStr.equals(TRIP_EXAMPLE_SCHEMA_EVOLVED) ? dataGenEvolved : dataGenDevolved;
    List<HoodieRecord> records = gen.generateInserts(commitTime, numRecords);
    return convertToSchema(records, schemaStr);
  }

  private List<HoodieRecord> generateUpdatesWithSchema(String commitTime, int numRecords, String schemaStr) {
    HoodieTestDataGenerator gen = schemaStr.equals(TRIP_EXAMPLE_SCHEMA_EVOLVED) ? dataGenEvolved : dataGenDevolved;
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

  private HoodieWriteConfig getWriteConfig(String schema) {
    return getWriteConfigBuilder(schema).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(String schema) {
    return getConfigBuilder(schema)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.INMEMORY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withAvroSchemaValidate(true);
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
