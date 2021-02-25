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

package org.apache.hudi.hive;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent.PartitionEventType;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncTool {

  private static Stream<Boolean> useJdbc() {
    return Stream.of(false, true);
  }

  private static Iterable<Object[]> useJdbcAndSchemaFromCommitMetadata() {
    return Arrays.asList(new Object[][] {{true, true}, {true, false}, {false, true}, {false, false}});
  }

  @BeforeEach
  public void setUp() throws IOException, InterruptedException {
    HiveTestUtil.setUp();
  }

  @AfterEach
  public void teardown() throws IOException {
    HiveTestUtil.clear();
  }

  @AfterAll
  public static void cleanUpClass() {
    HiveTestUtil.shutdown();
  }

  /**
   * Testing converting array types to Hive field declaration strings.
   * <p>
   * Refer to the Parquet-113 spec: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  @Test
  public void testSchemaConvertArray() throws IOException {
    // Testing the 3-level annotation structure
    MessageType schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .optional(PrimitiveType.PrimitiveTypeName.INT32).named("element").named("list").named("int_list")
        .named("ArrayOfInts");

    String schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list` ARRAY< int>", schemaString);

    // A array of arrays
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup().requiredGroup()
        .as(OriginalType.LIST).repeatedGroup().required(PrimitiveType.PrimitiveTypeName.INT32).named("element")
        .named("list").named("element").named("list").named("int_list_list").named("ArrayOfArrayOfInts");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list_list` ARRAY< ARRAY< int>>", schemaString);

    // A list of integers
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeated(PrimitiveType.PrimitiveTypeName.INT32)
        .named("element").named("int_list").named("ArrayOfInts");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list` ARRAY< int>", schemaString);

    // A list of structs with two fields
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("num").named("element").named("tuple_list").named("ArrayOfTuples");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`tuple_list` ARRAY< STRUCT< `str` : binary, `num` : int>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name is "array", we treat the
    // element type as a one-element struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("array").named("one_tuple_list")
        .named("ArrayOfOneTuples");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name ends with "_tuple", we also treat the
    // element type as a one-element struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("one_tuple_list_tuple")
        .named("one_tuple_list").named("ArrayOfOneTuples2");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

    // A list of structs with a single field
    // Unlike the above two cases, for this the element type is the type of the
    // only field in the struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("one_tuple_list").named("one_tuple_list")
        .named("ArrayOfOneTuples3");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< binary>", schemaString);

    // A list of maps
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup().as(OriginalType.MAP)
        .repeatedGroup().as(OriginalType.MAP_KEY_VALUE).required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.UTF8).named("string_key").required(PrimitiveType.PrimitiveTypeName.INT32).named("int_value")
        .named("key_value").named("array").named("map_list").named("ArrayOfMaps");

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`map_list` ARRAY< MAP< string, int>>", schemaString);
  }

  @Test
  public void testSchemaConvertTimestampMicros() throws IOException {
    MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64)
        .as(OriginalType.TIMESTAMP_MICROS).named("my_element").named("my_timestamp");
    String schemaString = HiveSchemaUtil.generateSchemaString(schema);
    // verify backward compability - int64 converted to bigint type
    assertEquals("`my_element` bigint", schemaString);
    // verify new functionality - int64 converted to timestamp type when 'supportTimestamp' is enabled
    schemaString = HiveSchemaUtil.generateSchemaString(schema, Collections.emptyList(), true);
    assertEquals("`my_element` TIMESTAMP", schemaString);
  }

  @Test
  public void testSchemaDiffForTimestampMicros() {
    MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64)
        .as(OriginalType.TIMESTAMP_MICROS).named("my_element").named("my_timestamp");
    // verify backward compability - int64 converted to bigint type
    SchemaDifference schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        Collections.emptyMap(), Collections.emptyList(), false);
    assertEquals("bigint", schemaDifference.getAddColumnTypes().get("`my_element`"));
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        schemaDifference.getAddColumnTypes(), Collections.emptyList(), false);
    assertTrue(schemaDifference.isEmpty());

    // verify schema difference is calculated correctly when supportTimestamp is enabled
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        Collections.emptyMap(), Collections.emptyList(), true);
    assertEquals("TIMESTAMP", schemaDifference.getAddColumnTypes().get("`my_element`"));
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        schemaDifference.getAddColumnTypes(), Collections.emptyList(), true);
    assertTrue(schemaDifference.isEmpty());
  }

  @ParameterizedTest
  @MethodSource({"useJdbcAndSchemaFromCommitMetadata"})
  public void testBasicSync(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Arrays.asList("2050/01/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, Arrays.asList());
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "New partition should be added");

    // Update partitions
    hiveClient.updatePartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, Arrays.asList());
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Partition count should remain the same");
    hiveClient.updatePartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Partition count should remain the same");

    // Alter partitions
    // Manually change a hive partition location to check if the sync will detect
    // it and generage a partition update event for it.
    hiveClient.updateHiveSQL("ALTER TABLE `" + HiveTestUtil.hiveSyncConfig.tableName
        + "` PARTITION (`datestr`='2050-01-01') SET LOCATION '/some/new/location'");

    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.empty());
    writtenPartitionsSince.add(newPartition.get(0));
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one paritition event");
    assertEquals(PartitionEventType.UPDATE, partitionEvents.iterator().next().eventType,
        "The one partition event must of type UPDATE");

    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    // Sync should update the changed partition to correct path
    List<Partition> tablePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    assertEquals(6, tablePartitions.size(), "The one partition we wrote should be added to hive");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be 100");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncremental(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, true, true, dateTime, commitTime2);

    // Lets do the sync
    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one paritition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncrementalWithSchemaEvolution(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    int fields = hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size();

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertEquals(fields + 3, hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("BIGINT", hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to long");
    assertTrue(hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnRead(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    HoodieHiveClient hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(roTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(roTableName), "Table " + roTableName + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.scanTablePartitions(roTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false,
        useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(roTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadRT(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    assertFalse(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false, useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was sycned should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testMultiPartitionKeySync(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    HiveSyncConfig hiveSyncConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    hiveSyncConfig.partitionValueExtractorClass = MultiPartKeysValueExtractor.class.getCanonicalName();
    hiveSyncConfig.tableName = "multi_part_key";
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(5, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");

    // HoodieHiveClient had a bug where partition vals were sorted
    // and stored as keys in a map. The following tests this particular case.
    // Now lets create partition "2010/01/02" and followed by "2010/02/01".
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartition("2010/01/02", true, true, commitTime2);

    hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(instantTime));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(hiveSyncConfig.tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one paritition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be 101");

    // create partition "2010/02/01" and ensure sync works
    String commitTime3 = "102";
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, commitTime3);
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(7, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");
    assertEquals(1, hiveClient.getPartitionsWrittenToSince(Option.of(commitTime2)).size());
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testNonPartitionedSync(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    HiveSyncConfig hiveSyncConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    // Set partition value extractor to NonPartitionedExtractor
    hiveSyncConfig.partitionValueExtractorClass = NonPartitionedExtractor.class.getCanonicalName();
    hiveSyncConfig.tableName = "non_partitioned";
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size(),
        "Hive Schema should match the table schema，ignoring the partition fields");
    assertEquals(0, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table should not have partitions because of the NonPartitionedExtractor");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testReadSchemaForMOR(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String commitTime = "100";
    String snapshotTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(commitTime, "", 5, false, true);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    assertFalse(hiveClientRT.doesTableExist(snapshotTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClientRT.doesTableExist(snapshotTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should exist after sync completes");

    // Schema being read from compacted base files
    assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClientRT.scanTablePartitions(snapshotTableName).size(), "Table partitions should match the number of partitions we wrote");

    // Now lets create more partitions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    // Schema being read from the log files
    assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the evolved table schema + partition field");
    // Sync should add the one partition
    assertEquals(6, hiveClientRT.scanTablePartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was sycned should be 103");
  }

  @Test
  public void testConnectExceptionIgnoreConfigSet() throws IOException, URISyntaxException {
    HiveTestUtil.hiveSyncConfig.useJdbc = true;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, false);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync

    HiveSyncConfig syncToolConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    syncToolConfig.ignoreExceptions = true;
    syncToolConfig.jdbcUrl = HiveTestUtil.hiveSyncConfig.jdbcUrl.replace("9999","9031");
    HiveSyncTool tool = new HiveSyncTool(syncToolConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
  }

}
