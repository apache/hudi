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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.sync.common.util.ConfigUtils;
import org.apache.hudi.sync.common.HoodieSyncClient.PartitionEvent;
import org.apache.hudi.sync.common.HoodieSyncClient.PartitionEvent.PartitionEventType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.hive.testutils.HiveTestUtil.basePath;
import static org.apache.hudi.hive.testutils.HiveTestUtil.ddlExecutor;
import static org.apache.hudi.hive.testutils.HiveTestUtil.fileSystem;
import static org.apache.hudi.hive.testutils.HiveTestUtil.getHiveConf;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncProps;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncTool {

  private static final List<Object> SYNC_MODES = Arrays.asList(
      "hms",
      "jdbc");

  private static Iterable<Object> syncMode() {
    return SYNC_MODES; // TODO include hiveql; skipped due to CI issue
  }

  // useSchemaFromCommitMetadata, syncMode
  private static Iterable<Object[]> syncModeAndSchemaFromCommitMetadata() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, mode});
      opts.add(new Object[] {false, mode});
    }
    return opts;
  }

  private HiveSyncTool hiveSyncTool;
  private HoodieHiveClient hiveClient;

  @AfterAll
  public static void cleanUpClass() {
    HiveTestUtil.shutdown();
  }

  private static Iterable<Object[]> syncModeAndSchemaFromCommitMetadataAndManagedTable() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, true, mode});
      opts.add(new Object[] {false, false, mode});
    }
    return opts;
  }

  // (useJdbc, useSchemaFromCommitMetadata, syncAsDataSource)
  private static Iterable<Object[]> syncDataSourceTableParams() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, true, mode});
      opts.add(new Object[] {false, false, mode});
    }
    return opts;
  }

  @BeforeEach
  public void setUp() throws Exception {
    HiveTestUtil.setUp();
  }

  @AfterEach
  public void teardown() throws Exception {
    HiveTestUtil.clear();
  }

  @ParameterizedTest
  @MethodSource({"syncModeAndSchemaFromCommitMetadata"})
  public void testBasicSync(boolean useSchemaFromCommitMetadata, String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Arrays.asList("2050/01/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, Arrays.asList());
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "New partition should be added");

    // Update partitions
    hiveClient.updatePartitionsToTable(HiveTestUtil.TABLE_NAME, Arrays.asList());
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Partition count should remain the same");
    hiveClient.updatePartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Partition count should remain the same");

    // Alter partitions
    // Manually change a hive partition location to check if the sync will detect
    // it and generate a partition update event for it.
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` PARTITION (`datestr`='2050-01-01') SET LOCATION '/some/new/location'");

    List<org.apache.hudi.sync.common.model.Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.empty());
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, false);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.UPDATE, partitionEvents.iterator().next().eventType,
        "The one partition event must of type UPDATE");

    // Lets do the sync
    reSyncHiveTable();

    // Sync should update the changed partition to correct path
    List<Partition> tablePartitions = hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(6, tablePartitions.size(), "The one partition we wrote should be added to hive");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 100");
  }

  @ParameterizedTest
  @MethodSource({"syncMode"})
  public void testSyncDataBase(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), HiveTestUtil.DB_NAME);

    // while autoCreateDatabase is false and database not exists;
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_AUTO_CREATE_DATABASE.key(), "false");
    reinitHiveSyncClient();
    // Lets do the sync
    assertThrows(Exception.class, (this::reSyncHiveTable));

    // while autoCreateDatabase is true and database not exists;
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_AUTO_CREATE_DATABASE.key(), "true");
    reinitHiveSyncClient();
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");

    // while autoCreateDatabase is false and database exists;
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_AUTO_CREATE_DATABASE.key(), "false");
    reinitHiveSyncClient();
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");

    // while autoCreateDatabase is true and database exists;
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_AUTO_CREATE_DATABASE.key(), "true");
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");
  }

  @ParameterizedTest
  @MethodSource({"syncDataSourceTableParams"})
  public void testSyncCOWTableWithProperties(boolean useSchemaFromCommitMetadata,
                                             boolean syncAsDataSourceTable,
                                             String syncMode) throws Exception {
    Map<String, String> serdeProperties = new HashMap<String, String>() {
      {
        put("path", HiveTestUtil.basePath);
      }
    };

    Map<String, String> tableProperties = new HashMap<String, String>() {
      {
        put("tp_0", "p0");
        put("tp_1", "p1");
      }
    };
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), String.valueOf(syncAsDataSourceTable));
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_TABLE_SERDE_PROPERTIES.key(), ConfigUtils.configToString(serdeProperties));
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_TABLE_PROPERTIES.key(), ConfigUtils.configToString(tableProperties));

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reinitHiveSyncClient();
    reSyncHiveTable();

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());
    String dbTableName = HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME;
    hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);
    List<String> results = new ArrayList<>();
    hiveDriver.getResults(results);

    String tblPropertiesWithoutDdlTime = String.join("\n",
        results.subList(0, results.size() - 1));

    String sparkTableProperties = getSparkTableProperties(syncAsDataSourceTable, useSchemaFromCommitMetadata);
    assertEquals(
        "EXTERNAL\tTRUE\n"
            + "last_commit_time_sync\t100\n"
            + sparkTableProperties
            + "tp_0\tp0\n"
            + "tp_1\tp1", tblPropertiesWithoutDdlTime);
    assertTrue(results.get(results.size() - 1).startsWith("transient_lastDdlTime"));

    results.clear();
    // validate serde properties
    hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
    hiveDriver.getResults(results);
    String ddl = String.join("\n", results);
    assertTrue(ddl.contains("'path'='" + HiveTestUtil.basePath + "'"));
    if (syncAsDataSourceTable) {
      assertTrue(ddl.contains("'" + ConfigUtils.IS_QUERY_AS_RO_TABLE + "'='false'"));
    }
  }

  private String getSparkTableProperties(boolean syncAsDataSourceTable, boolean useSchemaFromCommitMetadata) {
    if (syncAsDataSourceTable) {
      if (useSchemaFromCommitMetadata) {
        return "spark.sql.sources.provider\thudi\n"
            + "spark.sql.sources.schema.numPartCols\t1\n"
            + "spark.sql.sources.schema.numParts\t1\n"
            + "spark.sql.sources.schema.part.0\t{\"type\":\"struct\",\"fields\":"
            + "[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},"
            + "{\"name\":\"favorite_number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},"
            + "{\"name\":\"favorite_color\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},"
            + "{\"name\":\"datestr\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}\n"
            + "spark.sql.sources.schema.partCol.0\tdatestr\n";
      } else {
        return "spark.sql.sources.provider\thudi\n"
            + "spark.sql.sources.schema.numPartCols\t1\n"
            + "spark.sql.sources.schema.numParts\t1\n"
            + "spark.sql.sources.schema.part.0\t{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":"
            + "\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"favorite_number\",\"type\":\"integer\","
            + "\"nullable\":false,\"metadata\":{}},{\"name\":\"favorite_color\",\"type\":\"string\",\"nullable\":false,"
            + "\"metadata\":{}}]}\n"
            + "{\"name\":\"datestr\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}\n"
            + "spark.sql.sources.schema.partCol.0\tdatestr\n";
      }
    } else {
      return "";
    }
  }

  @ParameterizedTest
  @MethodSource({"syncDataSourceTableParams"})
  public void testSyncMORTableWithProperties(boolean useSchemaFromCommitMetadata,
                                             boolean syncAsDataSourceTable,
                                             String syncMode) throws Exception {
    Map<String, String> serdeProperties = new HashMap<String, String>() {
      {
        put("path", HiveTestUtil.basePath);
      }
    };

    Map<String, String> tableProperties = new HashMap<String, String>() {
      {
        put("tp_0", "p0");
        put("tp_1", "p1");
      }
    };
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), String.valueOf(syncAsDataSourceTable));
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_TABLE_SERDE_PROPERTIES.key(), ConfigUtils.configToString(serdeProperties));
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_TABLE_PROPERTIES.key(), ConfigUtils.configToString(tableProperties));

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    reinitHiveSyncClient();
    reSyncHiveTable();

    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    String rtTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;

    String[] tableNames = new String[] {roTableName, rtTableName};
    String[] readAsOptimizedResults = new String[] {"true", "false"};

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());

    String sparkTableProperties = getSparkTableProperties(syncAsDataSourceTable, useSchemaFromCommitMetadata);
    for (int i = 0; i < 2; i++) {
      String dbTableName = HiveTestUtil.DB_NAME + "." + tableNames[i];
      String readAsOptimized = readAsOptimizedResults[i];

      hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);
      List<String> results = new ArrayList<>();
      hiveDriver.getResults(results);

      String tblPropertiesWithoutDdlTime = String.join("\n",
          results.subList(0, results.size() - 1));
      assertEquals(
          "EXTERNAL\tTRUE\n"
              + "last_commit_time_sync\t101\n"
              + sparkTableProperties
              + "tp_0\tp0\n"
              + "tp_1\tp1", tblPropertiesWithoutDdlTime);
      assertTrue(results.get(results.size() - 1).startsWith("transient_lastDdlTime"));

      results.clear();
      // validate serde properties
      hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
      hiveDriver.getResults(results);
      String ddl = String.join("\n", results);
      assertTrue(ddl.contains("'path'='" + HiveTestUtil.basePath + "'"));
      assertTrue(ddl.toLowerCase().contains("create external table"));
      if (syncAsDataSourceTable) {
        assertTrue(ddl.contains("'" + ConfigUtils.IS_QUERY_AS_RO_TABLE + "'='" + readAsOptimized + "'"));
      }
    }
  }

  @ParameterizedTest
  @MethodSource({"syncModeAndSchemaFromCommitMetadataAndManagedTable"})
  public void testSyncManagedTable(boolean useSchemaFromCommitMetadata,
                                   boolean isManagedTable,
                                   String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_CREATE_MANAGED_TABLE.key(), String.valueOf(isManagedTable));

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reinitHiveSyncClient();
    reSyncHiveTable();

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());
    String dbTableName = HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME;
    hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);

    List<String> results = new ArrayList<>();
    hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
    hiveDriver.getResults(results);
    String ddl = String.join("\n", results).toLowerCase();
    if (isManagedTable) {
      assertTrue(ddl.contains("create table"));
    } else {
      assertTrue(ddl.contains("create external table"));
    }
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncWithSchema(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/complex.schema.avsc");

    reinitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(1, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncIncremental(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reinitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, true, true, dateTime, commitTime2);

    // Lets do the sync
    reSyncHiveTable();
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<org.apache.hudi.sync.common.model.Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, false);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    // Sync should add the one partition
    reSyncHiveTable();
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncIncrementalWithSchemaEvolution(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reinitHiveSyncClient();
    reSyncHiveTable();

    int fields = hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size();

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    reinitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(fields + 3, hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("BIGINT", hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to long");
    assertTrue(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testUpdateTableComments(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/simple-test.avsc");
    reinitHiveSyncClient();
    reSyncHiveTable();

    Map<String, ImmutablePair<String,String>> alterCommentSchema = new HashMap<>();
    //generate commented schema field
    Schema schema = SchemaTestUtil.getSchemaFromResource(HiveTestUtil.class, "/simple-test.avsc");
    Schema commentedSchema = SchemaTestUtil.getSchemaFromResource(HiveTestUtil.class, "/simple-test-doced.avsc");
    Map<String, String> fieldsNameAndDoc = commentedSchema.getFields().stream().collect(Collectors.toMap(field -> field.name().toLowerCase(Locale.ROOT),
        field -> StringUtils.isNullOrEmpty(field.doc()) ? "" : field.doc()));
    for (Field field : schema.getFields()) {
      String name = field.name().toLowerCase(Locale.ROOT);
      String comment = fieldsNameAndDoc.get(name);
      if (fieldsNameAndDoc.containsKey(name) && !comment.equals(field.doc())) {
        alterCommentSchema.put(name, new ImmutablePair<>(field.schema().getType().name(),comment));
      }
    }

    ddlExecutor.updateTableComments(HiveTestUtil.TABLE_NAME, alterCommentSchema);

    List<FieldSchema> fieldSchemas = hiveClient.getTableCommentUsingMetastoreClient(HiveTestUtil.TABLE_NAME);
    int commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (!StringUtils.isNullOrEmpty(fieldSchema.getComment())) {
        commentCnt++;
      }
    }
    assertEquals(2, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncWithCommentedSchema(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_COMMENT.key(), "false");
    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/simple-test-doced.avsc");

    reinitHiveSyncClient();
    reSyncHiveTable();
    List<FieldSchema> fieldSchemas = hiveClient.getTableCommentUsingMetastoreClient(HiveTestUtil.TABLE_NAME);
    int commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (!StringUtils.isNullOrEmpty(fieldSchema.getComment())) {
        commentCnt++;
      }
    }
    assertEquals(0, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");

    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_COMMENT.key(), "true");
    reinitHiveSyncClient();
    reSyncHiveTable();
    fieldSchemas = hiveClient.getTableCommentUsingMetastoreClient(HiveTestUtil.TABLE_NAME);
    commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (!StringUtils.isNullOrEmpty(fieldSchema.getComment())) {
        commentCnt++;
      }
    }
    assertEquals(2, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndSchemaFromCommitMetadata")
  public void testSyncMergeOnRead(boolean useSchemaFromCommitMetadata, String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(roTableName), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(roTableName), "Table " + roTableName + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.scanTablePartitions(roTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false,
        useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reinitHiveSyncClient();
    reSyncHiveTable();

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(roTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadRT(boolean useSchemaFromCommitMetadata, String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, useSchemaFromCommitMetadata);
    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.scanTablePartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false, useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reinitHiveSyncClient();
    reSyncHiveTable();

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(snapshotTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testMultiPartitionKeySync(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), MultiPartKeysValueExtractor.class.getCanonicalName());
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "year,month,day");

    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // HoodieHiveClient had a bug where partition vals were sorted
    // and stored as keys in a map. The following tests this particular case.
    // Now lets create partition "2010/01/02" and followed by "2010/02/01".
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartition("2010/01/02", true, true, commitTime2);

    reinitHiveSyncClient();
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(instantTime));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<org.apache.hudi.sync.common.model.Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, false);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    reSyncHiveTable();
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");

    // create partition "2010/02/01" and ensure sync works
    String commitTime3 = "102";
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, commitTime3);
    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reinitHiveSyncClient();
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(7, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    assertEquals(1, hiveClient.getPartitionsWrittenToSince(Option.of(commitTime2)).size());
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testDropPartitionKeySync(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 1, true);

    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(1, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Arrays.asList("2050/01/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, Arrays.asList());
    assertEquals(1, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    assertEquals(2, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "New partition should be added");

    reSyncHiveTable();

    // Drop 1 partition.
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` DROP PARTITION (`datestr`='2050-01-01')");

    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, hivePartitions.size(),
        "Table should have 1 partition because of the drop 1 partition");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testDropPartition(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 1, true);

    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    List<Partition> partitions = hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitions.size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    String partitiontoDelete = partitions.get(0).getValues().get(0).replace("-","/");
    // create a replace commit to delete current partitions+
    HiveTestUtil.createReplaceCommit("101", partitiontoDelete, WriteOperationType.DELETE_PARTITION, true, true);

    // sync drop partitions
    reinitHiveSyncClient();
    reSyncHiveTable();

    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(0, hivePartitions.size(),
        "Table should have 0 partition because of the drop the only one partition");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testNonPartitionedSync(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);
    // Set partition value extractor to NonPartitionedExtractor
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), NonPartitionedExtractor.class.getCanonicalName());
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "year, month, day");

    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size(),
        "Hive Schema should match the table schema，ignoring the partition fields");
    assertEquals(0, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table should not have partitions because of the NonPartitionedExtractor");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testReadSchemaForMOR(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    String commitTime = "100";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(commitTime, "", 5, false, true);
    reinitHiveSyncClient();

    assertFalse(hiveClient.tableExists(snapshotTableName), "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should not exist initially");

    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName), "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should exist after sync completes");

    // Schema being read from compacted base files
    assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(snapshotTableName).size(), "Table partitions should match the number of partitions we wrote");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reinitHiveSyncClient();
    reSyncHiveTable();

    // Schema being read from the log filesTestHiveSyncTool
    assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the evolved table schema + partition field");
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @Test
  public void testConnectExceptionIgnoreConfigSet() throws IOException, URISyntaxException, HiveException, MetaException {
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, false);
    reinitHiveSyncClient();
    HoodieHiveClient prevHiveClient = hiveClient;
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    // Lets do the sync
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_IGNORE_EXCEPTIONS.key(), "true");
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_URL.key(), hiveSyncProps.getString(HiveSyncConfig.HIVE_URL.key())
        .replace(String.valueOf(HiveTestUtil.hiveTestService.getHiveServerPort()), String.valueOf(NetworkTestUtils.nextFreePort())));
    reinitHiveSyncClient();
    reSyncHiveTable();

    assertNull(hiveClient);
    assertFalse(prevHiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
  }

  private void verifyOldParquetFileTest(HoodieHiveClient hiveClient, String emptyCommitTime) throws Exception {
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(1, hiveClient.scanTablePartitions(HiveTestUtil.TABLE_NAME).size(), "Table partitions should match the number of partitions we wrote");
    assertEquals(emptyCommitTime,
        hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(), "The last commit that was synced should be updated in the TBLPROPERTIES");

    // make sure correct schema is picked
    Schema schema = SchemaTestUtil.getSimpleSchema();
    for (Field field : schema.getFields()) {
      assertEquals(field.schema().getType().getName(),
          hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).get(field.name()).toLowerCase(),
          String.format("Hive Schema Field %s was added", field));
    }
    assertEquals("string",
        hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).get("datestr").toLowerCase(), "Hive Schema Field datestr was added");
    assertEquals(schema.getFields().size() + 1 + HoodieRecord.HOODIE_META_COLUMNS.size(),
        hiveClient.getTableSchema(HiveTestUtil.TABLE_NAME).size(), "Hive Schema fields size");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testPickingOlderParquetFileIfLatestIsEmptyCommit(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    reinitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    reinitHiveSyncClient();
    reSyncHiveTable();

    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testNotPickingOlderParquetFileWhenLatestCommitReadFails(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    // evolve the schema
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFile(commitMetadata, emptyCommitTime, basePath);

    reinitHiveSyncClient();
    assertFalse(
        hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    HiveSyncTool tool = new HiveSyncTool(new HiveSyncConfig(hiveSyncProps, getHiveConf()));
    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + hiveClient.getActiveTimeline().getInstants()
        .filter(inst -> inst.getTimestamp().equals(commitTime2))
        .findFirst().get().getFileName());
    assertTrue(HiveTestUtil.fileSystem.delete(fullPath, false));

    try {
      tool.syncHoodieTable();
    } catch (RuntimeException e) {
      // we expect the table sync to fail
    }

    // table should not be synced yet
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist at all");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testNotPickingOlderParquetFileWhenLatestCommitReadFailsForExistingTable(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    //HiveTestUtil.createCommitFile(commitMetadata, emptyCommitTime);
    reinitHiveSyncClient();
    assertFalse(
        hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    reSyncHiveTable();

    verifyOldParquetFileTest(hiveClient, emptyCommitTime);

    // evolve the schema
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "301";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);
    //HiveTestUtil.createCommitFileWithSchema(commitMetadata, "400", false); // create another empty commit
    //HiveTestUtil.createCommitFile(commitMetadata, "400"); // create another empty commit

    reinitHiveSyncClient();
    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + hiveClient.getActiveTimeline().getInstants()
        .filter(inst -> inst.getTimestamp().equals(commitTime2))
        .findFirst().get().getFileName());
    assertTrue(HiveTestUtil.fileSystem.delete(fullPath, false));
    try {
      reSyncHiveTable();
    } catch (RuntimeException e) {
      // we expect the table sync to fail
    } finally {
      reinitHiveSyncClient();
    }

    // old sync values should be left intact
    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testTypeConverter(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    HiveTestUtil.createCOWTable("100", 5, true);
    // create database.
    ddlExecutor.runSQL("create database " + HiveTestUtil.DB_NAME);
    reinitHiveSyncClient();
    String tableName = HiveTestUtil.TABLE_NAME;
    String tableAbsoluteName = String.format(" `%s.%s` ", HiveTestUtil.DB_NAME, tableName);
    String dropTableSql = String.format("DROP TABLE IF EXISTS %s ", tableAbsoluteName);
    String createTableSqlPrefix = String.format("CREATE TABLE IF NOT EXISTS %s ", tableAbsoluteName);
    String errorMsg = "An error occurred in decimal type converting.";
    ddlExecutor.runSQL(dropTableSql);

    // test one column in DECIMAL
    String oneTargetColumnSql = createTableSqlPrefix + "(`decimal_col` DECIMAL(9,8), `bigint_col` BIGINT)";
    ddlExecutor.runSQL(oneTargetColumnSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);

    // test multiple columns in DECIMAL
    String multipleTargetColumnSql =
        createTableSqlPrefix + "(`decimal_col1` DECIMAL(9,8), `bigint_col` BIGINT, `decimal_col2` DECIMAL(7,4))";
    ddlExecutor.runSQL(multipleTargetColumnSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)")
        && hiveClient.getTableSchema(tableName).containsValue("DECIMAL(7,4)"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);

    // test no columns in DECIMAL
    String noTargetColumnsSql = createTableSqlPrefix + "(`bigint_col` BIGINT)";
    ddlExecutor.runSQL(noTargetColumnsSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).size() == 1 && hiveClient.getTableSchema(tableName)
        .containsValue("BIGINT"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncWithoutDiffs(String syncMode) throws Exception {
    String tableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_CONDITIONAL_SYNC.key(), "true");

    String commitTime0 = "100";
    String commitTime1 = "101";
    String commitTime2 = "102";
    HiveTestUtil.createMORTable(commitTime0, commitTime1, 2, true, true);

    reinitHiveSyncClient();
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(tableName));
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get());

    HiveTestUtil.addMORPartitions(0, true, true, true, ZonedDateTime.now().plusDays(2), commitTime1, commitTime2);

    reSyncHiveTable();
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get());
  }

  private void reSyncHiveTable() {
    hiveSyncTool.syncHoodieTable();
    // we need renew the hiveclient after tool.syncHoodieTable(), because it will close hive
    // session, then lead to connection retry, we can see there is a exception at log.
    reinitHiveSyncClient();
  }

  private void reinitHiveSyncClient() {
    hiveSyncTool = new HiveSyncTool(new HiveSyncConfig(hiveSyncProps, HiveTestUtil.getHiveConf()));
    hiveClient = (HoodieHiveClient) hiveSyncTool.hoodieHiveClient;
  }

  private int getPartitionFieldSize() {
    return hiveSyncProps.getString(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key()).split(",").length;
  }
}
