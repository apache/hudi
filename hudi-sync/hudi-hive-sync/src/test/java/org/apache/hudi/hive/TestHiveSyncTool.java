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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSyncTableStrategy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.ddl.HMSDDLExecutor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.hive.util.IMetaStoreClientUtil;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionEvent.PartitionEventType;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.TIMELINEFOLDER_NAME;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getRelativePartitionPath;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_ENABLED;
import static org.apache.hudi.hive.HiveSyncConfig.RECREATE_HIVE_TABLE_ON_ERROR;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_COMMENT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_TABLE_STRATEGY;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.testutils.HiveTestUtil.basePath;
import static org.apache.hudi.hive.testutils.HiveTestUtil.ddlExecutor;
import static org.apache.hudi.hive.testutils.HiveTestUtil.getHiveConf;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncConfig;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncProps;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_INCREMENTAL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncTool {

  private static final List<Object> SYNC_MODES = Arrays.asList(
      "hiveql",
      "hms",
      "jdbc");

  private static Iterable<Object> syncMode() {
    return SYNC_MODES;
  }

  // syncMode, enablePushDown
  private static Iterable<Object[]> syncModeAndEnablePushDown() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {mode, "true"});
      opts.add(new Object[] {mode, "false"});
    }
    return opts;
  }

  // useSchemaFromCommitMetadata, syncMode, enablePushDown
  private static Iterable<Object[]> syncModeAndSchemaFromCommitMetadata() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, mode, "true"});
      opts.add(new Object[] {false, mode, "true"});
      opts.add(new Object[] {true, mode, "false"});
      opts.add(new Object[] {false, mode, "false"});
    }
    return opts;
  }

  private static Iterable<Object[]> syncModeAndStrategy() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {mode, HoodieSyncTableStrategy.ALL});
      opts.add(new Object[] {mode, HoodieSyncTableStrategy.RO});
      opts.add(new Object[] {mode, HoodieSyncTableStrategy.RT});
    }
    return opts;
  }

  private HiveSyncTool hiveSyncTool;
  private HoodieHiveSyncClient hiveClient;

  @AfterAll
  public static void cleanUpClass() throws IOException {
    HiveTestUtil.shutdown();
  }

  // (useSchemaFromCommitMetadata, isManagedTable, syncMode, enablePushDown)
  private static Iterable<Object[]> syncModeAndSchemaFromCommitMetadataAndManagedTable() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, true, mode, "true"});
      opts.add(new Object[] {false, false, mode, "true"});
      opts.add(new Object[] {true, true, mode, "false"});
      opts.add(new Object[] {false, false, mode, "false"});
    }
    return opts;
  }

  // (useJdbc, useSchemaFromCommitMetadata, syncAsDataSource, enablePushDown)
  private static Iterable<Object[]> syncDataSourceTableParams() {
    List<Object[]> opts = new ArrayList<>();
    for (Object mode : SYNC_MODES) {
      opts.add(new Object[] {true, true, mode, "true"});
      opts.add(new Object[] {false, false, mode, "true"});
      opts.add(new Object[] {true, true, mode, "false"});
      opts.add(new Object[] {false, false, mode, "false"});
    }
    return opts;
  }

  @BeforeEach
  public void setUp() throws Exception {
    HiveTestUtil.setUp(Option.empty(), true);
  }

  @AfterEach
  public void teardown() throws Exception {
    HiveTestUtil.clear();
  }

  @ParameterizedTest
  @MethodSource({"syncModeAndSchemaFromCommitMetadata"})
  public void testUpdateBasePath(boolean useSchemaFromCommitMetadata, String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);
    String instantTime = "100";
    // create a cow table and sync to hive
    HiveTestUtil.createCOWTable(instantTime, 1, useSchemaFromCommitMetadata);
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    IMetaStoreClient client = IMetaStoreClientUtil.getMSC(getHiveConf());
    Option<String> locationOption = getMetastoreLocation(client, HiveTestUtil.DB_NAME, HiveTestUtil.TABLE_NAME);
    assertTrue(locationOption.isPresent(),
        "The location of Table " + HiveTestUtil.TABLE_NAME + " is not present in metastore");
    String oldLocation = locationOption.get();

    // we change the base path, so we need to delete temp directory manually
    HiveTestUtil.fileSystem.delete(new Path(basePath), true);

    // create a new cow table and reSync
    basePath = Files.createTempDirectory("hivesynctest" + Instant.now().toEpochMilli()).toUri().toString();
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    HiveTestUtil.createCOWTable(instantTime, 1, useSchemaFromCommitMetadata);
    reInitHiveSyncClient();
    reSyncHiveTable();
    client.reconnect();
    Option<String> newLocationOption = getMetastoreLocation(client, HiveTestUtil.DB_NAME, HiveTestUtil.TABLE_NAME);
    assertTrue(newLocationOption.isPresent(),
        "The location of Table " + HiveTestUtil.TABLE_NAME + " is not present in metastore");
    String newLocation = newLocationOption.get();
    assertNotEquals(oldLocation, newLocation, "Update base path failed");
    client.close();
  }

  public Option<String> getMetastoreLocation(IMetaStoreClient client, String databaseName, String tableName) {
    try {
      Table table = client.getTable(databaseName, tableName);
      StorageDescriptor sd = table.getSd();
      return Option.ofNullable(sd.getLocation());
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the metastore location from the table " + tableName, e);
    }
  }
  
  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncAllPartition() throws Exception {
    // Create and write some partition
    HiveTestUtil.createCOWTable("100", 1, true);
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, "101");
    HiveTestUtil.addCOWPartition("2010/02/02", true, true, "102");
    HiveTestUtil.addCOWPartition("2010/02/03", true, true, "103");
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(4, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    // Drop one partition metadata
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` DROP PARTITION (`datestr`='2010-02-03')");
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(3, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    // Use META_SYNC_PARTITION_FIXMODE, sync all partition metadata
    hiveSyncProps.setProperty(META_SYNC_INCREMENTAL.key(), "false");
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(4, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testDropUpperCasePartitionWithHMS() throws Exception {
    hiveSyncConfig.setValue(META_SYNC_PARTITION_FIELDS.key(), "DATESTR");
    // Create and write some partition
    HiveTestUtil.createCOWTable("100", 1, true);
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, "101");
    HiveTestUtil.addCOWPartition("2010/02/02", true, true, "102");
    HiveTestUtil.addCOWPartition("2010/02/03", true, true, "103");
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(4, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    // Drop partition with HMSDDLExecutor
    try (HMSDDLExecutor hmsExecutor =
            new HMSDDLExecutor(hiveSyncConfig, IMetaStoreClientUtil.getMSC(hiveSyncConfig.getHiveConf()))) {
      hmsExecutor.dropPartitionsToTable(HiveTestUtil.TABLE_NAME, Collections.singletonList("2010/02/03"));
    }

    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(3, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");

    // Drop partition with QueryBasedDDLExecutor
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` DROP PARTITION (`datestr`='2010-02-02')");
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(2, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
  }

  @ParameterizedTest
  @MethodSource({"syncModeAndSchemaFromCommitMetadata"})
  public void testBasicSync(boolean useSchemaFromCommitMetadata, String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Arrays.asList("2050/01/01", "2040/02/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, Collections.emptyList());
    assertEquals(5, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    FileCreateUtilsLegacy.createPartitionMetaFile(basePath, "2050/01/01");
    FileCreateUtilsLegacy.createPartitionMetaFile(basePath, "2040/02/01");
    assertEquals(7, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "New partition should be added");

    // Update partitions
    hiveClient.updatePartitionsToTable(HiveTestUtil.TABLE_NAME, Collections.emptyList());
    assertEquals(7, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Partition count should remain the same");
    hiveClient.updatePartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    List<Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    Set<String> relativePartitionPaths = hivePartitions.stream()
        .map(p -> getRelativePartitionPath(new Path(basePath), new Path(p.getStorageLocation())))
        .collect(Collectors.toSet());
    // partition paths from the storage descriptor should be unique and contain the updated partitions
    assertEquals(7, hivePartitions.size(), "Partition count should remain the same");
    assertEquals(hivePartitions.size(), relativePartitionPaths.size());
    assertTrue(relativePartitionPaths.containsAll(newPartition));

    // Alter partitions
    // Manually change a hive partition location to check if the sync will detect
    // it and generate a partition update event for it.
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` PARTITION (`datestr`='2050-01-01') SET LOCATION '"
        + FSUtils.constructAbsolutePath(basePath, "2050/1/1").toString() + "'");

    hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<String> writtenPartitionsSince = hiveClient.getWrittenPartitionsSince(Option.empty(), Option.empty());
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, Collections.emptySet());
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.UPDATE, partitionEvents.iterator().next().eventType,
        "The one partition event must of type UPDATE");

    // Add a partition that does not belong to the table, i.e., not in the same base path
    // This should not happen in production.  However, if this happens, when doing fallback
    // to list all partitions in the metastore and we find such a partition, we simply ignore
    // it without dropping it from the metastore and notify the user with an error message,
    // so the user may manually fix it.

    String dummyBasePath = new Path(basePath).getParent().toString() + "/dummy_basepath";
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` ADD PARTITION (`datestr`='xyz') LOCATION '" + dummyBasePath + "/xyz'");

    // Lets do the sync
    reSyncHiveTable();

    // Sync should update the changed partition to correct path
    List<Partition> tablePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(8, tablePartitions.size(), "The two partitions we wrote should be added to hive");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 100");

    // Verify that there is one ADD, UPDATE, and DROP event for each type
    hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<String> allPartitionPathsOnStorage = hiveClient.getAllPartitionPathsOnStorage()
        .stream().sorted().collect(Collectors.toList());
    String dropPartition = allPartitionPathsOnStorage.remove(0);
    allPartitionPathsOnStorage.add("2050/01/02");
    partitionEvents = hiveClient.getPartitionEvents(hivePartitions, allPartitionPathsOnStorage);
    assertEquals(3, partitionEvents.size(), "There should be only one partition event");
    assertEquals(
        "2050/01/02",
        partitionEvents.stream().filter(e -> e.eventType == PartitionEventType.ADD)
            .findFirst().get().storagePartition,
        "There should be only one partition event of type ADD");
    assertEquals(
        "2050/01/01",
        partitionEvents.stream().filter(e -> e.eventType == PartitionEventType.UPDATE)
            .findFirst().get().storagePartition,
        "There should be only one partition event of type UPDATE");
    assertEquals(
        dropPartition,
        partitionEvents.stream().filter(e -> e.eventType == PartitionEventType.DROP)
            .findFirst().get().storagePartition,
        "There should be only one partition event of type DROP");

    // Simulate the case where the last sync timestamp is before the start of the active timeline,
    // by overwriting the same table with some partitions deleted and new partitions added
    HiveTestUtil.createCOWTable("200", 6, useSchemaFromCommitMetadata);
    reInitHiveSyncClient();
    reSyncHiveTable();
    tablePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(Option.of("200"), hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME));
    assertEquals(7, tablePartitions.size());

    // Trigger the fallback of listing all partitions again.  There is no partition change.
    HiveTestUtil.commitToTable("300", 1, useSchemaFromCommitMetadata);
    HiveTestUtil.removeCommitFromActiveTimeline("200", COMMIT_ACTION);
    reInitHiveSyncClient();
    reSyncHiveTable();
    tablePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(Option.of("300"), hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME));
    assertEquals(7, tablePartitions.size());

    // Add the following instants to the active timeline and sync: "400" (rollback), "500" (commit)
    // Last commit time sync is "500" after Hive sync
    HiveTestUtil.addRollbackInstantToTable("400", "350");
    HiveTestUtil.commitToTable("500", 7, useSchemaFromCommitMetadata);
    reInitHiveSyncClient();
    reSyncHiveTable();
    tablePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(Option.of("500"), hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME));
    assertEquals(8, tablePartitions.size());

    // Add more instants with adding a partition and simulate the case where the commit adding
    // the new partition is archived.
    // Before simulated archival: "300", "400" (rollback), "500", "600" (adding new partition), "700", "800"
    // After simulated archival: "400" (rollback), "700", "800"
    // In this case, listing all partitions should be triggered to catch up.
    HiveTestUtil.commitToTable("600", 8, useSchemaFromCommitMetadata);
    HiveTestUtil.commitToTable("700", 1, useSchemaFromCommitMetadata);
    HiveTestUtil.commitToTable("800", 1, useSchemaFromCommitMetadata);
    HiveTestUtil.removeCommitFromActiveTimeline("300", COMMIT_ACTION);
    HiveTestUtil.removeCommitFromActiveTimeline("500", COMMIT_ACTION);
    HiveTestUtil.removeCommitFromActiveTimeline("600", COMMIT_ACTION);
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(hiveClient.config.getHadoopConf()), basePath);
    assertEquals(
        Arrays.asList("400", "700", "800"),
        metaClient.getActiveTimeline().getInstants().stream()
            .map(HoodieInstant::requestedTime).sorted()
            .collect(Collectors.toList()));

    reInitHiveSyncClient();
    reSyncHiveTable();
    tablePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(Option.of("800"), hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME));
    assertEquals(9, tablePartitions.size());
  }

  @ParameterizedTest
  @MethodSource({"syncMode"})
  public void testSyncDataBase(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);
    hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), HiveTestUtil.DB_NAME);

    // while autoCreateDatabase is false and database not exists;
    hiveSyncProps.setProperty(HIVE_AUTO_CREATE_DATABASE.key(), "false");
    reInitHiveSyncClient();
    // Lets do the sync
    assertThrows(Exception.class, (this::reSyncHiveTable));

    // while autoCreateDatabase is true and database not exists;
    hiveSyncProps.setProperty(HIVE_AUTO_CREATE_DATABASE.key(), "true");
    reInitHiveSyncClient();
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");

    // while autoCreateDatabase is false and database exists;
    hiveSyncProps.setProperty(HIVE_AUTO_CREATE_DATABASE.key(), "false");
    reInitHiveSyncClient();
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");

    // while autoCreateDatabase is true and database exists;
    hiveSyncProps.setProperty(HIVE_AUTO_CREATE_DATABASE.key(), "true");
    assertDoesNotThrow((this::reSyncHiveTable));
    assertTrue(hiveClient.databaseExists(HiveTestUtil.DB_NAME),
        "DataBases " + HiveTestUtil.DB_NAME + " should exist after sync completes");
  }

  @ParameterizedTest
  @MethodSource({"syncDataSourceTableParams"})
  public void testSyncCOWTableWithProperties(boolean useSchemaFromCommitMetadata,
                                             boolean syncAsDataSourceTable,
                                             String syncMode,
                                             String enablePushDown) throws Exception {
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
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), String.valueOf(syncAsDataSourceTable));
    hiveSyncProps.setProperty(HIVE_TABLE_SERDE_PROPERTIES.key(), ConfigUtils.configToString(serdeProperties));
    hiveSyncProps.setProperty(HIVE_TABLE_PROPERTIES.key(), ConfigUtils.configToString(tableProperties));
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reInitHiveSyncClient();
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
            + "last_commit_completion_time_sync\t" + getLastCommitCompletionTimeSynced() + "\n"
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
    assertTrue(ddl.contains(String.format("ROW FORMAT SERDE \n  '%s'", ParquetHiveSerDe.class.getName())));
    assertTrue(ddl.contains("'path'='" + HiveTestUtil.basePath + "'"));
    if (syncAsDataSourceTable) {
      assertTrue(ddl.contains("'" + ConfigUtils.IS_QUERY_AS_RO_TABLE + "'='false'"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSyncCOWTableWithCreateManagedTable(boolean createManagedTable) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), HiveSyncMode.HMS.name());
    hiveSyncProps.setProperty(HIVE_CREATE_MANAGED_TABLE.key(), Boolean.toString(createManagedTable));

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    reInitHiveSyncClient();
    reSyncHiveTable();

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());
    hiveDriver.run(String.format("SHOW TBLPROPERTIES %s.%s", HiveTestUtil.DB_NAME, HiveTestUtil.TABLE_NAME));
    List<String> results = new ArrayList<>();
    hiveDriver.getResults(results);

    assertEquals(
        String.format("%slast_commit_completion_time_sync\t%s\nlast_commit_time_sync\t%s\n%s",
            createManagedTable ? StringUtils.EMPTY_STRING : "EXTERNAL\tTRUE\n",
            getLastCommitCompletionTimeSynced(),
            instantTime,
            getSparkTableProperties(true, true)),
        String.format("%s\n", String.join("\n", results.subList(0, results.size() - 1))));
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
                                             String syncMode,
                                             String enablePushDown) throws Exception {
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
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), String.valueOf(syncAsDataSourceTable));
    hiveSyncProps.setProperty(HIVE_TABLE_SERDE_PROPERTIES.key(), ConfigUtils.configToString(serdeProperties));
    hiveSyncProps.setProperty(HIVE_TABLE_PROPERTIES.key(), ConfigUtils.configToString(tableProperties));
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    reInitHiveSyncClient();
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
              + "last_commit_completion_time_sync\t" + getLastCommitCompletionTimeSynced() + "\n"
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
      assertTrue(ddl.contains(String.format("ROW FORMAT SERDE \n  '%s'", ParquetHiveSerDe.class.getName())));
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
                                   String syncMode,
                                   String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_CREATE_MANAGED_TABLE.key(), String.valueOf(isManagedTable));
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    reInitHiveSyncClient();
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
  @MethodSource("syncModeAndEnablePushDown")
  public void testSyncWithSchema(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/complex.schema.avsc");

    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(1, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testSyncIncremental(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(5, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, true, true, dateTime, commitTime2);

    // Lets do the sync
    reSyncHiveTable();
    List<String> writtenPartitionsSince = hiveClient.getWrittenPartitionsSince(Option.of(commitTime1), Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<org.apache.hudi.sync.common.model.Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, Collections.emptySet());
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    // Sync should add the one partition
    reSyncHiveTable();
    assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testSyncIncrementalWithSchemaEvolution(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reInitHiveSyncClient();
    reSyncHiveTable();

    int fields = hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size();

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(fields + 3, hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("BIGINT", hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to long");
    assertTrue(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testRecreateCOWTableOnBasePathChange(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reInitHiveSyncClient();
    reSyncHiveTable();

    String commitTime2 = "105";
    // let's update the basepath
    basePath = Files.createTempDirectory("hivesynctest_new" + Instant.now().toEpochMilli()).toUri().toString();
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), basePath);

    // let's create new table in new basepath
    HiveTestUtil.createCOWTable(commitTime2, 2, true);
    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime3 = "110";
    // let's add 2 more partitions to the new basepath
    HiveTestUtil.addCOWPartitions(2, false, true, dateTime, commitTime3);

    // reinitialize hive client
    reInitHiveSyncClient();
    // after reinitializing hive client, table location shouldn't match hoodie base path
    assertNotEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(HiveTestUtil.TABLE_NAME), "new table location should match hoodie basepath");

    // Lets do the sync
    reSyncHiveTable();
    // verify partition count should be 4 from new basepath, not 5 from old
    assertEquals(4, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "the 4 partitions from new base path should be present for hive");
    // verify last commit time synced
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 110");
    // table location now should be updated to latest hoodie basepath
    assertEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(HiveTestUtil.TABLE_NAME), "new table location should match hoodie basepath");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  void testRecreateCOWTableWithSchemaEvolution(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);
    hiveSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "true");
    hiveSyncProps.setProperty(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true");
    hiveSyncProps.setProperty(HoodieCommonConfig.BASE_PATH.key(), basePath);
    hiveSyncProps.setProperty(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.INMEMORY.name());

    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    reInitHiveSyncClient();
    reSyncHiveTable();

    int fields = hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size();

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    DateTimeFormatter dtfOut = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    List<String> partitions = Arrays.asList(dateTime.format(dtfOut));
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(partitions, "/complex-schema-evolved.avsc", "/complex-schema-evolved.data", true, commitTime2);

    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(fields + 3, hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("STRING", hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to string");
    assertTrue(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  void testRecreateCOWTableWithPartitionEvolution(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);
    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    hiveSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "true");

    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 0, true);
    reInitHiveSyncClient();
    reSyncHiveTable();

    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();

    // Sync should add the one partition
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testUpdateTableComments(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/simple-test.avsc");
    reInitHiveSyncClient();
    reSyncHiveTable();

    Map<String, Pair<String, String>> alterCommentSchema = new HashMap<>();
    //generate commented schema field
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(HiveTestUtil.class, "/simple-test.avsc");
    HoodieSchema commentedSchema = SchemaTestUtil.getSchemaFromResource(HiveTestUtil.class, "/simple-test-doced.avsc");
    Map<String, String> fieldsNameAndDoc = commentedSchema.getFields().stream()
        .collect(Collectors.toMap(field -> field.name().toLowerCase(Locale.ROOT),
            field -> field.doc().map(doc -> StringUtils.isNullOrEmpty(doc) ? "" : doc).orElse("")));
    for (HoodieSchemaField field : schema.getFields()) {
      String name = field.name().toLowerCase(Locale.ROOT);
      String comment = fieldsNameAndDoc.get(name);
      if (fieldsNameAndDoc.containsKey(name) && !comment.equals(field.doc())) {
        alterCommentSchema.put(name, new ImmutablePair<>(field.schema().getType().name(), comment));
      }
    }

    ddlExecutor.updateTableComments(HiveTestUtil.TABLE_NAME, alterCommentSchema);

    List<FieldSchema> fieldSchemas = hiveClient.getMetastoreFieldSchemas(HiveTestUtil.TABLE_NAME);
    int commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (StringUtils.nonEmpty(fieldSchema.getCommentOrEmpty())) {
        commentCnt++;
      }
    }
    assertEquals(2, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncWithCommentedSchema(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_COMMENT.key(), "false");
    String commitTime = "100";
    HiveTestUtil.createCOWTableWithSchema(commitTime, "/simple-test-doced.avsc");

    reInitHiveSyncClient();
    reSyncHiveTable();
    List<FieldSchema> fieldSchemas = hiveClient.getMetastoreFieldSchemas(HiveTestUtil.TABLE_NAME);
    int commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (StringUtils.nonEmpty(fieldSchema.getCommentOrEmpty())) {
        commentCnt++;
      }
    }
    assertEquals(0, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");

    hiveSyncProps.setProperty(HIVE_SYNC_COMMENT.key(), "true");
    hiveSyncProps.setProperty(META_SYNC_INCREMENTAL.key(), "false");
    reInitHiveSyncClient();
    reSyncHiveTable();
    fieldSchemas = hiveClient.getMetastoreFieldSchemas(HiveTestUtil.TABLE_NAME);
    commentCnt = 0;
    for (FieldSchema fieldSchema : fieldSchemas) {
      if (StringUtils.nonEmpty(fieldSchema.getCommentOrEmpty())) {
        commentCnt++;
      }
    }
    assertEquals(2, commentCnt, "hive schema field comment numbers should match the avro schema field doc numbers");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndSchemaFromCommitMetadata")
  public void testSyncMergeOnRead(boolean useSchemaFromCommitMetadata, String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(roTableName), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(roTableName), "Table " + roTableName + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getMetastoreSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getMetastoreSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.getAllPartitions(roTableName).size(),
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
    reInitHiveSyncClient();
    reSyncHiveTable();

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getMetastoreSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getMetastoreSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(roTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadWithBasePathChange(boolean useSchemaFromCommitMetadata, String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    String rtTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(roTableName), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    assertFalse(hiveClient.tableExists(rtTableName), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    // change the hoodie base path
    basePath = Files.createTempDirectory("hivesynctest_new" + Instant.now().toEpochMilli()).toUri().toString();
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), basePath);

    String instantTime2 = "102";
    String deltaCommitTime2 = "103";
    // let's create MOR table in the new basepath
    HiveTestUtil.createMORTable(instantTime2, deltaCommitTime2, 2, true,
        useSchemaFromCommitMetadata);

    // let's add more partitions in the new basepath
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime3 = "104";
    String deltaCommitTime3 = "105";
    HiveTestUtil.addMORPartitions(2, true, false,
        useSchemaFromCommitMetadata, dateTime, commitTime3, deltaCommitTime3);

    // reinitialize hive client
    reInitHiveSyncClient();
    // verify table location is different from hoodie basepath
    assertNotEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(roTableName), "ro table location should not match hoodie base path before sync");
    assertNotEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(rtTableName), "rt table location should not match hoodie base path before sync");
    // Lets do the sync
    reSyncHiveTable();

    // verify partition count should be 4, not 5 from old basepath
    assertEquals(4, hiveClient.getAllPartitions(roTableName).size(),
        "the 4 partitions from new base path should be present for ro table");
    assertEquals(4, hiveClient.getAllPartitions(rtTableName).size(),
        "the 4 partitions from new base path should be present for rt table");
    // verify last synced commit time
    assertEquals(deltaCommitTime3, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
    assertEquals(deltaCommitTime3, hiveClient.getLastCommitTimeSynced(rtTableName).get(),
        "The last commit that was synced should be 103");
    // verify table location is updated to the new hoodie basepath
    assertEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(roTableName), "ro table location should match hoodie base path after sync");
    assertEquals(hiveClient.getBasePath(), hiveClient.getTableLocation(rtTableName), "rt table location should match hoodie base path after sync");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadRT(boolean useSchemaFromCommitMetadata, String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, useSchemaFromCommitMetadata);
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Let's do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.getAllPartitions(snapshotTableName).size(),
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
    reInitHiveSyncClient();
    reSyncHiveTable();

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(snapshotTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndStrategy")
  public void testSyncMergeOnReadWithStrategy(String syncMode, HoodieSyncTableStrategy strategy)  throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_TABLE_STRATEGY.key(), strategy.name());

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, true);

    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(roTableName),
            "Table " + roTableName + " should not exist initially");
    assertFalse(hiveClient.tableExists(snapshotTableName),
            "Table " + snapshotTableName + " should not exist initially");
    reSyncHiveTable();
    switch (strategy) {
      case RO:
        assertFalse(hiveClient.tableExists(snapshotTableName),
                "Table " + snapshotTableName
                        + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
                "Table " + HiveTestUtil.TABLE_NAME
                        + " should exist after sync completes");
        break;
      case RT:
        assertFalse(hiveClient.tableExists(roTableName),
                "Table " + roTableName
                        + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
                "Table " + HiveTestUtil.TABLE_NAME
                        + " should exist after sync completes");
        break;
      default:
        assertTrue(hiveClient.tableExists(roTableName),
                "Table " + roTableName
                        + " should exist after sync completes");
        assertTrue(hiveClient.tableExists(snapshotTableName),
                "Table " + snapshotTableName
                        + " should exist after sync completes");
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieSyncTableStrategy.class, names = {"RO", "RT"})
  public void testSyncMergeOnReadWithStrategyWhenTableExist(HoodieSyncTableStrategy strategy) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_TABLE_STRATEGY.key(), strategy.name());

    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, true);

    reInitHiveSyncClient();
    MessageType schema = hiveClient.getStorageSchema(true);

    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    String initInputFormatClassName = strategy.equals(HoodieSyncTableStrategy.RO)
            ? HoodieParquetRealtimeInputFormat.class.getName()
            : HoodieParquetInputFormat.class.getName();

    String outputFormatClassName = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET);
    String serDeFormatClassName = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET);

    // Create table 'test1'.
    hiveClient.createDatabase(HiveTestUtil.DB_NAME);
    hiveClient.createTable(HiveTestUtil.TABLE_NAME, schema, initInputFormatClassName,
            outputFormatClassName, serDeFormatClassName, new HashMap<>(), new HashMap<>());
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME + " should exist initially");

    String targetInputFormatClassName = strategy.equals(HoodieSyncTableStrategy.RO)
            ? HoodieParquetInputFormat.class.getName()
            : HoodieParquetRealtimeInputFormat.class.getName();

    StorageDescriptor storageDescriptor = hiveClient.getMetastoreStorageDescriptor(HiveTestUtil.TABLE_NAME);
    assertEquals(initInputFormatClassName, storageDescriptor.getInputFormat(),
            "Table " + HiveTestUtil.TABLE_NAME + " inputFormat should be " + targetInputFormatClassName);
    assertFalse(storageDescriptor.getSerdeInfo().getParameters().containsKey(ConfigUtils.IS_QUERY_AS_RO_TABLE),
            "Table " + HiveTestUtil.TABLE_NAME + " serdeInfo parameter " + ConfigUtils.IS_QUERY_AS_RO_TABLE + " should not exist");

    reSyncHiveTable();
    storageDescriptor = hiveClient.getMetastoreStorageDescriptor(HiveTestUtil.TABLE_NAME);
    assertEquals(targetInputFormatClassName,
            storageDescriptor.getInputFormat(),
            "Table " + HiveTestUtil.TABLE_NAME + " inputFormat should be " + targetInputFormatClassName);
    assertEquals(storageDescriptor.getSerdeInfo().getParameters().get(ConfigUtils.IS_QUERY_AS_RO_TABLE),
            strategy.equals(HoodieSyncTableStrategy.RO) ? "true" : "false",
            "Table " + HiveTestUtil.TABLE_NAME + " serdeInfo parameter " + ConfigUtils.IS_QUERY_AS_RO_TABLE + " should be ");

  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testMultiPartitionKeySync(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    hiveSyncProps.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), MultiPartKeysValueExtractor.class.getCanonicalName());
    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "year,month,day");

    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(5, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // HoodieHiveSyncClient had a bug where partition vals were sorted
    // and stored as keys in a map. The following tests this particular case.
    // Now lets create partition "2010/01/02" and followed by "2010/02/01".
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartition("2010/01/02", true, true, commitTime2);

    reInitHiveSyncClient();
    List<String> writtenPartitionsSince = hiveClient.getWrittenPartitionsSince(Option.of(instantTime), Option.of(getLastCommitCompletionTimeSynced()));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<org.apache.hudi.sync.common.model.Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, Collections.emptySet());
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    reSyncHiveTable();
    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be 101");

    // create partition "2010/02/01" and ensure sync works
    String commitTime3 = "102";
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, commitTime3);
    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reInitHiveSyncClient();
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(7, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    assertEquals(1, hiveClient.getWrittenPartitionsSince(Option.of(commitTime2), Option.of(getLastCommitCompletionTimeSynced())).size());
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testDropPartitionKeySync(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 1, true);

    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(1, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Collections.singletonList("2050/01/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, Collections.emptyList());
    assertEquals(1, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.TABLE_NAME, newPartition);
    FileCreateUtilsLegacy.createPartitionMetaFile(basePath, "2050/01/01");
    assertEquals(2, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "New partition should be added");

    reSyncHiveTable();

    // Drop 1 partition.
    ddlExecutor.runSQL("ALTER TABLE `" + HiveTestUtil.TABLE_NAME
        + "` DROP PARTITION (`datestr`='2050-01-01')");

    List<Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, hivePartitions.size(),
        "Table should have 1 partition because of the drop 1 partition");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testDropPartition(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 1, true);

    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Let's do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    List<Partition> partitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitions.size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    // add a partition but do not sync
    String instantTime2 = "101";
    String newPartition = "2010/02/01";
    HiveTestUtil.addCOWPartition(newPartition, true, true, instantTime2);
    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);
    partitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitions.size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // create two replace commits to delete current partitions, but do not sync in between
    String partitiontoDelete = partitions.get(0).getValues().get(0).replace("-", "/");
    String instantTime3 = "102";
    HiveTestUtil.createReplaceCommit(instantTime3, partitiontoDelete, WriteOperationType.DELETE_PARTITION, true, true);
    String instantTime4 = "103";
    HiveTestUtil.createReplaceCommit(instantTime4, newPartition, WriteOperationType.DELETE_PARTITION, true, true);

    // now run hive sync
    reInitHiveSyncClient();
    reSyncHiveTable();

    List<Partition> hivePartitions = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(0, hivePartitions.size(),
        "Table should have no partitions");
    assertEquals(instantTime4, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  void testGetPartitionEvents_droppedStoragePartitionNotPresentInMetastore(
      String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    // Create a table with 1 partition
    String instantTime1 = "100";
    HiveTestUtil.createCOWTable(instantTime1, 1, true);

    reInitHiveSyncClient();
    // Sync the table to metastore
    reSyncHiveTable();
    
    List<Partition> partitionsInMetastore = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitionsInMetastore.size(), "Should have 1 partition in metastore");

    // Add a partition to storage but don't sync it to metastore
    String instantTime2 = "101";
    String newPartition = "2010/02/01";
    HiveTestUtil.addCOWPartition(newPartition, true, true, instantTime2);
    
    // Verify the partition is not in metastore yet
    partitionsInMetastore = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitionsInMetastore.size(), "Should have 1 partition in metastore");

    // Delete the partition that was never synced to metastore
    String instantTime3 = "102";
    HiveTestUtil.createReplaceCommit(instantTime3, newPartition, WriteOperationType.DELETE_PARTITION, true, true);
    
    // Add another partition to storage but don't sync to metastore
    String instantTime4 = "103";
    String addPartition = "2010/04/01";
    HiveTestUtil.addCOWPartition(addPartition, true, true, instantTime4);

    reInitHiveSyncClient();

    Set<String> droppedPartitionsOnStorage = hiveClient.getDroppedPartitionsSince(Option.of(instantTime1), Option.of(instantTime1));
    List<String> writtenPartitionsOnStorage = hiveClient.getWrittenPartitionsSince(Option.of(instantTime1), Option.of(instantTime1));
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(
        partitionsInMetastore, writtenPartitionsOnStorage, droppedPartitionsOnStorage);
    
    // Verify no DROP event is generated for partition that was never in metastore
    long dropEvents = partitionEvents.stream()
        .filter(e -> e.eventType == PartitionEventType.DROP)
        .count();
    assertEquals(0, dropEvents,
        "No DROP partition event should be generated for partition that was never in metastore");
    
    // Verify ADD event is generated for the new partition that was added to storage
    List<PartitionEvent> addEvents = partitionEvents.stream()
        .filter(e -> e.eventType == PartitionEventType.ADD)
        .collect(Collectors.toList());
    assertEquals(1, addEvents.size(),
        "ADD partition event should be generated for new partition added to storage");
    assertEquals(addPartition, addEvents.get(0).storagePartition);
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  void testGetPartitionEvents_droppedStoragePartitionPresentInMetastore(
      String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    // Create a table with 1 partition
    String instantTime1 = "100";
    HiveTestUtil.createCOWTable(instantTime1, 1, true);

    reInitHiveSyncClient();
    // Sync the table to metastore
    reSyncHiveTable();
    
    List<Partition> partitionsInMetastore = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(1, partitionsInMetastore.size(), "Should have 1 partition in metastore");

    // Add a partition and sync it to metastore
    String instantTime2 = "101";
    String newPartition = "2010/02/01";
    HiveTestUtil.addCOWPartition(newPartition, true, true, instantTime2);

    reInitHiveSyncClient();
    // Sync the table to metastore
    reSyncHiveTable();
    
    partitionsInMetastore = hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME);
    assertEquals(2, partitionsInMetastore.size(), "Should have 2 partitions in metastore");
    
    // Now delete the partition that exists in metastore
    String instantTime3 = "102";
    HiveTestUtil.createReplaceCommit(instantTime3, newPartition, WriteOperationType.DELETE_PARTITION, true, true);
    
    // Add another partition to storage but don't sync to metastore
    String instantTime4 = "103";
    String addPartition = "2010/04/01";
    HiveTestUtil.addCOWPartition(addPartition, true, true, instantTime4);
    
    reInitHiveSyncClient();
    
    // Get partition events
    Set<String> droppedPartitionsOnStorage = hiveClient.getDroppedPartitionsSince(Option.of(instantTime2), Option.of(instantTime2));
    List<String> writtenPartitionsOnStorage = hiveClient.getWrittenPartitionsSince(Option.of(instantTime2), Option.of(instantTime2));
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(
        partitionsInMetastore, writtenPartitionsOnStorage, droppedPartitionsOnStorage);
    
    // Verify DROP event is generated for partition that exists in metastore
    List<PartitionEvent> dropEvents = partitionEvents.stream()
        .filter(e -> e.eventType == PartitionEventType.DROP)
        .collect(Collectors.toList());
    assertEquals(1, dropEvents.size(),
        "DROP partition event should be generated for partition that exists in metastore");
    assertEquals(newPartition, dropEvents.get(0).storagePartition);
    
    // Verify ADD event is generated for the new partition that was added to storage
    List<PartitionEvent> addEvents = partitionEvents.stream()
        .filter(e -> e.eventType == PartitionEventType.ADD)
        .collect(Collectors.toList());
    assertEquals(1, addEvents.size(),
        "ADD partition event should be generated for new partition added to storage");
    assertEquals(addPartition, addEvents.get(0).storagePartition);
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testNonPartitionedSync(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);
    // Set partition value extractor to NonPartitionedExtractor
    hiveSyncProps.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), NonPartitionedExtractor.class.getCanonicalName());
    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");

    HiveTestUtil.getCreatedTablesSet().add(HiveTestUtil.DB_NAME + "." + HiveTestUtil.TABLE_NAME);

    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
    // Lets do the sync
    reSyncHiveTable();
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size(),
        "Hive Schema should match the table schema，ignoring the partition fields");
    assertEquals(0, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(),
        "Table should not have partitions because of the NonPartitionedExtractor");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testReadSchemaForMOR(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);

    String commitTime = "100";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(commitTime, "", 5, false, true);
    reInitHiveSyncClient();

    assertFalse(hiveClient.tableExists(snapshotTableName), "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should not exist initially");

    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName), "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should exist after sync completes");

    // Schema being read from compacted base files
    assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + getPartitionFieldSize()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.getAllPartitions(snapshotTableName).size(), "Table partitions should match the number of partitions we wrote");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();

    // Schema being read from the log filesTestHiveSyncTool
    assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
        SchemaTestUtil.getEvolvedSchema().getFields().size() + getPartitionFieldSize()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the evolved table schema + partition field");
    // Sync should add the one partition
    assertEquals(6, hiveClient.getAllPartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("syncModeAndStrategy")
  void testRecreateMORTableWithSchemaEvolution(String syncMode, HoodieSyncTableStrategy strategy) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_TABLE_STRATEGY.key(), strategy.name());
    hiveSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "true");

    String commitTime = "100";
    HiveTestUtil.createMORTable(commitTime, "", 5, false, true);
    reInitHiveSyncClient();
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    String roTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    // Lets do the sync
    reSyncHiveTable();
    switch (strategy) {
      case RO:
        assertFalse(hiveClient.tableExists(snapshotTableName),
            "Table " + snapshotTableName
                + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME
                + " should exist after sync completes");
        break;
      case RT:
        assertFalse(hiveClient.tableExists(roTableName),
            "Table " + roTableName
                + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME
                + " should exist after sync completes");
        break;
      default:
        assertTrue(hiveClient.tableExists(roTableName),
            "Table " + roTableName
                + " should exist after sync completes");
        assertTrue(hiveClient.tableExists(snapshotTableName),
            "Table " + snapshotTableName
                + " should exist after sync completes");
    }

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";
    DateTimeFormatter dtfOut = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    List<String> partitions = Arrays.asList(dateTime.format(dtfOut));
    HiveTestUtil.addMORPartitions(partitions, "/complex-schema-evolved.avsc", "/complex-schema-evolved.data", true, commitTime2, deltaCommitTime2);

    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();

    switch (strategy) {
      case RO:
        assertFalse(hiveClient.tableExists(snapshotTableName),
            "Table " + snapshotTableName
                + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME
                + " should exist after sync completes");
        assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
            7 + HoodieRecord.HOODIE_META_COLUMNS.size(),
            "Hive Schema should match the evolved table schema + partition field");
        // Sync should add the one partition
        assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(), "The 1 partition we wrote should be added to hive");
        assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
            "The last commit that was synced should be 103");
        break;
      case RT:
        assertFalse(hiveClient.tableExists(roTableName),
            "Table " + roTableName
                + " should not exist initially");
        assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
            "Table " + HiveTestUtil.TABLE_NAME
                + " should exist after sync completes");
        assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
            7 + HoodieRecord.HOODIE_META_COLUMNS.size(),
            "Hive Schema should match the evolved table schema + partition field");
        // Sync should add the one partition
        assertEquals(6, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(), "The 1 partition we wrote should be added to hive");
        assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(),
            "The last commit that was synced should be 103");
        break;
      default:
        assertTrue(hiveClient.tableExists(roTableName),
            "Table " + roTableName
                + " should exist after sync completes");
        assertTrue(hiveClient.tableExists(snapshotTableName),
            "Table " + snapshotTableName
                + " should exist after sync completes");
        assertEquals(hiveClient.getMetastoreSchema(roTableName).size(),
            7 + HoodieRecord.HOODIE_META_COLUMNS.size(),
            "Hive Schema should match the evolved table schema + partition field");
        // Sync should add the one partition
        assertEquals(6, hiveClient.getAllPartitions(roTableName).size(), "The 1 partition we wrote should be added to hive");
        assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
            "The last commit that was synced should be 103");
        // Sync should add the one partition
        assertEquals(6, hiveClient.getAllPartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
        assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
            7 + HoodieRecord.HOODIE_META_COLUMNS.size(),
            "Hive Schema should match the evolved table schema + partition field");
        assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
            "The last commit that was synced should be 103");
    }
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  void testRecreateMORTableWithPartitionEvolution(String syncMode, String enablePushDown) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);
    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    hiveSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "true");

    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 0, true, true);
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");


    assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");


    assertEquals(0, hiveClient.getAllPartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");


    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reInitHiveSyncClient();
    reSyncHiveTable();

    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @ValueSource(strings = {"hiveql", "hms", "jdbc"})
  void testMORTableWithPartitionEvolutionThrowsException(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), "true");
    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    hiveSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "false");

    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 0, true, true);
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(snapshotTableName),
        "Table " + HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");


    assertEquals(hiveClient.getMetastoreSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");


    assertEquals(0, hiveClient.getAllPartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");


    hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    reInitHiveSyncClient();
    assertThrows(HoodieException.class, this::reSyncHiveTable, "The sync operation should throw exception due to partition mismatch and table recreation is disabled");
  }

  @Test
  public void testConnectExceptionIgnoreConfigSet() throws IOException, URISyntaxException {
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, false);
    reInitHiveSyncClient();
    HoodieHiveSyncClient prevHiveClient = hiveClient;
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    // Lets do the sync
    hiveSyncProps.setProperty(HIVE_IGNORE_EXCEPTIONS.key(), "true");
    hiveSyncProps.setProperty(HIVE_URL.key(), hiveSyncProps.getString(HIVE_URL.key())
        .replace(String.valueOf(HiveTestUtil.hiveTestService.getHiveServerPort()), String.valueOf(NetworkTestUtils.nextFreePort())));
    reInitHiveSyncClient();
    reSyncHiveTable();

    assertNull(hiveClient);
    assertFalse(prevHiveClient.tableExists(HiveTestUtil.TABLE_NAME),
        "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");
  }

  private void verifyOldParquetFileTest(HoodieHiveSyncClient hiveClient, String emptyCommitTime) throws Exception {
    assertTrue(hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should exist after sync completes");
    assertEquals(hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(),
        hiveClient.getStorageSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(1, hiveClient.getAllPartitions(HiveTestUtil.TABLE_NAME).size(), "Table partitions should match the number of partitions we wrote");
    assertEquals(emptyCommitTime,
        hiveClient.getLastCommitTimeSynced(HiveTestUtil.TABLE_NAME).get(), "The last commit that was synced should be updated in the TBLPROPERTIES");

    // make sure correct schema is picked
    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();
    for (HoodieSchemaField field : schema.getFields()) {
      assertEquals(field.schema().getType().name().toLowerCase(),
          hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).get(field.name()).toLowerCase(),
          String.format("Hive Schema Field %s was added", field));
    }
    assertEquals("string",
        hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).get("datestr").toLowerCase(), "Hive Schema Field datestr was added");
    assertEquals(schema.getFields().size() + 1 + HoodieRecord.HOODIE_META_COLUMNS.size(),
        hiveClient.getMetastoreSchema(HiveTestUtil.TABLE_NAME).size(), "Hive Schema fields size");
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testPickingOlderParquetFileIfLatestIsEmptyCommit(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    reInitHiveSyncClient();
    assertFalse(hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    reInitHiveSyncClient();
    reSyncHiveTable();

    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testNotPickingOlderParquetFileWhenLatestCommitReadFails(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
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

    reInitHiveSyncClient();
    assertFalse(
        hiveClient.tableExists(HiveTestUtil.TABLE_NAME), "Table " + HiveTestUtil.TABLE_NAME + " should not exist initially");

    HiveSyncTool tool = new HiveSyncTool(hiveSyncProps, getHiveConf());
    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + TIMELINEFOLDER_NAME + "/" + INSTANT_FILE_NAME_GENERATOR.getFileName(hiveClient.getActiveTimeline().getInstantsAsStream()
        .filter(inst -> inst.requestedTime().equals(commitTime2))
        .findFirst().get()));
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
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    //HiveTestUtil.createCommitFile(commitMetadata, emptyCommitTime);
    reInitHiveSyncClient();
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

    reInitHiveSyncClient();
    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + TIMELINEFOLDER_NAME + "/" + INSTANT_FILE_NAME_GENERATOR.getFileName(hiveClient.getActiveTimeline().getInstantsAsStream()
        .filter(inst -> inst.requestedTime().equals(commitTime2))
        .findFirst().get()));
    assertTrue(HiveTestUtil.fileSystem.delete(fullPath, false));
    try {
      reSyncHiveTable();
    } catch (RuntimeException e) {
      // we expect the table sync to fail
    } finally {
      reInitHiveSyncClient();
    }

    // old sync values should be left intact
    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testTypeConverter(String syncMode) throws Exception {
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    HiveTestUtil.createCOWTable("100", 5, true);
    // create database.
    ddlExecutor.runSQL("create database " + HiveTestUtil.DB_NAME);
    reInitHiveSyncClient();
    String tableName = HiveTestUtil.TABLE_NAME;
    String tableAbsoluteName = String.format(" `%s.%s` ", HiveTestUtil.DB_NAME, tableName);
    String dropTableSql = String.format("DROP TABLE IF EXISTS %s ", tableAbsoluteName);
    String createTableSqlPrefix = String.format("CREATE TABLE IF NOT EXISTS %s ", tableAbsoluteName);
    String errorMsg = "An error occurred in decimal type converting.";
    ddlExecutor.runSQL(dropTableSql);

    // test one column in DECIMAL
    String oneTargetColumnSql = createTableSqlPrefix + "(`decimal_col` DECIMAL(9,8), `bigint_col` BIGINT)";
    ddlExecutor.runSQL(oneTargetColumnSql);
    assertTrue(hiveClient.getMetastoreSchema(tableName).containsValue("DECIMAL(9,8)"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);

    // test multiple columns in DECIMAL
    String multipleTargetColumnSql =
        createTableSqlPrefix + "(`decimal_col1` DECIMAL(9,8), `bigint_col` BIGINT, `decimal_col2` DECIMAL(7,4))";
    ddlExecutor.runSQL(multipleTargetColumnSql);
    assertTrue(hiveClient.getMetastoreSchema(tableName).containsValue("DECIMAL(9,8)")
        && hiveClient.getMetastoreSchema(tableName).containsValue("DECIMAL(7,4)"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);

    // test no columns in DECIMAL
    String noTargetColumnsSql = createTableSqlPrefix + "(`bigint_col` BIGINT)";
    ddlExecutor.runSQL(noTargetColumnsSql);
    assertTrue(hiveClient.getMetastoreSchema(tableName).size() == 1 && hiveClient.getMetastoreSchema(tableName)
        .containsValue("BIGINT"), errorMsg);
    ddlExecutor.runSQL(dropTableSql);
  }

  @ParameterizedTest
  @MethodSource("syncMode")
  public void testSyncWithoutDiffs(String syncMode) throws Exception {
    String tableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(META_SYNC_CONDITIONAL_SYNC.key(), "true");

    String commitTime0 = "100";
    String commitTime1 = "101";
    String commitTime2 = "102";
    String commitTime3 = "103";
    String commitTime4 = "104";
    String commitTime5 = "105";
    HiveTestUtil.createMORTable(commitTime0, commitTime1, 2, true, true);

    reInitHiveSyncClient();
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(tableName));
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get());

    HiveTestUtil.addMORPartitions(0, true, true, true, ZonedDateTime.now().plusDays(2), commitTime2, commitTime3);

    reSyncHiveTable();
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get());

    // Let the last commit time synced to be before the start of the active timeline,
    // to trigger the fallback of listing all partitions. There is no partition change
    // and the last commit time synced should still be the same.
    HiveTestUtil.addMORPartitions(0, true, true, true, ZonedDateTime.now().plusDays(2), commitTime4, commitTime5);
    HiveTestUtil.removeCommitFromActiveTimeline(commitTime0, COMMIT_ACTION);
    HiveTestUtil.removeCommitFromActiveTimeline(commitTime1, DELTA_COMMIT_ACTION);
    HiveTestUtil.removeCommitFromActiveTimeline(commitTime2, COMMIT_ACTION);
    HiveTestUtil.removeCommitFromActiveTimeline(commitTime3, DELTA_COMMIT_ACTION);
    reInitHiveSyncClient();
    reSyncHiveTable();
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get());
  }

  @ParameterizedTest
  @MethodSource("syncModeAndEnablePushDown")
  public void testHiveSyncWithMultiWriter(String syncMode, String enablePushDown) throws Exception {
    String tableName = HiveTestUtil.TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), syncMode);
    hiveSyncProps.setProperty(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED.key(), enablePushDown);
    hiveSyncProps.setProperty(META_SYNC_CONDITIONAL_SYNC.key(), "true");

    String commitTime1 = InProcessTimeGenerator.createNewInstantTime(1);
    String commitTime2 = InProcessTimeGenerator.createNewInstantTime(2);
    String commitTime3 = InProcessTimeGenerator.createNewInstantTime(3);
    String commitTime4 = InProcessTimeGenerator.createNewInstantTime(4);
    String commitTime5 = InProcessTimeGenerator.createNewInstantTime(5);

    // Commit 4 and commit5 will be committed first
    HiveTestUtil.createMORTable(commitTime4, commitTime5, 2, true, true);
    reInitHiveSyncClient();
    reSyncHiveTable();

    assertTrue(hiveClient.tableExists(tableName));
    assertEquals(commitTime5, hiveClient.getLastCommitTimeSynced(tableName).get());
    assertEquals(getLastCommitCompletionTimeSynced(), hiveClient.getLastCommitCompletionTimeSynced(tableName).get());
    assertEquals(2, hiveClient.getAllPartitions(tableName).size());

    // Commit2 and commit3 committed after commit4 and commit5
    HiveTestUtil.addMORPartitions(4, true, true, true, ZonedDateTime.now().plusDays(2), commitTime2, commitTime3);
    reInitHiveSyncClient();
    reSyncHiveTable();

    String lastCommitCompletionTimeSynced = getLastCommitCompletionTimeSynced();
    // LastCommitTimeSynced will still be commit3
    assertEquals(commitTime5, hiveClient.getLastCommitTimeSynced(tableName).get());
    assertEquals(lastCommitCompletionTimeSynced, hiveClient.getLastCommitCompletionTimeSynced(tableName).get());
    // Partitions included in commit0 and commit1 will be synced to HMS correctly
    assertEquals(4, hiveClient.getAllPartitions(tableName).size());

    List<Partition> partitions = hiveClient.getAllPartitions(tableName);
    // create two replace commits to delete current partitions, but do not sync in between
    String partitiontoDelete = partitions.get(0).getValues().get(0).replace("-", "/");
    // Add commit1 to the table that replace some partitions
    HiveTestUtil.createReplaceCommit(commitTime1, partitiontoDelete, WriteOperationType.DELETE_PARTITION, true, true);

    reInitHiveSyncClient();
    reSyncHiveTable();

    assertEquals(getLastCommitCompletionTimeSynced(), hiveClient.getLastCommitCompletionTimeSynced(tableName).get());
    assertEquals(commitTime5, hiveClient.getLastCommitTimeSynced(tableName).get());
    assertEquals(3, hiveClient.getAllPartitions(tableName).size());
  }

  @Test
  public void testDatabaseAndTableName() {
    reInitHiveSyncClient();

    assertEquals(HiveTestUtil.DB_NAME, hiveSyncTool.getDatabaseName());
    assertEquals(HiveTestUtil.TABLE_NAME, hiveSyncTool.getTableName());

    String updatedDb = "updated_db";
    String updatedTable = "updated_table";

    hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), updatedDb);
    hiveSyncProps.setProperty(META_SYNC_TABLE_NAME.key(), updatedTable);

    reInitHiveSyncClient();

    assertEquals(updatedDb, hiveSyncTool.getDatabaseName(), "Database name should match the updated value");
    assertEquals(updatedTable, hiveSyncTool.getTableName(), "Table name should match the updated value");

    assertEquals(updatedDb, hiveClient.getDatabaseName(), "Database name in sync client should match");
    assertEquals(updatedTable, hiveClient.getTableName(), "Table name in sync client should match");
  }

  private void reSyncHiveTable() {
    hiveSyncTool.syncHoodieTable();
    // we need renew the hive client after tool.syncHoodieTable(), because it will close hive
    // session, then lead to connection retry, we can see there is a exception at log.
    reInitHiveSyncClient();
  }

  private String getLastCommitCompletionTimeSynced() {
    return hiveClient.getActiveTimeline().getLatestCompletionTime().get();
  }

  private void reInitHiveSyncClient() {
    hiveSyncTool = new HiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf());
    hiveClient = (HoodieHiveSyncClient) hiveSyncTool.syncClient;
  }

  private int getPartitionFieldSize() {
    return hiveSyncProps.getString(META_SYNC_PARTITION_FIELDS.key()).split(",").length;
  }
}
