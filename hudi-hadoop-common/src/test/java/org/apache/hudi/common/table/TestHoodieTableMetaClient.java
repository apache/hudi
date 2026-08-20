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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie table meta client {@link HoodieTableMetaClient}.
 */
class TestHoodieTableMetaClient extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  void checkMetadata() {
    assertEquals(HoodieTestUtils.RAW_TRIPS_TEST_NAME, metaClient.getTableConfig().getTableName(),
        "Table name should be raw_trips");
    assertEquals(basePath, metaClient.getBasePath().toString(), "Basepath should be the one assigned");
    assertEquals(basePath + "/.hoodie", metaClient.getMetaPath().toString(),
        "Metapath should be ${basepath}/.hoodie");
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertTrue(HoodieTableConfig.validateChecksum(metaClient.getTableConfig().getProps()));
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig().getTableInitialVersion());
  }

  @Test
  void testSerDe() throws IOException {
    // check if this object is serialized and de-serialized, we are able to read from the file system
    HoodieTableMetaClient deserializedMetaClient =
        HoodieTestUtils.serializeDeserialize(metaClient, HoodieTableMetaClient.class);
    assertNotNull(deserializedMetaClient);
    HoodieActiveTimeline commitTimeline = deserializedMetaClient.getActiveTimeline();
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "1");
    commitTimeline.createNewInstant(instant);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("key", "val");
    commitTimeline.saveAsComplete(instant, Option.of(metadata));
    commitTimeline = commitTimeline.reload();
    HoodieInstant completedInstant = commitTimeline.getInstantsAsStream().findFirst().get();
    assertTrue(completedInstant.isCompleted());
    assertEquals(completedInstant.requestedTime(), instant.requestedTime());
    assertEquals("val", metaClient.getActiveTimeline().readCommitMetadata(completedInstant).getExtraMetadata().get("key"));
  }

  @Test
  void testCommitTimeline() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertTrue(activeCommitTimeline.empty(), "Should be empty commit timeline");

    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "1");
    activeTimeline.createNewInstant(instant);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("key", "val");
    activeTimeline.saveAsComplete(instant, Option.of(metadata));

    // Commit timeline should not auto-reload every time getActiveCommitTimeline(), it should be cached
    activeTimeline = metaClient.getActiveTimeline();
    activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertTrue(activeCommitTimeline.empty(), "Should be empty commit timeline");

    activeTimeline = activeTimeline.reload();
    HoodieInstant completedInstant = activeTimeline.getCommitsTimeline().getInstantsAsStream().findFirst().get();
    activeCommitTimeline = activeTimeline.getCommitAndReplaceTimeline();
    assertFalse(activeCommitTimeline.empty(), "Should be the 1 commit we made");
    assertTrue(completedInstant.isCompleted());
    assertTrue(completedInstant.requestedTime().equals(instant.requestedTime()));
    assertEquals("val", metaClient.getActiveTimeline().readCommitMetadata(completedInstant).getExtraMetadata().get("key"));
  }

  @Test
  void testEquals() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1, metaClient2);
    assertNotEquals(metaClient1, null);
    assertNotEquals(metaClient1, new Object());
  }

  @Test
  void testToString() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1.toString(), metaClient2.toString());
    assertNotEquals(metaClient1.toString(), new Object().toString());
  }

  @Test
  void testMetaFieldsModeDerivesLegacyBooleanAndOverridesContradictions() throws IOException {
    // hoodie.properties must never record a legacy boolean that contradicts the mode: a pre-1.3.0
    // reader ignores hoodie.meta.fields.mode entirely and would otherwise treat a selectively
    // written table as ALL. For NONE that is unsafe — an old incremental reader would run against
    // all-null commit times and silently return no rows.
    //
    // The mode is the only source of truth: the deprecated boolean is always derived from it.
    for (MetaFieldsMode mode : MetaFieldsMode.values()) {
      final String path = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-derive-" + mode.name();
      HoodieTableMetaClient created = HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName("mfm-derive-" + mode.name())
          .setMetaFieldsMode(mode)
          .initTable(this.metaClient.getStorageConf(), path);
      assertEquals(mode, created.getTableConfig().getMetaFieldsMode());
      assertEquals(mode.toLegacyPopulateMetaFields(), created.getTableConfig().populateMetaFields(),
          "the legacy boolean must be derived from the mode for " + mode);

      // Restating the derived value is coherent and must be accepted.
      final String restatedPath = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-restate-" + mode.name();
      HoodieTableMetaClient restated = HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName("mfm-restate-" + mode.name())
          .setPopulateMetaFields(mode.toLegacyPopulateMetaFields())
          .setMetaFieldsMode(mode)
          .initTable(this.metaClient.getStorageConf(), restatedPath);
      assertEquals(mode, restated.getTableConfig().getMetaFieldsMode());

      // A contradicting legacy boolean is ignored in favor of the mode.
      final String conflictPath = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-conflict-" + mode.name();
      HoodieTableMetaClient conflict = HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName("mfm-conflict-" + mode.name())
          .setPopulateMetaFields(!mode.toLegacyPopulateMetaFields())
          .setMetaFieldsMode(mode)
          .initTable(this.metaClient.getStorageConf(), conflictPath);
      assertEquals(mode, conflict.getTableConfig().getMetaFieldsMode());
      assertEquals(mode.toLegacyPopulateMetaFields(), conflict.getTableConfig().populateMetaFields());
    }

    // No explicit mode: pre-1.3.0 behavior is preserved by inferring and recording the mode.
    final String legacyPath = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-legacy";
    HoodieTableMetaClient legacy = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("mfm-legacy")
        .setPopulateMetaFields(false)
        .initTable(this.metaClient.getStorageConf(), legacyPath);
    assertEquals(MetaFieldsMode.NONE, legacy.getTableConfig().getMetaFieldsMode());
    assertEquals(MetaFieldsMode.NONE.name(),
        legacy.getTableConfig().getString(HoodieTableConfig.META_FIELDS_MODE),
        "legacy table settings must be normalized to the mode property");
    assertFalse(legacy.getTableConfig().populateMetaFields());
  }

  /**
   * From table version 10 a newly created table records only {@code hoodie.meta.fields.mode}; below
   * that it records the deprecated boolean too.
   *
   * <p>The split is about who reads the table. Selective modes have to work on older versions -- a
   * fleet adopting this feature runs patched and unpatched pipelines against the same v6 tables for
   * months, and an unpatched reader knows only the boolean. With the property absent it falls back
   * to its {@code true} default and would treat a selective table as ALL, over-claiming meta columns
   * that are physically null. A table only reaches v10 once everything touching it understands the
   * mode, so by then the boolean is redundant.
   *
   * <p>The v9-to-v10 upgrade similarly persists the mode and removes the deprecated boolean.
   */
  @Test
  void testMetaFieldsModeDropsTheLegacyBooleanFromTableVersionTen() throws IOException {
    for (MetaFieldsMode mode : MetaFieldsMode.values()) {
      final String newPath = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-v10-" + mode.name();
      HoodieTableConfig newTable = HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName("mfm-v10-" + mode.name())
          .setTableVersion(HoodieTableVersion.TEN.versionCode())
          .setMetaFieldsMode(mode)
          .initTable(this.metaClient.getStorageConf(), newPath)
          .getTableConfig();
      assertEquals(mode, newTable.getMetaFieldsMode());
      assertFalse(newTable.getProps().containsKey(HoodieTableConfig.POPULATE_META_FIELDS.key()),
          "a table created at v10 records the mode alone for " + mode);

      final String oldPath = tempDir.toAbsolutePath() + Path.SEPARATOR + "mfm-v6-" + mode.name();
      HoodieTableConfig oldTable = HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName("mfm-v6-" + mode.name())
          .setTableVersion(HoodieTableVersion.SIX.versionCode())
          .setMetaFieldsMode(mode)
          .initTable(this.metaClient.getStorageConf(), oldPath)
          .getTableConfig();
      assertEquals(mode, oldTable.getMetaFieldsMode());
      // Assert the raw property, not populateMetaFields(): the accessor resolves through the mode,
      // so it answers the same whether or not the boolean was persisted. An unpatched reader sees
      // only the raw key.
      assertEquals(Boolean.toString(mode.toLegacyPopulateMetaFields()),
          oldTable.getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()),
          "a table below v10 must persist the derived boolean for unpatched readers, " + mode);
    }
  }

  @Test
  void testTableVersion() throws IOException {
    final String basePath = tempDir.toAbsolutePath() + Path.SEPARATOR + "t1";
    HoodieTableMetaClient metaClient1 = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ.name())
        .setTableName("table-version-test")
        .setTableVersion(HoodieTableVersion.SIX.versionCode())
        .initTable(this.metaClient.getStorageConf(), basePath);
    assertEquals(HoodieTableVersion.SIX, metaClient1.getTableConfig().getTableVersion());

    HoodieTableMetaClient metaClient2 = HoodieTableMetaClient.builder()
        .setConf(this.metaClient.getStorageConf())
        .setBasePath(basePath)
        .build();
    assertEquals(HoodieTableVersion.SIX, metaClient2.getTableConfig().getTableVersion());
  }

  @Test
  void testGenerateFromAnotherMetaClient() throws IOException {
    final String basePath1 = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "t2A";
    final String basePath2 = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "t2B";

    HoodieTableMetaClient metaClient1 = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ.name())
        .setTableName("table-version-test")
        .setTableVersion(HoodieTableVersion.SIX.versionCode())
        .initTable(this.metaClient.getStorageConf(), basePath1);

    HoodieTableMetaClient metaClient2 = HoodieTableMetaClient.newTableBuilder()
        .fromMetaClient(metaClient1)
        .initTable(this.metaClient.getStorageConf(), basePath2);

    assertEquals(metaClient1.getTableConfig().getTableType(), metaClient2.getTableConfig().getTableType());
    assertEquals(metaClient1.getTableConfig().getTableVersion(), metaClient2.getTableConfig().getTableVersion());
    assertEquals(metaClient1.getTableConfig().getTableName(), metaClient2.getTableConfig().getTableName());
  }

  @Test
  void testTableStorageLayoutFromMetaClient() throws IOException {
    final String basePath1 = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "lsm1";
    final String basePath2 = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "lsm2";

    HoodieTableMetaClient metaClient1 = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName("lsm-table")
        .setTableStorageLayout(HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue())
        .initTable(this.metaClient.getStorageConf(), basePath1);

    HoodieTableMetaClient metaClient2 = HoodieTableMetaClient.newTableBuilder()
        .fromMetaClient(metaClient1)
        .initTable(this.metaClient.getStorageConf(), basePath2);

    assertEquals(HoodieTableConfig.TableStorageLayout.LSM_TREE,
        metaClient2.getTableConfig().getTableStorageLayout());
  }

  @Test
  void testTableBuilderRequiresTableNameAndType() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieTableMetaClient.builder()
          .setConf(this.metaClient.getStorageConf())
          .build();
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieTableMetaClient.newTableBuilder()
          .setTableName("test-table")
          .initTable(this.metaClient.getStorageConf(), tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "failing2");
    });
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieTableMetaClient.newTableBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .initTable(this.metaClient.getStorageConf(), tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "failing3");
    });
  }

  @Test
  void testCreateMetaClientFromProperties() throws IOException {
    final String basePath = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "t5";
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), "test-table");
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    props.setProperty(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());

    HoodieTableMetaClient metaClient1 = HoodieTableMetaClient.newTableBuilder()
        .fromProperties(props)
        .initTable(this.metaClient.getStorageConf(), basePath);

    HoodieTableMetaClient metaClient2 = HoodieTableMetaClient.builder()
        .setConf(this.metaClient.getStorageConf())
        .setBasePath(basePath)
        .build();

    // test table name and type and precombine field also match
    assertEquals(metaClient1.getTableConfig().getTableName(), metaClient2.getTableConfig().getTableName());
    assertEquals(metaClient1.getTableConfig().getTableType(), metaClient2.getTableConfig().getTableType());
    assertEquals(metaClient1.getTableConfig().getOrderingFields(), metaClient2.getTableConfig().getOrderingFields());
    assertEquals(HoodieTableConfig.TableStorageLayout.LSM_TREE, metaClient2.getTableConfig().getTableStorageLayout());
    // default table version should be current version
    assertEquals(HoodieTableVersion.current(), metaClient2.getTableConfig().getTableVersion());
  }

  @Test
  void testCreateLayoutInStorage() throws IOException {
    final String basePath = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "t6";
    HoodieTableMetaClient metaClient1 = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("table-layout-test")
        .initTable(this.metaClient.getStorageConf(), basePath);

    // test the folder structure
    this.metaClient.getRawStorage().exists(new StoragePath(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME));
    this.metaClient.getRawStorage().exists(new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
    this.metaClient.getRawStorage().exists(new StoragePath(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME));
    this.metaClient.getRawStorage().exists(new StoragePath(basePath, HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue()));
    this.metaClient.getRawStorage().exists(new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME
        + Path.SEPARATOR + "hoodie.properties"));
  }

  @Test
  void testGetIndexDefinitionPath() throws IOException {
    final String basePath = tempDir.toAbsolutePath() + Path.SEPARATOR + "t7";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("table")
        .initTable(this.metaClient.getStorageConf(), basePath);
    assertEquals(metaClient.getMetaPath() + "/.index_defs/index.json", metaClient.getIndexDefinitionPath());

    String randomDefinitionPath = "/a/b/c";
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key(), "/a/b/c");
    assertEquals(randomDefinitionPath, metaClient.getIndexDefinitionPath());
  }

  @Test
  void testDeleteDefinition() throws IOException {
    final String basePath = tempDir.toAbsolutePath() + Path.SEPARATOR + "t7";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("table")
        .initTable(this.metaClient.getStorageConf(), basePath);
    Map<String, Map<String, String>> columnsMap = new HashMap<>();
    columnsMap.put("c1", Collections.emptyMap());
    String indexName = MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath() + "idx";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType("column_stats")
        .withIndexFunction("identity")
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), indexName))
        .withSourceFields(new ArrayList<>(columnsMap.keySet()))
        .withIndexOptions(Collections.emptyMap())
        .build();
    metaClient.buildIndexDefinition(indexDefinition);
    assertTrue(metaClient.getIndexMetadata().isPresent());
    assertTrue(metaClient.getIndexForMetadataPartition(indexName).isPresent());
    assertTrue(metaClient.getStorage().exists(new StoragePath(metaClient.getIndexDefinitionPath())));
    metaClient.deleteIndexDefinition(indexName);
    assertFalse(metaClient.getIndexMetadata().isPresent());
    assertTrue(metaClient.getStorage().exists(new StoragePath(metaClient.getIndexDefinitionPath())));
    // Read from storage
    HoodieIndexMetadata indexMetadata = HoodieIndexMetadata.fromJson(
        new String(FileIOUtils.readDataFromPath(metaClient.getStorage(), new StoragePath(metaClient.getIndexDefinitionPath())).get()));
    assertTrue(indexMetadata.getIndexDefinitions().isEmpty());
  }

  @Test
  void testReadIndexDefFromStorage() throws Exception {
    final String basePath = tempDir.toAbsolutePath() + Path.SEPARATOR + "t8";

    // No index definition path configured - should return empty
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("table")
        .initTable(this.metaClient.getStorageConf(), basePath);

    Method readIndexDefMethod = HoodieTableMetaClient.class
        .getDeclaredMethod("readIndexDefFromStorage",
            org.apache.hudi.storage.HoodieStorage.class,
            StoragePath.class,
            HoodieTableConfig.class);
    readIndexDefMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
    assertTrue(result.isEmpty(), "Should return empty when no index definition path is configured");

    // Empty index definition path - should return empty
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key(), "");
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result2 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
    assertTrue(result2.isEmpty(), "Should return empty when index definition path is empty string");

    // Valid path but file doesn't exist - should return empty HoodieIndexMetadata
    String relativePath = ".hoodie/.index_defs/index.json";
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key(), relativePath);
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result3 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
    assertTrue(result3.isPresent(), "Should return present Option when path is configured but file doesn't exist");
    assertTrue(result3.get().getIndexDefinitions().isEmpty(), "Should return empty HoodieIndexMetadata when file doesn't exist");

    // Valid path with existing empty file - should return empty HoodieIndexMetadata
    StoragePath indexPath = new StoragePath(metaClient.getBasePath(), relativePath);
    FileIOUtils.createFileInPath(metaClient.getStorage(), indexPath,
        Option.of(HoodieInstantWriter.convertByteArrayToWriter("{}".getBytes())));
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result4 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
    assertTrue(result4.isPresent(), "Should return present Option when file exists");
    assertTrue(result4.get().getIndexDefinitions().isEmpty(), "Should return empty HoodieIndexMetadata for empty file");

    // Valid path with valid index metadata - should return populated HoodieIndexMetadata
    Map<String, Map<String, String>> columnsMap = new HashMap<>();
    columnsMap.put("c1", Collections.emptyMap());
    String indexName = MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath() + "test_idx";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType("column_stats")
        .withIndexFunction("identity")
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), indexName))
        .withSourceFields(new ArrayList<>(columnsMap.keySet()))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefMap = new HashMap<>();
    indexDefMap.put(indexName, indexDefinition);
    HoodieIndexMetadata validIndexMetadata = new HoodieIndexMetadata(indexDefMap);

    FileIOUtils.createFileInPath(metaClient.getStorage(), indexPath,
        Option.of(HoodieInstantWriter.convertByteArrayToWriter(validIndexMetadata.toJson().getBytes())));
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result5 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
    assertTrue(result5.isPresent(), "Should return present Option when valid file exists");
    assertFalse(result5.get().getIndexDefinitions().isEmpty(), "Should return populated HoodieIndexMetadata");
    assertEquals(1, result5.get().getIndexDefinitions().size(), "Should have one index definition");
    assertTrue(result5.get().getIndexDefinitions().containsKey(indexName), "Should contain the test index");
    assertEquals("column_stats", result5.get().getIndexDefinitions().get(indexName).getIndexType(), "Index type should match");

    // Invalid JSON file - should throw HoodieIOException
    FileIOUtils.createFileInPath(metaClient.getStorage(), indexPath,
        Option.of(HoodieInstantWriter.convertByteArrayToWriter("invalid json".getBytes())));
    assertThrows(HoodieIOException.class, () -> {
      try {
        readIndexDefMethod.invoke(null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig());
      } catch (java.lang.reflect.InvocationTargetException e) {
        if (e.getCause() instanceof HoodieIOException) {
          throw (HoodieIOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }, "Should throw HoodieIOException for invalid JSON");
  }
}
