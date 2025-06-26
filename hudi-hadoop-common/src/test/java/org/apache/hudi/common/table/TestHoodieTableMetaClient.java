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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.hudi.common.table.HoodieTableMetaClient.loadIndexDefFromStorage;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie table meta client {@link HoodieTableMetaClient}.
 */
public class TestHoodieTableMetaClient extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void checkMetadata() {
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
  public void testSerDe() throws IOException {
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
  public void testCommitTimeline() throws IOException {
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
  public void testEquals() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1, metaClient2);
    assertNotEquals(metaClient1, null);
    assertNotEquals(metaClient1, new Object());
  }

  @Test
  public void testToString() throws IOException {
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType());
    assertEquals(metaClient1.toString(), metaClient2.toString());
    assertNotEquals(metaClient1.toString(), new Object().toString());
  }

  @Test
  public void testTableVersion() throws IOException {
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
  public void testGenerateFromAnotherMetaClient() throws IOException {
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
  public void testTableBuilderRequiresTableNameAndType() {
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
  public void testCreateMetaClientFromProperties() throws IOException {
    final String basePath = tempDir.toAbsolutePath().toString() + Path.SEPARATOR + "t5";
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), "test-table");
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    props.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");

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
    assertEquals(metaClient1.getTableConfig().getPreCombineField(), metaClient2.getTableConfig().getPreCombineField());
    // default table version should be current version
    assertEquals(HoodieTableVersion.current(), metaClient2.getTableConfig().getTableVersion());
  }

  @Test
  public void testCreateLayoutInStorage() throws IOException {
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
  public void testGetIndexDefinitionPath() throws IOException {
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
  public void testDeleteDefinition() throws IOException {
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
    assertTrue(metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(indexName));
    assertTrue(metaClient.getStorage().exists(new StoragePath(metaClient.getIndexDefinitionPath())));
    metaClient.deleteIndexDefinition(indexName);
    assertTrue(metaClient.getIndexMetadata().isEmpty());
    assertTrue(metaClient.getStorage().exists(new StoragePath(metaClient.getIndexDefinitionPath())));
    // Read from storage
    HoodieIndexMetadata indexMetadata = HoodieIndexMetadata.fromJson(
        new String(FileIOUtils.readDataFromPath(metaClient.getStorage(), new StoragePath(metaClient.getIndexDefinitionPath())).get()));
    assertTrue(indexMetadata.getIndexDefinitions().isEmpty());
  }

  @Test
  public void testIndexJsonFileMissingVersionField() {
    // Json file with no version attribute
    HoodieIndexMetadata loadedDef = loadIndexDefFromStorage(
        new StoragePath(Objects.requireNonNull(getClass().getClassLoader().getResource("indexMissingVersion1.json")).toString()), "",
        metaClient.getStorage()).get();
    assertEquals(2, loadedDef.getIndexDefinitions().size());
    validateAllFieldsExcludingVersion(loadedDef);
    // The populated definition object should use null.
    assertNull(loadedDef.getIndexDefinitions().get("column_stats").getVersion());
    assertNull(loadedDef.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
  }

  @Test
  public void testPopulateVersionFieldIfMissing() {
    Function<String, HoodieIndexMetadata> getIndexDef = (idxFileName) ->
        loadIndexDefFromStorage(
           new StoragePath(Objects.requireNonNull(getClass().getClassLoader().getResource(idxFileName)).toString()), "",
           metaClient.getStorage()).get();
    HoodieIndexMetadata loadedDef = getIndexDef.apply("indexMissingVersion1.json");
    assertEquals(2, loadedDef.getIndexDefinitions().size());
    // Apply the function fixing the missing version field
    // Table version 9 with missing version field is not acceptable for secondary index as it should always write the version field.
    assertThrows(IllegalArgumentException.class, () -> HoodieTableMetaClient.populateIndexVersionIfMissing(HoodieTableVersion.NINE,
        Option.of(getIndexDef.apply("indexMissingVersion1.json"))));

    // If it is table version 8, secondary index def missing version field will be fixed.
    HoodieIndexMetadata loadedDef2 = getIndexDef.apply("indexMissingVersion1.json");
    HoodieTableMetaClient.populateIndexVersionIfMissing(HoodieTableVersion.EIGHT, Option.of(loadedDef2));

    assertEquals(HoodieIndexVersion.COLUMN_STATS_ONE, loadedDef2.getIndexDefinitions().get("column_stats").getVersion());
    assertEquals(HoodieIndexVersion.SECONDARY_INDEX_ONE, loadedDef2.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
    validateAllFieldsExcludingVersion(loadedDef2);

    // If it is table version 9 and only non secondary index index missing version attribute
    HoodieIndexMetadata loadedDef3 = getIndexDef.apply("indexMissingVersion2.json");
    HoodieTableMetaClient.populateIndexVersionIfMissing(HoodieTableVersion.NINE, Option.of(loadedDef3));

    assertEquals(HoodieIndexVersion.COLUMN_STATS_ONE, loadedDef3.getIndexDefinitions().get("column_stats").getVersion());
    assertEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO, loadedDef3.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
    validateAllFieldsExcludingVersion(loadedDef3);
  }

  private static void validateAllFieldsExcludingVersion(HoodieIndexMetadata loadedDef) {
    HoodieIndexDefinition colStatsDef = loadedDef.getIndexDefinitions().get("column_stats");
    assertEquals("column_stats", colStatsDef.getIndexName());
    assertEquals("column_stats", colStatsDef.getIndexType());
    assertEquals(Collections.emptyMap(), colStatsDef.getIndexOptions());
    assertEquals(Arrays.asList(
        "_hoodie_commit_time",
        "_hoodie_partition_path",
        "_hoodie_record_key",
        "key",
        "secKey",
        "partition",
        "intField",
        "city",
        "textField1",
        "textField2",
        "textField3",
        "textField4",
        "decimalField",
        "longField",
        "incrLongField",
        "round"), colStatsDef.getSourceFields());

    HoodieIndexDefinition secIdxDef = loadedDef.getIndexDefinitions().get("secondary_index_idx_price");
    assertEquals("secondary_index_idx_price", secIdxDef.getIndexName());
    assertEquals("secondary_index", secIdxDef.getIndexType());
    assertEquals("identity", secIdxDef.getIndexFunction());
    assertEquals(Collections.singletonList("price"), secIdxDef.getSourceFields());
    assertEquals(Collections.emptyMap(), secIdxDef.getIndexOptions());
  }
}
