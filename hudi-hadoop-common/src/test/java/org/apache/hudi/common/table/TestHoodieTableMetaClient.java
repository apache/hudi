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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
            HoodieTableConfig.class,
            Option.class);
    readIndexDefMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
    assertTrue(result.isEmpty(), "Should return empty when no index definition path is configured");

    // Empty index definition path - should return empty
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key(), "");
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result2 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
    assertTrue(result2.isEmpty(), "Should return empty when index definition path is empty string");

    // Valid path but file doesn't exist - should return empty HoodieIndexMetadata
    String relativePath = ".hoodie/.index_defs/index.json";
    metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key(), relativePath);
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result3 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
    assertTrue(result3.isPresent(), "Should return present Option when path is configured but file doesn't exist");
    assertTrue(result3.get().getIndexDefinitions().isEmpty(), "Should return empty HoodieIndexMetadata when file doesn't exist");

    // Valid path with existing empty file - should return empty HoodieIndexMetadata
    StoragePath indexPath = new StoragePath(metaClient.getBasePath(), relativePath);
    FileIOUtils.createFileInPath(metaClient.getStorage(), indexPath,
        Option.of(HoodieInstantWriter.convertByteArrayToWriter("{}".getBytes())));
    @SuppressWarnings("unchecked")
    Option<HoodieIndexMetadata> result4 = (Option<HoodieIndexMetadata>) readIndexDefMethod.invoke(
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
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
        null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
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
        readIndexDefMethod.invoke(null, metaClient.getStorage(), metaClient.getBasePath(), metaClient.getTableConfig(), Option.empty());
      } catch (java.lang.reflect.InvocationTargetException e) {
        if (e.getCause() instanceof HoodieIOException) {
          throw (HoodieIOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }, "Should throw HoodieIOException for invalid JSON");
  }

  @Test
  void testIsTimestampMillisField() {
    // Test timestamp-millis
    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    assertTrue(HoodieTableMetaClient.isTimestampMillisField(timestampMillisSchema),
        "Should return true for timestamp-millis");

    // Test local-timestamp-millis
    Schema localTimestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(localTimestampMillisSchema);
    assertTrue(HoodieTableMetaClient.isTimestampMillisField(localTimestampMillisSchema),
        "Should return true for local-timestamp-millis");

    // Test nullable timestamp-millis
    Schema nullableTimestampMillisSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        timestampMillisSchema);
    assertTrue(HoodieTableMetaClient.isTimestampMillisField(nullableTimestampMillisSchema),
        "Should return true for nullable timestamp-millis");

    // Test timestamp-micros (should return false)
    Schema timestampMicrosSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    assertFalse(HoodieTableMetaClient.isTimestampMillisField(timestampMicrosSchema),
        "Should return false for timestamp-micros");

    // Test regular long (should return false)
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertFalse(HoodieTableMetaClient.isTimestampMillisField(longSchema),
        "Should return false for regular long");

    // Test string (should return false)
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertFalse(HoodieTableMetaClient.isTimestampMillisField(stringSchema),
        "Should return false for string");
  }

  @Test
  void testGetTimestampMillisColumns() {
    // Test null schema
    assertTrue(HoodieTableMetaClient.getTimestampMillisColumns(null).isEmpty(),
        "Should return empty list for null schema");
    assertTrue(HoodieTableMetaClient.getTimestampMillisColumns(Option.empty()).isEmpty(),
        "Should return empty list for empty option");

    // Test schema with timestamp-millis field
    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    Schema.Field timestampMillisField =
        new Schema.Field("ts_millis", timestampMillisSchema, null, null);
    Schema recordSchema1 = Schema.createRecord("TestRecord", null, null, false);
    recordSchema1.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        timestampMillisField,
        new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)
    ));
    List<String> result2 = HoodieTableMetaClient.getTimestampMillisColumns(Option.of(recordSchema1));
    assertEquals(1, result2.size(), "Should return one timestamp_millis column");
    assertTrue(result2.contains("ts_millis"), "Should contain ts_millis column");

    // Test schema with local-timestamp-millis field
    Schema localTimestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(localTimestampMillisSchema);
    Schema.Field localTimestampMillisField =
        new Schema.Field("local_ts_millis", localTimestampMillisSchema, null, null);
    Schema recordSchema2 = Schema.createRecord("TestRecord2", null, null, false);
    recordSchema2.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        localTimestampMillisField
    ));
    List<String> result3 = HoodieTableMetaClient.getTimestampMillisColumns(Option.of(recordSchema2));
    assertEquals(1, result3.size(), "Should return one local-timestamp-millis column");
    assertTrue(result3.contains("local_ts_millis"), "Should contain local_ts_millis column");

    // Test schema with nullable timestamp-millis
    Schema nullableTimestampMillisSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        timestampMillisSchema);
    Schema.Field nullableTimestampMillisField = new Schema.Field("nullable_ts_millis",
        nullableTimestampMillisSchema, null, null);
    Schema recordSchema4 = Schema.createRecord("TestRecord4", null, null, false);
    recordSchema4.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        nullableTimestampMillisField
    ));
    List<String> result5 = HoodieTableMetaClient.getTimestampMillisColumns(Option.of(recordSchema4));
    assertEquals(1, result5.size(), "Should return one nullable timestamp_millis column");
    assertTrue(result5.contains("nullable_ts_millis"), "Should contain nullable_ts_millis column");

    // Test schema without timestamp-millis fields
    Schema recordSchema5 = Schema.createRecord("TestRecord5", null, null, false);
    recordSchema5.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)
    ));
    List<String> result6 = HoodieTableMetaClient.getTimestampMillisColumns(Option.of(recordSchema5));
    assertTrue(result6.isEmpty(), "Should return empty list for schema without timestamp_millis fields");

    // Test non-RECORD schema
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    List<String> result7 = HoodieTableMetaClient.getTimestampMillisColumns(Option.of(stringSchema));
    assertTrue(result7.isEmpty(), "Should return empty list for non-RECORD schema");
  }

  @Test
  void testDisableV1ColumnStatsForTimestampMillisColumns() {
    // Create a schema with a timestamp_millis column
    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    Schema.Field timestampMillisField = new Schema.Field("ts_millis", timestampMillisSchema, null, null);
    Schema recordSchema = Schema.createRecord("TestRecord", null, null, false);
    recordSchema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        timestampMillisField,
        new Schema.Field("regular_col", Schema.create(Schema.Type.INT), null, null)
    ));

    // Create a Lazy<Option<Schema>> with the schema
    Lazy<Option<Schema>> lazySchemaOpt = Lazy.lazily(() -> Option.of(recordSchema));

    // Test case 1: V1 column stats index with timestamp_millis column - should filter it out
    HoodieIndexDefinition v1ColStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(java.util.Arrays.asList("ts_millis", "regular_col"))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefs = new HashMap<>();
    indexDefs.put(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, v1ColStatsDef);
    HoodieIndexMetadata originalMetadata = new HoodieIndexMetadata(indexDefs);

    HoodieIndexMetadata result1 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata, lazySchemaOpt);
    HoodieIndexDefinition filteredDef = result1.getIndexDefinitions()
        .get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    assertNotNull(filteredDef, "Index should still exist");
    assertEquals(1, filteredDef.getSourceFields().size());
    assertTrue(filteredDef.getSourceFields().contains("regular_col"));
    assertFalse(filteredDef.getSourceFields().contains("ts_millis"));

    // Test case 2: V1 column stats index with only timestamp_millis columns - should remove index
    HoodieIndexDefinition v1ColStatsDefOnlyTs = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(java.util.Arrays.asList("ts_millis"))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefs2 = new HashMap<>();
    indexDefs2.put(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, v1ColStatsDefOnlyTs);
    HoodieIndexMetadata originalMetadata2 = new HoodieIndexMetadata(indexDefs2);

    HoodieIndexMetadata result2 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata2, lazySchemaOpt);
    assertFalse(result2.getIndexDefinitions().containsKey(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS),
        "Index should be removed when all columns are timestamp_millis");

    // Test case 3: V2 column stats index - should not be filtered
    HoodieIndexDefinition v2ColStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.V2)
        .withSourceFields(java.util.Arrays.asList("ts_millis", "regular_col"))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefs3 = new HashMap<>();
    indexDefs3.put(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, v2ColStatsDef);
    HoodieIndexMetadata originalMetadata3 = new HoodieIndexMetadata(indexDefs3);

    HoodieIndexMetadata result3 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata3, lazySchemaOpt);
    HoodieIndexDefinition v2Def = result3.getIndexDefinitions()
        .get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    assertEquals(2, v2Def.getSourceFields().size(), "V2 index should not be filtered");
    assertTrue(v2Def.getSourceFields().contains("ts_millis"), "V2 index should contain ts_millis");

    // Test case 4: Non-column-stats index - should not be filtered
    String expressionIndexName = MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath() + "test_idx";
    HoodieIndexDefinition expressionIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(expressionIndexName)
        .withIndexType("expression_index")
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(java.util.Arrays.asList("ts_millis", "regular_col"))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefs4 = new HashMap<>();
    indexDefs4.put(expressionIndexName, expressionIndexDef);
    HoodieIndexMetadata originalMetadata4 = new HoodieIndexMetadata(indexDefs4);

    // Use a lazy schema that tracks if it's been evaluated
    boolean[] schemaEvaluated = {false};
    Lazy<Option<Schema>> lazyMockSchema = Lazy.lazily(() -> {
      schemaEvaluated[0] = true;
      return Option.of(mock(Schema.class));
    });

    HoodieIndexMetadata result4 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata4, lazyMockSchema);
    HoodieIndexDefinition exprDef = result4.getIndexDefinitions().get(expressionIndexName);
    assertEquals(2, exprDef.getSourceFields().size());
    assertTrue(exprDef.getSourceFields().contains("ts_millis"));

    // Assert that the lazy schema was not evaluated for non-column-stats index
    assertFalse(schemaEvaluated[0], "Schema should not be evaluated for non-column-stats indexes");

    // Test case 5: Empty timestamp_millis columns (schema with no timestamp_millis) - should not filter anything
    Schema schemaWithoutTimestampMillis = Schema.createRecord("TestRecord2", null, null, false);
    schemaWithoutTimestampMillis.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("regular_col", Schema.create(Schema.Type.INT), null, null)
    ));
    Lazy<Option<Schema>> lazySchemaOptNoTs = Lazy.lazily(() -> Option.of(schemaWithoutTimestampMillis));

    HoodieIndexMetadata result5 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata, lazySchemaOptNoTs);
    HoodieIndexDefinition unchangedDef = result5.getIndexDefinitions()
        .get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    assertEquals(2, unchangedDef.getSourceFields().size(),
        "Should not filter when schema has no timestamp_millis columns");

    // Test case 6: Test with partition stats index
    HoodieIndexDefinition v1PartStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS)
        .withVersion(HoodieIndexVersion.V1)
        .withSourceFields(java.util.Arrays.asList("ts_millis", "regular_col"))
        .withIndexOptions(Collections.emptyMap())
        .build();

    Map<String, HoodieIndexDefinition> indexDefs6 = new HashMap<>();
    indexDefs6.put(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, v1PartStatsDef);
    HoodieIndexMetadata originalMetadata6 = new HoodieIndexMetadata(indexDefs6);

    HoodieIndexMetadata result6 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata6, lazySchemaOpt);
    HoodieIndexDefinition filteredPartStatsDef = result6.getIndexDefinitions()
        .get(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);
    assertNotNull(filteredPartStatsDef, "Partition stats index should still exist");
    assertEquals(1, filteredPartStatsDef.getSourceFields().size());
    assertTrue(filteredPartStatsDef.getSourceFields().contains("regular_col"));
    assertFalse(filteredPartStatsDef.getSourceFields().contains("ts_millis"));

    // Test case 7: Test with empty Option<Schema> - should not filter anything
    Lazy<Option<Schema>> lazyEmptySchemaOpt = Lazy.lazily(Option::empty);

    HoodieIndexMetadata result7 = HoodieTableMetaClient.disableV1ColumnStatsForTimestampMillisColumns(
        originalMetadata, lazyEmptySchemaOpt);
    HoodieIndexDefinition unchangedDef2 = result7.getIndexDefinitions()
        .get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    assertEquals(2, unchangedDef2.getSourceFields().size(),
        "Should not filter when schema Option is empty");
  }

  @Test
  void testGetLatestTableSchema() throws IOException {
    final String basePath = tempDir.toAbsolutePath() + Path.SEPARATOR + "t9";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName("table")
        .initTable(this.metaClient.getStorageConf(), basePath);

    // Test with table that has no commits - should fallback to table create schema
    Option<Schema> result1 = metaClient.getLatestTableSchema(metaClient.getTableConfig());
    // Should return empty or fallback to create schema
    assertNotNull(result1, "Should return Option (may be empty)");

    // Test with table that has schema in table config
    Schema testSchema = Schema.createRecord("TestRecord", null, null, false);
    testSchema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)
    ));
    metaClient.getTableConfig().setValue(HoodieTableConfig.CREATE_SCHEMA.key(), testSchema.toString());
    Option<Schema> result2 = metaClient.getLatestTableSchema(metaClient.getTableConfig());
    // Should return schema from table config as fallback
    assertNotNull(result2, "Should return Option");
  }
}
