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

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_WRITE_TOKEN;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.DATA_GEN;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestFlinkRowDataReaderContext {
  private final StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
      ));

  private FlinkRowDataReaderContext readerContext;

  @BeforeEach
  void setUp() {
    List<ExpressionPredicates.Predicate> predicates = new ArrayList<>();
    when(tableConfig.isRecordKeyPopulated()).thenReturn(true);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"id"}));
    readerContext = new FlinkRowDataReaderContext(
        storageConfig, null, predicates, tableConfig, Option.empty());
  }

  @Test
  void testConstructEngineRecordWithFieldValues() {
    Object[] fieldVals = new Object[] {1, StringData.fromString("Alice"), true};
    RowData row = readerContext.getRecordContext().constructEngineRecord(SCHEMA, fieldVals);
    assertEquals(fieldVals[0], row.getInt(0));
    assertEquals(fieldVals[1], row.getString(1));
    assertEquals(fieldVals[2], row.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNoUpdates() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(1, result.getInt(0));
    assertEquals("Alice", result.getString(1).toString());
    assertTrue(result.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithUpdateOneField() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(1, StringData.fromString("Bob"));

    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(1, result.getInt(0)); // unchanged
    assertEquals("Bob", result.getString(1).toString()); // updated
    assertTrue(result.getBoolean(2)); // unchanged
  }

  @Test
  void testConstructEngineRecordWithUpdateAllFields() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, 42);
    updates.put(1, StringData.fromString("Zoe"));
    updates.put(2, false);
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(42, result.getInt(0));
    assertEquals("Zoe", result.getString(1).toString());
    assertFalse(result.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNullUpdate() {
    RowData base = createBaseRow(5, "Eve", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(1, null);
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(5, result.getInt(0));
    assertTrue(result.isNullAt(1));
    assertTrue(result.getBoolean(2));
  }

  @Nested
  class NativeLogFilters {
    private static final String PARTITION = "partition";
    private static final String FILE_ID = "file-id";

    @TempDir
    Path tempDir;

    private HoodieTableMetaClient metaClient;
    private HoodieStorage storage;
    private StoragePath partitionPath;

    @BeforeEach
    void setUpNativeLogs() throws Exception {
      Properties properties = new Properties();
      properties.setProperty(HoodieTableConfig.VERSION.key(), "10");
      properties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
      properties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), ROW_KEY);
      properties.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
      properties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
      metaClient = HoodieTestUtils.init(new HadoopStorageConfiguration(new Configuration()), tempDir.toString(),
          HoodieTableType.MERGE_ON_READ, properties);
      assertEquals(HoodieTableVersion.TEN, metaClient.getTableConfig().getTableVersion());
      storage = metaClient.getStorage();
      partitionPath = new StoragePath(metaClient.getBasePath(), PARTITION);
      storage.createDirectory(partitionPath);
    }

    @Test
    void testPrimaryKeyFilterDoesNotResurrectDeletedRecord() throws Exception {
      HoodieBaseFile baseFile = HoodieFileSliceTestUtils.createBaseFile(
          new StoragePath(partitionPath, FSUtils.makeBaseFileName("001", DEFAULT_WRITE_TOKEN, FILE_ID, ".parquet")).toString(),
          Arrays.asList(record("k", 100L), record("survivor", 100L)), HOODIE_SCHEMA, "001");
      HoodieLogFile deleteFile = createDeleteFile();
      assertTrue(FSUtils.isNativeDeleteLogFile(deleteFile.getPath().getName()));
      // Establish that the physical delete schema does not contain the business key column.
      try (HoodieRowDataParquetReader reader = new HoodieRowDataParquetReader(storage, deleteFile.getPath())) {
        assertEquals(Arrays.asList("_hoodie_record_key", "timestamp"),
            reader.getSchema().getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList()));
      }
      assertEquals(Collections.singletonList("survivor"), readSnapshot(baseFile, deleteFile, null));
      assertEquals(Collections.singletonList("survivor"), readSnapshot(baseFile, deleteFile, "survivor"));
      assertEquals(Collections.emptyList(), readSnapshot(baseFile, deleteFile, "k"));
    }

    @Test
    void testStoredRecordKeyPredicatePrunesDeleteRowGroups() throws Exception {
      HoodieLogFile deleteFile = createDeleteFile();
      assertEquals(Collections.singletonList("k"), readDeleteKeys(deleteFile, keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "k")));
      assertEquals(Collections.emptyList(), readDeleteKeys(deleteFile, keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "other")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.apache.hudi.table.format.TestFlinkRowDataReaderContext#deleteLogPredicates")
    void testDeleteLogPredicateSafety(ExpressionPredicates.Predicate predicate, List<String> expectedKeys) throws Exception {
      assertEquals(expectedKeys, readDeleteKeys(createDeleteFile(), predicate));
    }

    @Test
    void testNativeDataLogRetainsPrimaryKeyPushdown() throws Exception {
      HoodieLogFile dataFile = HoodieFileSliceTestUtils.createNativeDataLogFile(
          nativeLogPath(".log.parquet"), Collections.singletonList(record("other", 100L)), HOODIE_SCHEMA, "002");
      FlinkRowDataReaderContext context = readerContext("k");
      context.setHasLogFiles(true);
      try (ClosableIterator<RowData> iterator = context.getFileRecordIterator(dataFile.getPath(), 0, Long.MAX_VALUE,
          HOODIE_SCHEMA, HOODIE_SCHEMA, storage)) {
        assertFalse(iterator.hasNext(), "Primary key pushdown should still prune nonmatching data log row groups");
      }
    }

    private HoodieLogFile createDeleteFile() throws Exception {
      return HoodieFileSliceTestUtils.createNativeDeleteLogFile(storage,
          nativeLogPath(".deletes.parquet"), Collections.singletonList(record("k", 200L)), HOODIE_SCHEMA, "002");
    }

    private List<String> readDeleteKeys(HoodieLogFile deleteFile, ExpressionPredicates.Predicate predicate) throws Exception {
      HoodieSchema schema = HoodieSchemaUtils.createDeleteLogSchema(HOODIE_SCHEMA, Collections.singletonList("timestamp"));
      FlinkRowDataReaderContext context = readerContext(Collections.singletonList(predicate));
      context.setHasLogFiles(true);
      List<String> keys = new ArrayList<>();
      try (ClosableIterator<RowData> iterator = context.getFileRecordIterator(deleteFile.getPath(), 0, Long.MAX_VALUE,
          schema, schema, storage)) {
        while (iterator.hasNext()) {
          keys.add(iterator.next().getString(0).toString());
        }
      }
      return keys;
    }

    private List<String> readSnapshot(HoodieBaseFile baseFile, HoodieLogFile deleteFile, String key) throws Exception {
      HoodieFileGroupReader<RowData> reader = HoodieFileGroupReader.<RowData>builder()
          .withReaderContext(readerContext(key))
          .withHoodieTableMetaClient(metaClient)
          .withStorage(storage)
          .withLatestCommitTime("002")
          .withBaseFileOption(Option.of(baseFile))
          .withLogFiles(Collections.singletonList(deleteFile).stream())
          .withPartitionPath(PARTITION)
          .withDataSchema(HOODIE_SCHEMA)
          .withRequestedSchema(HOODIE_SCHEMA)
          .withProps(TypedProperties.copy(metaClient.getTableConfig().getProps()))
          .withShouldUseRecordPosition(false)
          .build();
      List<String> keys = new ArrayList<>();
      try (ClosableIterator<RowData> iterator = reader.getClosableIterator()) {
        while (iterator.hasNext()) {
          String recordKey = iterator.next().getString(HOODIE_SCHEMA.getField(ROW_KEY).get().pos()).toString();
          // Parquet pushdown prunes row groups; the SQL engine evaluates the predicate after merging.
          if (key == null || key.equals(recordKey)) {
            keys.add(recordKey);
          }
        }
      }
      return keys;
    }

    private FlinkRowDataReaderContext readerContext(String key) {
      return readerContext(key == null ? Collections.emptyList() : Collections.singletonList(keyEquals(ROW_KEY, key)));
    }

    private FlinkRowDataReaderContext readerContext(List<ExpressionPredicates.Predicate> predicates) {
      return new FlinkRowDataReaderContext(storage.getConf(),
          () -> InternalSchemaManager.DISABLED, predicates, metaClient.getTableConfig(), Option.empty());
    }

    private String nativeLogPath(String extension) {
      return new StoragePath(partitionPath, FILE_ID + "_" + DEFAULT_WRITE_TOKEN + "_002_1" + extension).toString();
    }

    private IndexedRecord record(String key, long timestamp) {
      return DATA_GEN.generateGenericRecord(key, PARTITION, "rider." + key, "driver." + key, timestamp, false, false);
    }
  }

  private static Stream<Arguments> deleteLogPredicates() {
    ExpressionPredicates.Predicate missingKey = keyEquals(ROW_KEY, "k");
    ExpressionPredicates.Predicate storedKey = keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "other");
    List<String> retainedKeys = Collections.singletonList("k");
    return Stream.of(
        Arguments.of(ExpressionPredicates.Or.getInstance().bindPredicates(missingKey, storedKey), retainedKeys),
        Arguments.of(ExpressionPredicates.Or.getInstance().bindPredicates(storedKey, missingKey), retainedKeys),
        Arguments.of(ExpressionPredicates.Not.getInstance().bindPredicate(
            ExpressionPredicates.And.getInstance().bindPredicates(keyEquals(ROW_KEY, "other"),
                keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "k"))), retainedKeys),
        Arguments.of(ExpressionPredicates.Or.getInstance().bindPredicates(
            keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "a"), keyEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, "b")), Collections.emptyList()),
        // Physical presence does not make a non-key ordering predicate safe for MOR.
        Arguments.of(ExpressionPredicates.Equals.getInstance()
            .bindValueLiteral(new ValueLiteralExpression(300L))
            .bindFieldReference(new FieldReferenceExpression("timestamp", DataTypes.BIGINT(), 0, 1)), retainedKeys));
  }

  private static ExpressionPredicates.Predicate keyEquals(String field, String key) {
    return ExpressionPredicates.Equals.getInstance()
        .bindValueLiteral(new ValueLiteralExpression(key))
        .bindFieldReference(new FieldReferenceExpression(field, DataTypes.STRING(), 0, 0));
  }

  private GenericRowData createBaseRow(int id, String name, boolean active) {
    return GenericRowData.of(id, StringData.fromString(name), active);
  }
}
