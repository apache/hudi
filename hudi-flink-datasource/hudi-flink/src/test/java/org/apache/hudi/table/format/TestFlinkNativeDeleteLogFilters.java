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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_WRITE_TOKEN;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.DATA_GEN;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests predicate pushdown against actual native delete and data Parquet logs. */
class TestFlinkNativeDeleteLogFilters {
  private static final String PARTITION = "partition";
  private static final String FILE_ID = "file-id";

  @TempDir
  Path tempDir;

  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;
  private StoragePath partitionPath;

  @BeforeEach
  void setUp() throws Exception {
    Configuration hadoopConf = new Configuration();
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.VERSION.key(), "10");
    properties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), ROW_KEY);
    properties.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    properties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    metaClient = HoodieTestUtils.init(new HadoopStorageConfiguration(hadoopConf), tempDir.toString(),
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
    HoodieLogFile deleteFile = HoodieFileSliceTestUtils.createNativeDeleteLogFile(storage,
        nativeLogPath(".deletes.parquet"), Collections.singletonList(record("k", 200L)), HOODIE_SCHEMA, "002");
    assertTrue(FSUtils.isNativeDeleteLogFile(deleteFile.getPath().getName()));

    // Establish that this is an independent delete Parquet file without the business key column.
    try (HoodieRowDataParquetReader reader = new HoodieRowDataParquetReader(storage, deleteFile.getPath())) {
      assertEquals(Arrays.asList("_hoodie_record_key", "timestamp"), reader.getSchema().getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList()));
    }
    assertEquals(Collections.singletonList("survivor"), readSnapshot(baseFile, deleteFile, null));
    assertEquals(Collections.singletonList("survivor"), readSnapshot(baseFile, deleteFile, "survivor"));
    assertEquals(Collections.emptyList(), readSnapshot(baseFile, deleteFile, "k"));
  }

  @Test
  void testNativeDeleteScanRetainsTombstones() throws Exception {
    HoodieLogFile deleteFile = HoodieFileSliceTestUtils.createNativeDeleteLogFile(storage,
        nativeLogPath(".deletes.parquet"), Collections.singletonList(record("k", 200L)), HOODIE_SCHEMA, "002");
    HoodieSchema deleteSchema = HoodieSchemaUtils.createDeleteLogSchema(HOODIE_SCHEMA, Collections.singletonList("timestamp"));
    FlinkRowDataReaderContext context = readerContext("k");
    context.setHasLogFiles(true);
    try (ClosableIterator<RowData> iterator = context.getFileRecordIterator(deleteFile.getPath(), 0, Long.MAX_VALUE,
        deleteSchema, deleteSchema, storage)) {
      assertTrue(iterator.hasNext(), "The business primary key filter must not discard the tombstone");
      assertEquals("k", iterator.next().getString(0).toString());
      assertFalse(iterator.hasNext());
    }
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

  private List<String> readSnapshot(HoodieBaseFile baseFile, HoodieLogFile deleteFile,
                                    String key) throws Exception {
    FlinkRowDataReaderContext context = readerContext(key);
    HoodieFileGroupReader<RowData> reader = HoodieFileGroupReader.<RowData>builder()
        .withReaderContext(context)
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
    List<ExpressionPredicates.Predicate> predicates = Collections.emptyList();
    if (key != null) {
      ExpressionPredicates.Predicate predicate = ExpressionPredicates.Equals.getInstance()
          .bindValueLiteral(new ValueLiteralExpression(key))
          .bindFieldReference(new FieldReferenceExpression(ROW_KEY, DataTypes.STRING(), 0, HOODIE_SCHEMA.getField(ROW_KEY).get().pos()));
      predicates = Collections.singletonList(predicate);
    }
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
