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

package org.apache.hudi.table;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.CustomPayloadForTesting;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderBase;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.utils.TestData;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests {@code HoodieFileGroupReader} with {@code FlinkRowDataReaderContext} on Flink.
 */
public class TestHoodieFileGroupReaderOnFlink extends TestHoodieFileGroupReaderBase<RowData> {
  private Configuration conf;
  private Option<InstantRange> instantRangeOpt = Option.empty();

  @BeforeEach
  public void setup() {
    conf = new Configuration();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.PATH, getBasePath());
    conf.set(FlinkOptions.TABLE_NAME, "TestHoodieTable");
    // use hive style partition as a workaround for HUDI-9396
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
  }

  @Override
  public StorageConfiguration<?> getStorageConf() {
    return new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf));
  }

  @Override
  public String getBasePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }

  @Override
  public HoodieReaderContext<RowData> getHoodieReaderContext(
      String tablePath,
      Schema avroSchema,
      StorageConfiguration<?> storageConf,
      HoodieTableMetaClient metaClient) {
    return new FlinkRowDataReaderContext(
        storageConf,
        () -> InternalSchemaManager.DISABLED,
        Collections.emptyList(),
        metaClient.getTableConfig(),
        instantRangeOpt);
  }

  @Override
  public String getCustomPayload() {
    return CustomPayloadForTesting.class.getName();
  }

  @Override
  protected void readWithFileGroupReader(
      HoodieFileGroupReader<RowData> fileGroupReader,
      List<RowData> recordList,
      Schema recordSchema,
      HoodieReaderContext<RowData> readerContext,
      boolean sortOutput) throws IOException {
    RowDataSerializer rowDataSerializer = RowDataAvroQueryContexts.getRowDataSerializer(recordSchema);
    try (ClosableIterator<RowData> iterator = fileGroupReader.getClosableIterator()) {
      while (iterator.hasNext()) {
        RowData rowData = rowDataSerializer.copy(iterator.next());
        recordList.add(rowData);
      }
    }
  }

  @Override
  public void commitToTable(List<HoodieRecord> recordList, String operation, boolean firstCommit, Map<String, String> writeConfigs, String schemaStr) {
    writeConfigs.forEach((key, value) -> conf.setString(key, value));
    conf.set(FlinkOptions.PRECOMBINE_FIELDS, writeConfigs.get("hoodie.datasource.write.precombine.fields"));
    conf.set(FlinkOptions.OPERATION, operation);
    Schema localSchema = getRecordAvroSchema(schemaStr);
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, localSchema.toString());
    AvroToRowDataConverters.AvroToRowDataConverter avroConverter =
        RowDataAvroQueryContexts.fromAvroSchema(localSchema).getAvroToRowDataConverter();
    List<RowData> rowDataList = recordList.stream().map(record -> {
      try {
        return (RowData) avroConverter.convert(record.toIndexedRecord(localSchema, CollectionUtils.emptyProps()).get().getData());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    try {
      TestData.writeData(rowDataList, conf);
    } catch (Exception e) {
      throw new HoodieException("Failed to insert data", e);
    }
  }

  @Override
  public void assertRecordsEqual(Schema schema, RowData expected, RowData actual) {
    TestData.assertRowDataEquals(
        Collections.singletonList(actual),
        Collections.singletonList(expected),
        RowDataAvroQueryContexts.fromAvroSchema(schema).getRowType());
  }

  @Override
  public void assertRecordMatchesSchema(Schema schema, RowData record) {
    // TODO: Add support for RowData
  }

  @Override
  public HoodieTestDataGenerator.SchemaEvolutionConfigs getSchemaEvolutionConfigs() {
    HoodieTestDataGenerator.SchemaEvolutionConfigs configs = new HoodieTestDataGenerator.SchemaEvolutionConfigs();
    configs.nestedSupport = false;
    configs.arraySupport = false;
    configs.mapSupport = false;
    configs.anyArraySupport = false;
    configs.addNewFieldSupport = false;
    configs.intToLongSupport = false;
    configs.intToFloatSupport = false;
    configs.intToDoubleSupport = false;
    configs.intToStringSupport = false;
    configs.longToFloatSupport = false;
    configs.longToDoubleSupport = false;
    configs.longToStringSupport = false;
    configs.floatToDoubleSupport = false;
    configs.floatToStringSupport = false;
    configs.doubleToStringSupport = false;
    configs.stringToBytesSupport = false;
    configs.bytesToStringSupport = false;
    return configs;
  }

  @Test
  public void testGetOrderingValue() {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(getStorageConf(), () -> InternalSchemaManager.DISABLED, Collections.emptyList(), tableConfig, Option.empty());
    Schema schema = SchemaBuilder.builder()
        .record("test")
        .fields()
        .requiredString("field1")
        .optionalString("field2")
        .optionalLong("ts")
        .endRecord();
    GenericRowData rowData = GenericRowData.of(StringData.fromString("f1"), StringData.fromString("f2"), 1000L);
    assertEquals(1000L, readerContext.getRecordContext().getOrderingValue(rowData, schema, Collections.singletonList("ts")));
    assertEquals(OrderingValues.getDefault(), readerContext.getRecordContext().getOrderingValue(rowData, schema, Collections.singletonList("non_existent_col")));
  }

  @Test
  public void getRecordKeyFromMetadataFields() {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(getStorageConf(), () -> InternalSchemaManager.DISABLED, Collections.emptyList(), tableConfig, Option.empty());
    Schema schema = SchemaBuilder.builder()
        .record("test")
        .fields()
        .requiredString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .optionalString("field2")
        .endRecord();
    String key = "my_key";
    GenericRowData rowData = GenericRowData.of(StringData.fromString(key), StringData.fromString("field2_val"));
    assertEquals(key, readerContext.getRecordContext().getRecordKey(rowData, schema));
  }

  @Test
  public void getRecordKeySingleKey() {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"field1"}));
    FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(getStorageConf(), () -> InternalSchemaManager.DISABLED, Collections.emptyList(), tableConfig, Option.empty());
    Schema schema = SchemaBuilder.builder()
        .record("test")
        .fields()
        .requiredString("field1")
        .optionalString("field2")
        .endRecord();
    String key = "key";
    GenericRowData rowData = GenericRowData.of(StringData.fromString(key), StringData.fromString("other"));
    assertEquals(key, readerContext.getRecordContext().getRecordKey(rowData, schema));
  }

  @Test
  public void getRecordKeyWithMultipleKeys() {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"field1", "field2"}));
    FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(getStorageConf(), () -> InternalSchemaManager.DISABLED, Collections.emptyList(), tableConfig, Option.empty());

    Schema schema = SchemaBuilder.builder()
        .record("test")
        .fields()
        .requiredString("field1")
        .requiredString("field2")
        .requiredString("field3")
        .endRecord();
    String key = "field1:va1,field2:__empty__";
    GenericRowData rowData = GenericRowData.of(StringData.fromString("va1"), StringData.fromString(""), StringData.fromString("other"));
    assertEquals(key, readerContext.getRecordContext().getRecordKey(rowData, schema));
  }

  @ParameterizedTest
  @MethodSource("logFileOnlyCases")
  public void testReadLogFilesOnlyInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a log file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
      commitToTable(initialRecords, UPSERT.value(), true, writeConfigs, TRIP_EXAMPLE_SCHEMA);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 1, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing two log files
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      commitToTable(updates, UPSERT.value(), false, writeConfigs, TRIP_EXAMPLE_SCHEMA);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 2, recordMergeMode,
          allRecords, CollectionUtils.combine(initialRecords, updates));
    }
  }

  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"INSERT", "UPSERT"})
  public void testFilterFileWithInstantRange(WriteOperationType firstCommitOperation) throws Exception {
    RecordMergeMode recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    boolean isFirstCommitInsert = firstCommitOperation == INSERT;

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a log file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
      commitToTable(initialRecords, firstCommitOperation.value(), true, writeConfigs, TRIP_EXAMPLE_SCHEMA);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), isFirstCommitInsert,
          isFirstCommitInsert ? 0 : 1, recordMergeMode, initialRecords, initialRecords);

      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(getStorageConf(), getBasePath());
      String latestCompleteTime = metaClient.getActiveTimeline().getLatestCompletionTime().get();
      instantRangeOpt = Option.of(InstantRange.builder().startInstant(latestCompleteTime).rangeType(InstantRange.RangeType.OPEN_CLOSED).build());

      // the base/log file of the first commit is filtered out by the instant range.
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      commitToTable(updates, UPSERT.value(), false, writeConfigs, TRIP_EXAMPLE_SCHEMA);
      validateOutputFromFileGroupReader(getStorageConf(), getBasePath(),
          isFirstCommitInsert, isFirstCommitInsert ? 1 : 2, recordMergeMode, updates, updates);
      // reset instant range
      instantRangeOpt = Option.empty();
    }
  }

  private static Schema getRecordAvroSchema(String schemaStr) {
    Schema recordSchema = new Schema.Parser().parse(schemaStr);
    return AvroSchemaConverter.convertToSchema(RowDataAvroQueryContexts.fromAvroSchema(recordSchema).getRowType().getLogicalType());
  }
}
