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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.CustomPayloadForTesting;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderBase;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

/**
 * Tests {@code HoodieFileGroupReader} with {@code FlinkRowDataReaderContext} on Flink.
 */
public class TestHoodieFileGroupReaderOnFlink extends TestHoodieFileGroupReaderBase<RowData> {
  private Configuration conf;
  private static final Schema RECORD_SCHEMA = getRecordAvroSchema();
  private static final AvroToRowDataConverters.AvroToRowDataConverter AVRO_CONVERTER =
      RowDataAvroQueryContexts.fromAvroSchema(RECORD_SCHEMA).getAvroToRowDataConverter();

  @BeforeEach
  public void setup() {
    conf = new Configuration();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, RECORD_SCHEMA.toString());
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
        metaClient.getTableConfig());
  }

  @Override
  public String getCustomPayload() {
    return CustomPayloadForTesting.class.getName();
  }

  @Override
  protected void readWithFileGroupReader(
      HoodieFileGroupReader<RowData> fileGroupReader,
      List<RowData> recordList,
      Schema recordSchema) throws IOException {
    RowDataSerializer rowDataSerializer = RowDataAvroQueryContexts.getRowDataSerializer(recordSchema);
    fileGroupReader.initRecordIterators();
    while (fileGroupReader.hasNext()) {
      RowData rowData = rowDataSerializer.copy(fileGroupReader.next());
      recordList.add(rowData);
    }
  }

  @Override
  public void commitToTable(List<HoodieRecord> recordList, String operation, Map<String, String> writeConfigs) {
    writeConfigs.forEach((key, value) -> conf.setString(key, value));
    conf.set(FlinkOptions.OPERATION, operation);
    List<RowData> rowDataList = recordList.stream().map(record -> {
      try {
        return (RowData) AVRO_CONVERTER.convert(record.toIndexedRecord(RECORD_SCHEMA, CollectionUtils.emptyProps()).get().getData());
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

  @ParameterizedTest
  @MethodSource("logFileOnlyCases")
  public void testReadLogFilesOnlyInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a log file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
      commitToTable(initialRecords, UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 1, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing two log files
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      commitToTable(updates, UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 2, recordMergeMode,
          allRecords, CollectionUtils.combine(initialRecords, updates));
    }
  }

  private static Schema getRecordAvroSchema() {
    Schema recordSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
    return AvroSchemaConverter.convertToSchema(RowDataAvroQueryContexts.fromAvroSchema(recordSchema).getRowType().getLogicalType());
  }
}
