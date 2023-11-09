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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGER_STRATEGY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getLogFileListFromFileSlice;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieFileGroupReader} with different engines
 */
public abstract class TestHoodieFileGroupReaderBase<T> {
  @TempDir
  protected java.nio.file.Path tempDir;

  public abstract Configuration getHadoopConf();

  public abstract String getBasePath();

  public abstract HoodieReaderContext<T> getHoodieReaderContext(String tablePath, String[] partitionPath);

  public abstract void commitToTable(List<String> recordList, String operation,
                                     Map<String, String> writeConfigs);

  public abstract void validateRecordsInFileGroup(String tablePath,
                                                  List<T> actualRecordList,
                                                  Schema schema,
                                                  String fileGroupId);

  @ParameterizedTest
  @ValueSource(strings = {"avro", "parquet"})
  public void testReadFileGroupInMergeOnReadTable(String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs());
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      commitToTable(recordsToStrings(dataGen.generateInserts("001", 100)), INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), true, 0);

      // Two commits; reading one file group containing a base file and a log file
      commitToTable(recordsToStrings(dataGen.generateUpdates("002", 100)), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), true, 1);

      // Three commits; reading one file group containing a base file and two log files
      commitToTable(recordsToStrings(dataGen.generateUpdates("003", 100)), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), true, 2);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro", "parquet"})
  public void testReadLogFilesOnlyInMergeOnReadTable(String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs());
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);
    // Use InMemoryIndex to generate log only mor table
    writeConfigs.put("hoodie.index.type", "INMEMORY");

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      commitToTable(recordsToStrings(dataGen.generateInserts("001", 100)), INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), false, 1);

      // Two commits; reading one file group containing a base file and a log file
      commitToTable(recordsToStrings(dataGen.generateUpdates("002", 100)), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), false, 2);
    }
  }

  private Map<String, String> getCommonConfigs() {
    Map<String, String> configMapping = new HashMap<>();
    configMapping.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    configMapping.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    configMapping.put("hoodie.datasource.write.precombine.field", "timestamp");
    configMapping.put("hoodie.payload.ordering.field", "timestamp");
    configMapping.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "hoodie_test");
    configMapping.put("hoodie.insert.shuffle.parallelism", "4");
    configMapping.put("hoodie.upsert.shuffle.parallelism", "4");
    configMapping.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    configMapping.put("hoodie.delete.shuffle.parallelism", "1");
    configMapping.put("hoodie.merge.small.file.group.candidates.limit", "0");
    configMapping.put("hoodie.compact.inline", "false");
    return configMapping;
  }

  private void validateOutputFromFileGroupReader(Configuration hadoopConf,
                                                 String tablePath,
                                                 String[] partitionPaths,
                                                 boolean containsBaseFile,
                                                 int expectedLogFileNum) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf).setBasePath(tablePath).build();
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(hadoopConf);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(
        engineContext, metadataConfig, FileSystemViewStorageConfig.newBuilder().build(),
        HoodieCommonConfig.newBuilder().build(),
        mc -> HoodieTableMetadata.create(
            engineContext, metadataConfig, mc.getBasePathV2().toString()));
    SyncableFileSystemView fsView = viewManager.getFileSystemView(metaClient);
    FileSlice fileSlice = fsView.getAllFileSlices(partitionPaths[0]).findFirst().get();
    List<String> logFilePathList = getLogFileListFromFileSlice(fileSlice);
    Collections.sort(logFilePathList);
    assertEquals(expectedLogFileNum, logFilePathList.size());

    List<T> actualRecordList = new ArrayList<>();
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
    props.setProperty("hoodie.payload.ordering.field", "timestamp");
    props.setProperty(RECORD_MERGER_STRATEGY.key(), RECORD_MERGER_STRATEGY.defaultValue());
    if (metaClient.getTableConfig().contains(PARTITION_FIELDS)) {
      props.setProperty(PARTITION_FIELDS.key(), metaClient.getTableConfig().getString(PARTITION_FIELDS));
    }
    String[] partitionValues = partitionPaths[0].isEmpty() ? new String[] {} : new String[] {partitionPaths[0]};
    assertEquals(containsBaseFile, fileSlice.getBaseFile().isPresent());
    HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        getHoodieReaderContext(tablePath, partitionValues),
        hadoopConf,
        tablePath,
        metaClient.getActiveTimeline().lastInstant().get().getTimestamp(),
        fileSlice.getBaseFile(),
        logFilePathList.isEmpty() ? Option.empty() : Option.of(logFilePathList),
        avroSchema,
        props,
        0,
        fileSlice.getTotalFileSize(),
        false);
    fileGroupReader.initRecordIterators();
    while (fileGroupReader.hasNext()) {
      actualRecordList.add(fileGroupReader.next());
    }
    fileGroupReader.close();

    validateRecordsInFileGroup(tablePath, actualRecordList, avroSchema, fileSlice.getFileId());
  }
}
