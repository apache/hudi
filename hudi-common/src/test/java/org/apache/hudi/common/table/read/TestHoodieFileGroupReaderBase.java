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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
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

  public abstract HoodieReaderContext<T> getHoodieReaderContext();

  public abstract void commitToTable(List<String> recordList, String operation,
                                     Map<String, String> writeConfigs);

  public abstract void validateRecordsInFileGroup(List<T> actualRecordList,
                                                  Schema schema,
                                                  String fileGroupId);

  @Test
  public void testReadFileGroupInMergeOnReadTable() throws Exception {
    Map<String, String> writeConfigs = new HashMap<>();
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet");
    writeConfigs.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    writeConfigs.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    writeConfigs.put("hoodie.datasource.write.precombine.field", "timestamp");
    writeConfigs.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "hoodie_test");
    writeConfigs.put("hoodie.insert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.upsert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    writeConfigs.put("hoodie.delete.shuffle.parallelism", "1");
    writeConfigs.put("hoodie.merge.small.file.group.candidates.limit", "0");
    writeConfigs.put(HoodieMetadataConfig.ENABLE.key(), "false");

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      commitToTable(recordsToStrings(dataGen.generateInserts("001", 100)), INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), 0);

      // Two commits; reading one file group containing a base file and a log file
      commitToTable(recordsToStrings(dataGen.generateUpdates("002", 100)), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), 1);

      // Three commits; reading one file group containing a base file and two log files
      commitToTable(recordsToStrings(dataGen.generateUpdates("003", 100)), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(getHadoopConf(), getBasePath(), dataGen.getPartitionPaths(), 2);
    }
  }

  private void validateOutputFromFileGroupReader(Configuration hadoopConf,
                                                 String basePath,
                                                 String[] partitionPaths,
                                                 int expectedLogFileNum) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf).setBasePath(basePath).build();
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(
        new HoodieLocalEngineContext(hadoopConf), HoodieMetadataConfig.newBuilder().build(),
        FileSystemViewStorageConfig.newBuilder().build(),
        HoodieCommonConfig.newBuilder().build());
    SyncableFileSystemView fsView = viewManager.getFileSystemView(metaClient);
    FileSlice fileSlice = fsView.getAllFileSlices(partitionPaths[0]).findFirst().get();
    List<String> logFilePathList = getLogFileListFromFileSlice(fileSlice);
    Collections.sort(logFilePathList);
    assertEquals(expectedLogFileNum, logFilePathList.size());

    List<T> actualRecordList = new ArrayList<>();
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
    props.setProperty(HoodieTableConfig.RECORD_MERGER_STRATEGY.key(),
        HoodieTableConfig.RECORD_MERGER_STRATEGY.defaultValue());
    HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<T>(
        getHoodieReaderContext(),
        hadoopConf,
        basePath,
        metaClient.getActiveTimeline().lastInstant().get().getTimestamp(),
        fileSlice.getBaseFile(),
        logFilePathList.isEmpty() ? Option.empty() : Option.of(logFilePathList),
        avroSchema,
        props,
        -1,
        -1);
    fileGroupReader.initRecordIterators();
    while (fileGroupReader.hasNext()) {
      actualRecordList.add(fileGroupReader.next());
    }
    fileGroupReader.close();

    validateRecordsInFileGroup(actualRecordList, avroSchema, fileSlice.getFileId());
  }
}
