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

package org.apache.hudi.execution;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.HoodieAvroParquetWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.table.HoodieTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestParquetFileMetaToWriteStatusConvertor extends HoodieCommonTestHarness {

  private TaskContextSupplier mockedContextSupplier;
  private HoodieTable mockedHoodieTable;
  private HoodieIndex mockedIndex;

  @BeforeEach
  public void init() throws IOException, NoSuchFieldException {
    initPath();
    initTestDataGenerator();
    initMetaClient();

    mockedContextSupplier = mock(TaskContextSupplier.class);
    when(mockedContextSupplier.getAttemptIdSupplier()).thenReturn(() -> 1L);
    when(mockedContextSupplier.getStageIdSupplier()).thenReturn(() -> 1);
    when(mockedContextSupplier.getPartitionIdSupplier()).thenReturn(() -> 1);

    mockedIndex = mock(HoodieIndex.class);
    when(mockedIndex.isImplicitWithStorage()).thenReturn(true);

    Configuration configuration = metaClient.getHadoopConf();
    mockedHoodieTable = mock(HoodieTable.class);
    when(mockedHoodieTable.getHadoopConf()).thenReturn(configuration);
    when(mockedHoodieTable.getMetaClient()).thenReturn(metaClient);
    when(mockedHoodieTable.getIndex()).thenReturn(mockedIndex);
  }

  @Test
  public void testWriteStatusConvertion() throws IOException {
    HoodieWriteConfig writeConfig =
        getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM)
            .build();
    String fileId = UUID.randomUUID().toString();
    String prevCommitTime = HoodieActiveTimeline.createNewInstantTime();
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    String writeToken = FSUtils.makeWriteToken(mockedContextSupplier.getPartitionIdSupplier().get(),
        mockedContextSupplier.getStageIdSupplier().get(), mockedContextSupplier.getAttemptIdSupplier().get());
    String srcFileName = fileId + "_" + writeToken + "_" + newCommitTime + ".parquet";
    String partitionPath = "2021/09/21";
    String srcPath = basePath + "/" + partitionPath + "/" + srcFileName;

    Map<String, Object> executionConfigs = new HashMap<>();
    executionConfigs.put("timeTaken", 1000L);
    executionConfigs.put("prevCommit", prevCommitTime);
    writeParquetFile(writeConfig, srcPath, partitionPath, newCommitTime, 50);
    ParquetFileMetaToWriteStatusConvertor convertor =
        new ParquetFileMetaToWriteStatusConvertor(mockedHoodieTable, writeConfig);
    WriteStatus writeStatus = convertor.convert(srcPath, partitionPath, executionConfigs);
    Assertions.assertEquals(writeStatus.getFileId(), fileId);
    Assertions.assertEquals(writeStatus.getPartitionPath(), partitionPath);
    HoodieWriteStat writeStat = writeStatus.getStat();
    Assertions.assertNotNull(writeStat);
    Assertions.assertEquals(50, writeStat.getNumWrites());
    Assertions.assertEquals(50, writeStat.getNumInserts());
    Assertions.assertEquals(prevCommitTime, writeStat.getPrevCommit());

    HoodieWriteStat.RuntimeStats runtimeStats = writeStat.getRuntimeStats();
    Assertions.assertNotNull(runtimeStats);
    Assertions.assertEquals(1000, runtimeStats.getTotalCreateTime());

  }

  /**
   * This method creates HoodieWriteConfig with default configuration.
   */
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(WriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024)
            .parquetMaxFileSize(1024 * 1024)
            .build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  /**
   * This method writes a parquet file for testing.
   */
  public void writeParquetFile(HoodieWriteConfig writeConfig, String fileName, String partitionPath,
                               String instantTime, int recordsCount) throws IOException {
    Schema originalSchema = new Schema.Parser().parse(writeConfig.getSchema());
    Schema hoodieSchemaWithMetadataFields = HoodieAvroUtils.addMetadataFields(originalSchema);
    HoodieAvroParquetWriter fileWriter =
        (HoodieAvroParquetWriter) HoodieFileWriterFactory.getFileWriter(instantTime, new Path(fileName),
            metaClient.getHadoopConf(), writeConfig, hoodieSchemaWithMetadataFields, mockedContextSupplier,
            writeConfig.getRecordMerger().getRecordType());

    for (int i = 0; i < recordsCount; i++) {
      String recordKey = UUID.randomUUID().toString();
      HoodieKey key = new HoodieKey(recordKey, partitionPath.replaceAll("/", "-"));
      HoodieRecord<RawTripTestPayload> record =
          new HoodieAvroRecord<>(key, dataGen.generateRandomValue(key, instantTime));
      Option<IndexedRecord> indexedRecord = record.getData().getInsertValue(originalSchema);
      IndexedRecord recordWithMetadataInSchema =
          HoodieAvroUtils.rewriteRecord((GenericRecord) indexedRecord.get(), hoodieSchemaWithMetadataFields);
      fileWriter.writeAvroWithMetadata(record.getKey(), recordWithMetadataInSchema);
    }
    fileWriter.close();
  }

}
