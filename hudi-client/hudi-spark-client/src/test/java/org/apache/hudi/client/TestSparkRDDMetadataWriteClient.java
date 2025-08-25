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

package org.apache.hudi.client;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkRDDMetadataWriteClient extends HoodieClientTestBase {

  private Random random;

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    random = new Random(0xDEED);
  }

  @Test
  public void testWritesViaMetadataWriteClient() throws Exception {

    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(false).withEnableRecordIndex(true)
            .withRecordIndexFileGroupCount(1, 1).withStreamingWriteEnabled(true).build()).build();

    // trigger end to end write to data table so that metadata table is also initialized.
    initDataTableWithACommit(hoodieWriteConfig);

    // fetch metadata file slice info
    HoodieWriteConfig mdtWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(hoodieWriteConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    Map<String, List<String>> mdtPartitionsFileIdMapping = new HashMap<>();
    List<HoodieFileGroupId> nonFilesPartitionFileGroupIdList = new ArrayList<>();
    List<HoodieFileGroupId> filesPartitionFileGroupIdList = new ArrayList<>();
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setBasePath(mdtWriteConfig.getBasePath()).setConf(context.getStorageConf()).build();
    fetchMetadataFileSliceInfo(metadataMetaClient, filesPartitionFileGroupIdList, nonFilesPartitionFileGroupIdList, mdtPartitionsFileIdMapping);

    List<HoodieRecord> filesPartitionExpectedRecords = new ArrayList<>();
    Map<String, HoodieRecord> filesPartitionExpectedRecordsMap = new HashMap<>();
    List<String> expectedAllPartitions = new ArrayList<>();
    List<HoodieRecord> rliRecords = new ArrayList<>();
    Map<String, HoodieRecord> rliPartitionExpectedRecordsMap = new HashMap<>();
    String commitTimeOfInterest = null;

    // create Write client to SparkRDDMetadataWriteClient and trigger writes.
    try (SparkRDDMetadataWriteClient client = new SparkRDDMetadataWriteClient(context, mdtWriteConfig)) {
      commitTimeOfInterest = WriteClientTestUtils.createNewInstantTime();

      // prepare FILES partition records.
      prepareFilesPartitionRecords(mdtPartitionsFileIdMapping.get(MetadataPartitionType.FILES.getPartitionPath()).get(0),
          commitTimeOfInterest, filesPartitionExpectedRecords, filesPartitionExpectedRecordsMap, expectedAllPartitions);

      // prepare RLI records.
      prepareRliRecords(commitTimeOfInterest, mdtPartitionsFileIdMapping, expectedAllPartitions, rliRecords, rliPartitionExpectedRecordsMap);

      // ingest RLI records to metadata table.
      client.startCommitForMetadataTable(metadataMetaClient, commitTimeOfInterest, DELTA_COMMIT_ACTION);
      JavaRDD<WriteStatus> partialWriteStatusesRDD = client.firstUpsertPreppedRecords(jsc.parallelize(rliRecords), commitTimeOfInterest, nonFilesPartitionFileGroupIdList);
      List<WriteStatus> partialWriteStatuses = partialWriteStatusesRDD.collect();
      // assert that isMetadataTable is rightly set
      partialWriteStatuses.forEach(writeStatus -> assertTrue(writeStatus.isMetadataTable()));

      // validate that the commit is still pending since we are streaming write to metadata table.
      HoodieActiveTimeline reloadedMdtActiveTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(reloadedMdtActiveTimeline.filterCompletedInstants().getInstants().stream().count(), 3); // files, rli instantiaton and 1 write to data table.
      String finalCommitTimeOfInterest = commitTimeOfInterest;
      assertTrue(reloadedMdtActiveTimeline.filterInflightsAndRequested().getInstants().stream().anyMatch(instant -> instant.requestedTime().equals(finalCommitTimeOfInterest)));

      // write to FILES partition
      JavaRDD<WriteStatus> filePartitionWriteStatusesRDD = client.secondaryUpsertPreppedRecords(jsc.parallelize(filesPartitionExpectedRecords), commitTimeOfInterest);
      List<WriteStatus> filesPartitionWriteStatus = filePartitionWriteStatusesRDD.collect();
      // assert that isMetadataTable is rightly set
      filesPartitionWriteStatus.forEach(writeStatus -> assertTrue(writeStatus.isMetadataTable()));
      List<HoodieWriteStat> allWriteStats = new ArrayList<>();
      allWriteStats.addAll(partialWriteStatuses.stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList()));
      allWriteStats.addAll(filesPartitionWriteStatus.stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList()));
      client.commitStats(commitTimeOfInterest, allWriteStats, Option.empty(), DELTA_COMMIT_ACTION);
    }

    // validate
    readFromMetadataTableAndValidateRecords(metadataMetaClient, hoodieWriteConfig, filesPartitionExpectedRecordsMap, rliPartitionExpectedRecordsMap, commitTimeOfInterest);
  }

  private void initDataTableWithACommit(HoodieWriteConfig hoodieWriteConfig) throws Exception {
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      final int numRecords = 100;
      String newCommitTime = WriteClientTestUtils.createNewInstantTime();
      insertBatch(hoodieWriteConfig, client, newCommitTime, HoodieTimeline.INIT_INSTANT_TS, numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords, 1, Option.empty(), INSTANT_GENERATOR);
    }
  }

  private void fetchMetadataFileSliceInfo(HoodieTableMetaClient metadataMetaClient, List<HoodieFileGroupId> filesPartitionFileGroupIdList,
                                          List<HoodieFileGroupId> nonFilesPartitionsFileGroupIdList, Map<String, List<String>> mdtPartitionsFileIdMapping) {
    try (HoodieTableFileSystemView fsView = HoodieTableMetadataUtil.getFileSystemViewForMetadataTable(metadataMetaClient)) {
      List<FileSlice> fileSlices =
          HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), MetadataPartitionType.FILES.getPartitionPath());
      mdtPartitionsFileIdMapping.put(MetadataPartitionType.FILES.getPartitionPath(), fileSlices.stream().map(fileSlice -> fileSlice.getFileId()).collect(Collectors.toList()));
      fileSlices.forEach(fileSlice -> {
        filesPartitionFileGroupIdList.add(new HoodieFileGroupId(MetadataPartitionType.FILES.getPartitionPath(), fileSlice.getFileId()));
      });

      fileSlices =
          HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      mdtPartitionsFileIdMapping.put(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), fileSlices.stream().map(fileSlice -> fileSlice.getFileId()).collect(Collectors.toList()));
      fileSlices.forEach(fileSlice -> {
        nonFilesPartitionsFileGroupIdList.add(new HoodieFileGroupId(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), fileSlice.getFileId()));
      });
    }
  }

  private void readFromMetadataTableAndValidateRecords(HoodieTableMetaClient metadataMetaClient, HoodieWriteConfig hoodieWriteConfig,
                                                       Map<String, HoodieRecord> filesPartitionExpectedRecordsMap,
                                                       Map<String, HoodieRecord> rliPartitionExpectedRecordsMap,
                                                       String validMetadataInstant) throws IOException {
    // read from MDT and validate all records.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
    try (HoodieTableFileSystemView fsView = HoodieTableMetadataUtil.getFileSystemViewForMetadataTable(metadataMetaClient)) {
      List<FileSlice> filesFileSliceList = HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), MetadataPartitionType.FILES.getPartitionPath());
      assertEquals(1, filesFileSliceList.size(), "File partition is expected to contain just 1 file slice");
      FileSlice filesFileSlice = filesFileSliceList.get(0);
      readFromMDTFileSliceAndValidate(metadataMetaClient, filesFileSlice, filesPartitionExpectedRecordsMap, validMetadataInstant, hoodieWriteConfig);

      List<FileSlice> rliFileSliceList = HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.ofNullable(fsView), MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      assertEquals(1, rliFileSliceList.size(), "RLI partition is expected to contain just 1 file slice");
      FileSlice rliFileSlice = rliFileSliceList.get(0);
      readFromMDTFileSliceAndValidate(metadataMetaClient, rliFileSlice, rliPartitionExpectedRecordsMap, validMetadataInstant, hoodieWriteConfig);
    }
  }

  private void readFromMDTFileSliceAndValidate(HoodieTableMetaClient metadataMetaClient, FileSlice fileSlice, Map<String, HoodieRecord> expectedRecordsMap,
                                               String validMetadataInstant, HoodieWriteConfig hoodieWriteConfig)
      throws IOException {

    // read from the file slice of interest.
    List<String> sortedKeysForFilesPartition = new ArrayList<>(expectedRecordsMap.keySet());
    Collections.sort(sortedKeysForFilesPartition);
    List<Expression> right = sortedKeysForFilesPartition.stream().map(Literal::from).collect(Collectors.toList());
    Predicate predicate = Predicates.in(null, right);

    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(metaClient, metadataMetaClient);
    validInstantTimestamps.add(validMetadataInstant);
    InstantRange instantRange = InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build();
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(metadataMetaClient.getStorageConf(), metadataMetaClient.getTableConfig(), Option.of(instantRange), Option.of(predicate));
    HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withFileSlice(fileSlice)
        .withLatestCommitTime(validMetadataInstant)
        .withRequestedSchema(HoodieMetadataRecord.getClassSchema())
        .withDataSchema(schema)
        .withProps(new TypedProperties())
        .withEnableOptimizedLogBlockScan(hoodieWriteConfig.getMetadataConfig().isOptimizedLogBlocksScanEnabled())
        .build();
    try (ClosableIterator<HoodieRecord<IndexedRecord>> records = fileGroupReader.getClosableHoodieRecordIterator()) {
      Map<String, HoodieRecord<HoodieMetadataPayload>> actualMdtRecordMap = new HashMap<>();
      records.forEachRemaining(record -> {
        HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of((GenericRecord) record.getData()));
        actualMdtRecordMap.put(record.getRecordKey(), new HoodieAvroRecord<>(record.getKey(), payload));
      });

      assertEquals(actualMdtRecordMap.size(), expectedRecordsMap.size());
      actualMdtRecordMap.forEach((recordKey, record) -> {
        assertTrue(expectedRecordsMap.containsKey(recordKey));
        if (!recordKey.equals(RECORDKEY_PARTITION_LIST)) {
          // ignore __all_partition_records sinec it could have partitions from first commit which could be from HoodieTestDatagenerator.
          assertEquals(((HoodieMetadataPayload) expectedRecordsMap.get(recordKey).getData()).getFilenames(), record.getData().getFilenames());
        }
      });
    }
  }

  private void prepareFilesPartitionRecords(String filesPartitionFileId, String commitTime, List<HoodieRecord> filesPartitionExpectedRecords,
                                            Map<String, HoodieRecord> filesPartitionExpectedRecordsMap,
                                            List<String> expectedAllPartitions) {
    // prepare FILES partition records.
    int counter = 0;
    while (counter < 3) {
      String partition = "p_" + counter;
      expectedAllPartitions.add(partition);
      String fileNamePrefix = "file_";
      Map<String, Long> filesAdded = new HashMap<>();
      for (int j = 0; j < 5; j++) {
        filesAdded.put(partition + "_" + fileNamePrefix + j, 100L);
      }
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded, Collections.emptyList());
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation(commitTime, filesPartitionFileId));
      record.seal();
      filesPartitionExpectedRecords.add(record);
      filesPartitionExpectedRecordsMap.put(record.getRecordKey(), record);
      counter++;
    }
    // all partition record
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(expectedAllPartitions);
    record.unseal();
    record.setCurrentLocation(new HoodieRecordLocation(commitTime, filesPartitionFileId));
    record.seal();
    filesPartitionExpectedRecords.add(record);
    filesPartitionExpectedRecordsMap.put(record.getRecordKey(), record);
  }

  private void prepareRliRecords(String commitTimeOfInterest, Map<String, List<String>> mdtPartitionsFileIdMapping, List<String> expectedAllPartitions,
                                 List<HoodieRecord> rliRecords, Map<String, HoodieRecord> rliPartitionExpectedRecordsMap) {
    List<String> randomFileIds = new ArrayList<>();
    int counter = 0;
    while (counter++ < 3) {
      randomFileIds.add(UUID.randomUUID().toString() + "-0");
    }
    counter = 0;
    List<HoodieRecord> phoneyInsertRecords = dataGen.generateInserts(commitTimeOfInterest, 100);
    String rliFileId = mdtPartitionsFileIdMapping.get(MetadataPartitionType.RECORD_INDEX.getPartitionPath()).get(0);
    while (counter < 100) {
      String randomPartition = expectedAllPartitions.get(random.nextInt(expectedAllPartitions.size()));
      String randomFileId = randomFileIds.get(random.nextInt(randomFileIds.size()));
      HoodieRecord rliRecord = HoodieMetadataPayload.createRecordIndexUpdate(phoneyInsertRecords.get(counter).getRecordKey(), randomPartition,
          randomFileId, commitTimeOfInterest, 0);
      rliRecord.unseal();
      rliRecord.setCurrentLocation(new HoodieRecordLocation(commitTimeOfInterest, rliFileId));
      rliRecord.seal();
      rliRecords.add(rliRecord);
      rliPartitionExpectedRecordsMap.put(rliRecord.getRecordKey(), rliRecord);
      counter++;
    }
  }

}
