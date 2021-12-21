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

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableInsertUpdateDelete extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> testSimpleInsertAndUpdate() {
    return Stream.of(
        Arguments.of(HoodieFileFormat.PARQUET, true),
        Arguments.of(HoodieFileFormat.PARQUET, false),
        Arguments.of(HoodieFileFormat.HFILE, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSimpleInsertAndUpdate(HoodieFileFormat fileFormat, boolean populateMetaFields) throws Exception {
    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records, client, cfg, newCommitTime);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (updates)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime);

      String compactionCommitTime = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(compactionCommitTime);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      hoodieTable.getHoodieView().sync();
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
      assertTrue(HoodieTimeline.compareTimestamps("000", HoodieTimeline.LESSER_THAN, latestCompactionCommitTime));

      if (cfg.populateMetaFields()) {
        assertEquals(200, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.of("000")),
            "Must contain 200 records");
      } else {
        assertEquals(200, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.empty()));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSimpleInsertUpdateAndDelete(boolean populateMetaFields) throws Exception {
    Properties properties = populateMetaFields ? new Properties() : getPropertiesForKeyGen();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(true);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      /*
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      HoodieTableFileSystemView tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc().parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      /*
       * Write 2 (only deletes, written to .log file)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);

      statuses = client.upsert(jsc().parallelize(fewRecordsForDelete, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("004", deltaCommit.get().getTimestamp(), "Latest Delta commit should be 004");

      commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf(), dataFiles, basePath(), new JobConf(hadoopConf()), true, false);
      // Wrote 20 records and deleted 20 records, so remaining 20-20 = 0
      assertEquals(0, recordsRead.size(), "Must contain 0 records");
    }
  }

  @Test
  public void testSimpleInsertsGeneratedIntoLogFiles() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, HoodieIndex.IndexType.INMEMORY).build();
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc().parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      HoodieTable table = HoodieSparkTable.create(config, context(), metaClient);
      table.getHoodieView().sync();
      TableFileSystemView.SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> allSlices = tableRTFileSystemView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
        assertEquals(0, allSlices.stream().filter(fileSlice -> fileSlice.getBaseFile().isPresent()).count());
        assertTrue(allSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        long logFileCount = allSlices.stream().filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
        if (logFileCount > 0) {
          // check the log versions start from the base version
          assertTrue(allSlices.stream().map(slice -> slice.getLogFiles().findFirst().get().getLogVersion())
              .allMatch(version -> version.equals(HoodieLogFile.LOGFILE_BASE_VERSION)));
        }
        numLogFiles += logFileCount;
      }

      assertTrue(numLogFiles > 0);
      // Do a compaction
      String instantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      statuses = (JavaRDD<WriteStatus>) writeClient.compact(instantTime);
      String extension = table.getBaseFileExtension();
      assertEquals(numLogFiles, statuses.map(status -> status.getStat().getPath().contains(extension)).count());
      assertEquals(numLogFiles, statuses.count());
      writeClient.commitCompaction(instantTime, statuses, Option.empty());
    }
  }
}
