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
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieHFileInputFormat;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableIncrementalRead extends SparkClientFunctionalTestHarness {

  private JobConf roSnapshotJobConf;
  private JobConf roJobConf;
  private JobConf rtJobConf;

  @BeforeEach
  void setUp() {
    roSnapshotJobConf = new JobConf(storageConf().unwrap());
    roJobConf = new JobConf(storageConf().unwrap());
    rtJobConf = new JobConf(storageConf().unwrap());
  }

  // test incremental read does not go past compaction instant for RO views
  // For RT views, incremental read can go past compaction
  @Test
  public void testIncrementalReadsWithCompaction() throws Exception {
    final String partitionPath = "2020/02/20"; // use only one partition for this test
    final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] { partitionPath });
    Properties props = getPropertiesForKeyGen(true);
    props.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.PARQUET.toString());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    HoodieWriteConfig cfg = getConfigBuilder(true).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      /*
       * Write 1 (only inserts)
       */
      String commitTime1 = "001";
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);

      List<HoodieRecord> records001 = dataGen.generateInserts(commitTime1, 200);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records001, client, cfg, commitTime1);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      // verify only one base file shows up with commit time 001
      FileStatus[] snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath, 1, snapshotROFiles, false, roSnapshotJobConf, 200, commitTime1);

      FileStatus[] incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);
      Path firstFilePath = incrementalROFiles[0].getPath();

      FileStatus[] incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf,200, commitTime1);

      assertEquals(firstFilePath, incrementalRTFiles[0].getPath());

      /*
       * Write 2 (updates)
       */
      String updateTime = "004";
      WriteClientTestUtils.startCommitWithTime(client, updateTime);
      List<HoodieRecord> records004 = dataGen.generateUpdates(updateTime, 100);
      updateRecordsInMORTable(metaClient, records004, client, cfg, updateTime, false);

      // verify RO incremental reads - only one base file shows up because updates to into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf, 200, commitTime1, updateTime);

      // request compaction, but do not perform compaction
      String compactionCommitTime = "005";
      WriteClientTestUtils.scheduleTableService(client, "005", Option.empty(), TableServiceType.COMPACT);

      // verify RO incremental reads - only one base file shows up because updates go into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateFiles(partitionPath,1, incrementalROFiles, false, roJobConf, 200, commitTime1);

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf, 200, commitTime1, updateTime);

      // write 3 - more inserts
      String insertsTime = "006";
      List<HoodieRecord> records006 = dataGen.generateInserts(insertsTime, 200);
      WriteClientTestUtils.startCommitWithTime(client, insertsTime);
      dataFiles = insertRecordsToMORTable(metaClient, records006, client, cfg, insertsTime);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      // verify new write shows up in snapshot mode even though there is pending compaction
      snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath, 2, snapshotROFiles, false, roSnapshotJobConf,400, commitTime1, insertsTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());
      // verify 006 does not show up in RO mode because of pending compaction

      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);

      // verify that if stopAtCompaction is disabled, inserts from "insertsTime" show up
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateFiles(partitionPath,2, incrementalROFiles, false, roJobConf, 400, commitTime1, insertsTime);

      // verify 006 shows up in RT views
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 2, incrementalRTFiles, true, rtJobConf, 400, commitTime1, updateTime, insertsTime);

      // perform the scheduled compaction
      HoodieWriteMetadata result = client.compact(compactionCommitTime);
      client.commitCompaction(compactionCommitTime, result, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionCommitTime));

      // verify new write shows up in snapshot mode after compaction is complete
      snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath,2, snapshotROFiles, false, roSnapshotJobConf,400, commitTime1, updateTime,
          insertsTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, "002", -1, true);
      assertTrue(incrementalROFiles.length == 2);
      // verify 006 shows up because of pending compaction
      validateFiles(partitionPath, 2, incrementalROFiles, false, roJobConf, 400, commitTime1, updateTime,
          insertsTime);
    }
  }

  private FileStatus[] getROSnapshotFiles(String partitionPath)
      throws Exception {
    FileInputFormat.setInputPaths(roSnapshotJobConf, Paths.get(basePath(), partitionPath).toString());
    return listStatus(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue(), roSnapshotJobConf, false);
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, boolean stopAtCompaction)
      throws Exception {
    return getROIncrementalFiles(partitionPath, "000", -1, stopAtCompaction);
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull, boolean stopAtCompaction)
      throws Exception {
    setupIncremental(roJobConf, startCommitTime, numCommitsToPull, stopAtCompaction);
    FileInputFormat.setInputPaths(roJobConf, Paths.get(basePath(), partitionPath).toString());
    return listStatus(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue(), roJobConf, false);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath)
      throws Exception {
    return getRTIncrementalFiles(partitionPath, "000", -1);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
      throws Exception {
    setupIncremental(rtJobConf, startCommitTime, numCommitsToPull, false);
    FileInputFormat.setInputPaths(rtJobConf, Paths.get(basePath(), partitionPath).toString());
    return listStatus(HoodieTableConfig.BASE_FILE_FORMAT.defaultValue(), rtJobConf, true);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull, boolean stopAtCompaction) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);

    String stopAtCompactionPropName =
        String.format(HoodieHiveUtils.HOODIE_STOP_AT_COMPACTION_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setBoolean(stopAtCompactionPropName, stopAtCompaction);
  }

  private void validateFiles(String partitionPath, int expectedNumFiles,
      FileStatus[] files, boolean realtime, JobConf jobConf,
      int expectedRecords, String... expectedCommits) {

    assertEquals(expectedNumFiles, files.length);
    Set<String> expectedCommitsSet = Arrays.stream(expectedCommits).collect(Collectors.toSet());
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf(),
        Collections.singletonList(Paths.get(basePath(), partitionPath).toString()), basePath(), jobConf, realtime);
    assertEquals(expectedRecords, records.size());
    Set<String> actualCommits = records.stream().map(r ->
        r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()).collect(Collectors.toSet());
    assertEquals(expectedCommitsSet, actualCommits);
  }

  private FileStatus[] listStatus(HoodieFileFormat baseFileFormat, JobConf jobConf, boolean realtime) throws IOException {
    // This is required as Hoodie InputFormats do not extend a common base class and FileInputFormat's
    // listStatus() is protected.
    FileInputFormat inputFormat = HoodieInputFormatUtils.getInputFormat(baseFileFormat, realtime, jobConf);
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          return ((HoodieParquetRealtimeInputFormat)inputFormat).listStatus(jobConf);
        } else {
          return ((HoodieParquetInputFormat)inputFormat).listStatus(jobConf);
        }
      case HFILE:
        if (realtime) {
          return ((HoodieHFileRealtimeInputFormat)inputFormat).listStatus(jobConf);
        } else {
          return ((HoodieHFileInputFormat)inputFormat).listStatus(jobConf);
        }
      default:
        throw new HoodieIOException("Hoodie InputFormat not implemented for base file format " + baseFileFormat);
    }
  }
}
