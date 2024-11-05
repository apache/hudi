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

package org.apache.hudi.table.functional;

import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieInconsistentMetadataException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMarkerBasedReconciliation extends HoodieClientTestBase {

  private HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient(tableType);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCommitMetadataValidationForDuplicateFilesCOW(boolean enablePostCommitMetadataValidation) throws Exception {
    testCommitMetadataValidationCOW(enablePostCommitMetadataValidation, true);
  }

  @Test
  public void testCommitMetadataValidationForDuplicateFilesNonPartitioned() throws Exception {
    testCommitMetadataValidationCOW(true, false);
  }

  private void testCommitMetadataValidationCOW(boolean enablePostCommitMetadataValidation, boolean testPartitionedTable) throws Exception {
    // write some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieMetrics metrics = Mockito.mock(HoodieMetrics.class);
    String partitionA = testPartitionedTable ? "partA" : StringUtils.EMPTY_STRING;
    String partitionB = testPartitionedTable ? "partB" : StringUtils.EMPTY_STRING;
    String f0 = testTable.addRequestedCommit("001")
        .getFileIdsWithBaseFilesInPartitions(partitionA).get(partitionA);
    String f1 = testTable.addCommit("001")
        .getFileIdsWithBaseFilesInPartitions(partitionB).get(partitionB);

    testTable.forCommit("001")
        .withMarkerFile(partitionA, f0, IOType.CREATE)
        .withMarkerFile(partitionB, f1, IOType.CREATE);

    // test happy path. no retries.
    HoodieWriteConfig writerConfig = getConfigBuilder()
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(false).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .doValidateCommitMetadataConsistency(enablePostCommitMetadataValidation)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true).build()).build();
    HoodieTable hoodieTable = HoodieSparkTable.create(writerConfig, context, metaClient);

    FileStatus[] baseFiles = testTable.listAllBaseFiles();

    List<HoodieWriteStat> stats = new ArrayList<>();
    List<HoodieWriteStat> finalStats = stats;
    Arrays.stream(baseFiles).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        if (!testPartitionedTable) {
          stat.setPath(fileStatus.getPath().getName());
        } else {
          stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        }
        finalStats.add(stat);
      }
    });

    // there should not be any additional data files found.
    assertTrue(hoodieTable.getInvalidDataFilePaths("001", finalStats, true, "").isEmpty());

    // lets do reconcile and again check for invalid files.
    hoodieTable.finalizeWrite(context, "001", finalStats); // no op since there are no additional files
    hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", finalStats, metrics); // should not throw

    // lets test failure scenarios
    f0 = testTable.addRequestedCommit("002")
        .getFileIdsWithBaseFilesInPartitions(partitionA).get(partitionB);
    f1 = testTable.addCommit("002")
        .getFileIdsWithBaseFilesInPartitions(partitionB).get(partitionB);

    testTable.forCommit("002")
        .withMarkerFile(partitionA, f0, IOType.CREATE)
        .withMarkerFile(partitionB, f1, IOType.CREATE);

    // create another data file to simulate spark task retries
    String duplicateDataFile = createDuplicateDataFile(partitionA, f0, "1-2-3");
    hoodieTable = HoodieSparkTable.create(writerConfig, context, metaClient);

    baseFiles = testTable.listAllBaseFiles();

    stats = new ArrayList<>();
    List<HoodieWriteStat> finalStats1 = stats;
    Arrays.stream(baseFiles).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        if (!testPartitionedTable) {
          stat.setPath(fileStatus.getPath().getName());
        } else {
          stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        }
        finalStats1.add(stat);
      }
    });

    Set<String> invalidDataFilePaths = hoodieTable.getInvalidDataFilePaths("001", finalStats1, true, "");
    assertEquals(invalidDataFilePaths.size(), 1);
    assertEquals(invalidDataFilePaths.stream().findFirst().get(), duplicateDataFile);

    // lets do reconcile and again check for invalid files.
    hoodieTable.finalizeWrite(context, "001", finalStats1); // this should have deleted the duplicate data file
    hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", finalStats1, metrics); // should not throw

    // create another data file to simulate stray executor adding new data file post marker based reconciliation
    createDuplicateDataFile(partitionA, f0, "3-4-5");
    if (enablePostCommitMetadataValidation) {
      // post commit validation is expected to fail due to the presence of data file
      HoodieTable finalHoodieTable1 = hoodieTable;
      assertThrows(HoodieInconsistentMetadataException.class, () -> {
        finalHoodieTable1.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", finalStats1, metrics);
      });
      Mockito.verify(metrics).emitCommitValidationFailures(HoodieActiveTimeline.DELTA_COMMIT_ACTION);
    } else {
      // no exception will be thrown since post commit metadata validation is disabled.
      hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", stats, metrics);
      Mockito.verify(metrics, Mockito.never()).emitCommitValidationFailures(HoodieActiveTimeline.DELTA_COMMIT_ACTION);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true})
  public void testCommitMetadataValidationForDuplicateFilesMOR(boolean enablePostCommitMetadataValidation) throws Exception {
    // write some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieMetrics metrics = Mockito.mock(HoodieMetrics.class);
    String f0 = testTable.addRequestedCommit("001")
        .getFileIdsWithBaseFilesInPartitions("partA").get("partA");
    String f1 = testTable.addCommit("001")
        .getFileIdsWithBaseFilesInPartitions("partB").get("partB");
    // add few log files
    Pair<HoodieTestTable, List<String>> logFileMap = testTable.forCommit("001")
        .withLogFile("partA", f0, 1, 2);
    String logFile1 = logFileMap.getValue().get(0);
    String logFile2 = logFileMap.getValue().get(1);

    testTable.forCommit("001")
        .withMarkerFile("partA", f0, IOType.CREATE)
        .withMarkerFile("partB", f1, IOType.CREATE);
    testTable.withLogMarkerFile("partA", logFile1.substring(logFile1.lastIndexOf("/") + 1));
    testTable.withLogMarkerFile("partA", logFile2.substring(logFile2.lastIndexOf("/") + 1));

    // create a data file and log file to simulate spark task retries
    String duplicateDataFile = createDuplicateDataFile("partA", f0, "1-2-3");
    // create a new log file to simulate spark task retry
    logFileMap = testTable.forCommit("001")
        .withLogFile("partA", f0, 3);
    testTable.withLogMarkerFile("partA", logFileMap.getValue().get(0).substring(logFileMap.getValue().get(0).lastIndexOf("/") + 1));

    HoodieWriteConfig writerConfig = getConfigBuilder()
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(false).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .doValidateCommitMetadataConsistency(enablePostCommitMetadataValidation)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true).build()).build();
    HoodieTable hoodieTable = HoodieSparkTable.create(writerConfig, context, metaClient);

    List<HoodieWriteStat> stats = new ArrayList<>();
    Arrays.stream(testTable.listAllBaseFiles()).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        stats.add(stat);
      }
    });

    // prepare write stats for log files
    // lets ensure we have write stats for spurious log file as well
    Arrays.stream(testTable.listAllLogFiles()).forEach(fileStatus -> {
      HoodieWriteStat stat = new HoodieWriteStat();
      String partitionPath = fileStatus.getPath().getParent().getName();
      stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
      stats.add(stat);
    });

    Set<String> invalidFilePaths = hoodieTable.getInvalidDataFilePaths("001", stats, true, "");
    assertFalse(invalidFilePaths.isEmpty());
    assertEquals(invalidFilePaths.size(), 1);
    assertEquals(invalidFilePaths.stream().findFirst().get(), duplicateDataFile);

    // lets do reconcile and again check for invalid files.
    hoodieTable.finalizeWrite(context, "001", stats); // this should have deleted the duplicate data file. duplicate log files are left intact.
    hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", stats, metrics); // should not throw

    // create another log file to simulate stray executor adding new data file post marker based reconciliation
    logFileMap = testTable.forCommit("001")
        .withLogFile("partA", f0, 4);
    testTable.withLogMarkerFile("partA", logFileMap.getValue().get(0).substring(logFileMap.getValue().get(0).lastIndexOf("/") + 1));

    // do not update write stats. We simulate the dag retrigger here. and so HoodieWriteStat will not be updated w/ spurious data files. Spurious data files exists only on storage.
    if (enablePostCommitMetadataValidation) {
      // post commit validation is expected to fail due to the presence of spurious log file which is not tracked in CommitMetadata/WriteStats
      assertThrows(HoodieInconsistentMetadataException.class, () -> {
        hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", stats, metrics);
      });
      Mockito.verify(metrics).emitCommitValidationFailures(HoodieActiveTimeline.DELTA_COMMIT_ACTION);
    } else {
      // no exception will be thrown since post commit metadata validation is disabled.
      hoodieTable.doValidateCommitMetadataConsistency(HoodieActiveTimeline.DELTA_COMMIT_ACTION, "001", stats, metrics);
      Mockito.verify(metrics, Mockito.never()).emitCommitValidationFailures(HoodieActiveTimeline.DELTA_COMMIT_ACTION);
    }
  }

  private String createDuplicateDataFile(String partition, String fileId, String writeToken) throws Exception {
    String dupDataFile = FSUtils.makeBaseFileName("001", writeToken, fileId, HoodieFileFormat.PARQUET.getFileExtension());
    createBaseFile(basePath, partition, dupDataFile, 1, Instant.now().toEpochMilli());
    // create marker for the dup data file
    FileCreateUtils.createMarkerFile(basePath, partition, "001", "001", fileId, IOType.CREATE, writeToken);
    return StringUtils.isNullOrEmpty(partition) ? dupDataFile : partition + "/" + dupDataFile;
  }

  public static String createBaseFile(String basePath, String partitionPath, String fileName, long length, long lastModificationTimeMilli)
      throws Exception {
    java.nio.file.Path parentPath = Paths.get(basePath, partitionPath);
    java.nio.file.Path baseFilePath = parentPath.resolve(fileName);
    Files.createFile(baseFilePath);
    try (RandomAccessFile raf = new RandomAccessFile(baseFilePath.toFile(), "rw")) {
      raf.setLength(length);
    }
    Files.setLastModifiedTime(baseFilePath, FileTime.fromMillis(lastModificationTimeMilli));
    return baseFilePath.toString();
  }

}
