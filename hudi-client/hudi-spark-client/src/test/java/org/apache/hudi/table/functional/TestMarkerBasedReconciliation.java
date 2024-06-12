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
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  public void testCommitMetadataValidationForDuplicateFiles(boolean enablePostCommitMetadataValidation) throws Exception {
    // given: wrote some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("001")
        .getFileIdsWithBaseFilesInPartitions("partA").get("partA");
    String f1 = testTable.addCommit("001")
        .getFileIdsWithBaseFilesInPartitions("partB").get("partB");

    testTable.forCommit("001")
        .withMarkerFile("partA", f0, IOType.CREATE)
        .withMarkerFile("partB", f1, IOType.CREATE);

    // create another data file to simulate spark task retries
    createDuplicateDataFile(f0, "1-2-3");
    // when
    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder()
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(false).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .doValidateCommitMetadataConsistency(enablePostCommitMetadataValidation)
        .withEmbeddedTimelineServerEnabled(false).build(), context, metaClient);

    FileStatus[] baseFiles = testTable.listAllBaseFiles();

    List<HoodieWriteStat> stats = new ArrayList<>();
    Arrays.stream(baseFiles).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        stats.add(stat);
      }
    });

    assertFalse(hoodieTable.getInvalidDataFilePaths("001", stats).isEmpty());

    // lets do reconcile and again check for invalid files.
    hoodieTable.finalizeWrite(context, "001", stats); // this should have deleted the duplicate data file
    hoodieTable.doValidateCommitMetadataConsistency("001", stats); // should not throw

    // create another data file to simulate stray executor adding new data file post marker based reconciliation
    createDuplicateDataFile(f0, "3-4-5");
    if (enablePostCommitMetadataValidation) {
      // post commit validation is expected to fail due to the presence of data file
      assertThrows(HoodieException.class, () -> {
        hoodieTable.doValidateCommitMetadataConsistency("001", stats);
      });
    } else {
      // no exception will be thrown since post commit metadata validation is disabled.
      hoodieTable.doValidateCommitMetadataConsistency("001", stats);
    }
  }

  private void createDuplicateDataFile(String fileId, String writeToken) throws Exception {
    String dupDataFile = FSUtils.makeBaseFileName("001", writeToken, fileId, HoodieFileFormat.PARQUET.getFileExtension());
    createBaseFile(basePath, "partA", dupDataFile, 1, Instant.now().toEpochMilli());
    // create marker for the dup data file
    FileCreateUtils.createMarkerFile(basePath, "partA", "001", "001", fileId, IOType.CREATE, writeToken);
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
