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

package org.apache.hudi.table.action.rollback;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.io.IOType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMarkerBasedRollbackStrategy extends HoodieClientTestBase {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient();
    initDFS();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private void givenCommit0(boolean isDeltaCommit) throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partA", "000", "f2");
    if (isDeltaCommit) {
      HoodieClientTestUtils.fakeDeltaCommit(basePath, "000");
    } else {
      HoodieClientTestUtils.fakeCommit(basePath, "000");
    }
  }

  private void givenInflightCommit1(boolean isDeltaCommit) throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partB", "001", "f1");
    HoodieClientTestUtils.createMarkerFile(basePath, "partB", "001", "f1", IOType.CREATE);

    HoodieClientTestUtils.createMarkerFile(basePath, "partA", "001", "f3", IOType.CREATE);

    if (isDeltaCommit) {
      HoodieClientTestUtils.fakeLogFile(basePath, "partA", "001", "f2", 0);
      HoodieClientTestUtils.createMarkerFile(basePath, "partA", "001", "f2", IOType.APPEND);
      HoodieClientTestUtils.createMarkerFile(basePath, "partB", "001", "f4", IOType.APPEND);
      HoodieClientTestUtils.fakeInflightDeltaCommit(basePath, "001");
    } else {
      HoodieClientTestUtils.fakeDataFile(basePath, "partA", "001", "f2");
      HoodieClientTestUtils.createMarkerFile(basePath, "partA", "001", "f2", IOType.MERGE);
      HoodieClientTestUtils.fakeInFlightCommit(basePath, "001");
    }
  }

  @Test
  public void testCopyOnWriteRollback() throws Exception {
    // given: wrote some base files and corresponding markers
    givenCommit0(false);
    givenInflightCommit1(false);

    // when
    List<HoodieRollbackStat> stats = new MarkerBasedRollbackStrategy(HoodieTable.create(metaClient, getConfig(), hadoopConf), jsc, getConfig(), "002")
        .execute(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));

    // then: ensure files are deleted correctly, non-existent files reported as failed deletes
    assertEquals(2, stats.size());
    assertEquals(0, FileSystemTestUtils.listRecursive(fs, new Path(basePath + "/partB")).size());
    assertEquals(1, FileSystemTestUtils.listRecursive(fs, new Path(basePath + "/partA")).size());
    assertEquals(2, stats.stream().mapToInt(r -> r.getSuccessDeleteFiles().size()).sum());
    assertEquals(1, stats.stream().mapToInt(r -> r.getFailedDeleteFiles().size()).sum());
  }

  @Test
  public void testMergeOnReadRollback() throws Exception {
    // given: wrote some base + log files and corresponding markers
    givenCommit0(true);
    givenInflightCommit1(true);

    // when
    List<HoodieRollbackStat> stats = new MarkerBasedRollbackStrategy(HoodieTable.create(metaClient, getConfig(), hadoopConf), jsc, getConfig(), "002")
        .execute(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));

    // then: ensure files are deleted, rollback block is appended (even if append does not exist)
    assertEquals(2, stats.size());
    // will have the log file
    List<FileStatus> partBFiles = FileSystemTestUtils.listRecursive(fs, new Path(basePath + "/partB"));
    assertEquals(1, partBFiles.size());
    assertTrue(partBFiles.get(0).getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension()));
    assertTrue(partBFiles.get(0).getLen() > 0);

    List<FileStatus> partAFiles = FileSystemTestUtils.listRecursive(fs, new Path(basePath + "/partA"));
    assertEquals(3, partAFiles.size());
    assertEquals(2, partAFiles.stream().filter(s -> s.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())).count());
    assertEquals(1, partAFiles.stream().filter(s -> s.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())).filter(f -> f.getLen() > 0).count());

    // only partB/f1_001 will be deleted
    assertEquals(1, stats.stream().mapToInt(r -> r.getSuccessDeleteFiles().size()).sum());
    // partA/f3_001 is non existent
    assertEquals(1, stats.stream().mapToInt(r -> r.getFailedDeleteFiles().size()).sum());
  }
}
