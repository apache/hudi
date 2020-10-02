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

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FileStatus;
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

  @Test
  public void testCopyOnWriteRollback() throws Exception {
    // given: wrote some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("000")
        .withBaseFilesInPartitions("partA").get("partA");
    String f1 = testTable.addCommit("001")
        .withBaseFilesInPartition("partA", f0)
        .withBaseFilesInPartitions("partB").get("partB");
    String f2 = "f2";
    testTable.forCommit("001")
        .withMarkerFile("partA", f0, IOType.MERGE)
        .withMarkerFile("partB", f1, IOType.CREATE)
        .withMarkerFile("partA", f2, IOType.CREATE);

    // when
    List<HoodieRollbackStat> stats = new SparkMarkerBasedRollbackStrategy(HoodieSparkTable.create(getConfig(), context, metaClient), context, getConfig(), "002")
        .execute(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));

    // then: ensure files are deleted correctly, non-existent files reported as failed deletes
    assertEquals(2, stats.size());

    List<FileStatus> partAFiles = testTable.listAllFiles("partA");
    List<FileStatus> partBFiles = testTable.listAllFiles("partB");

    assertEquals(0, partBFiles.size());
    assertEquals(1, partAFiles.size());
    assertEquals(2, stats.stream().mapToInt(r -> r.getSuccessDeleteFiles().size()).sum());
    assertEquals(1, stats.stream().mapToInt(r -> r.getFailedDeleteFiles().size()).sum());
  }

  @Test
  public void testMergeOnReadRollback() throws Exception {
    // given: wrote some base + log files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f2 = testTable.addRequestedDeltaCommit("000")
        .withBaseFilesInPartitions("partA").get("partA");
    String f1 = testTable.addDeltaCommit("001")
        .withLogFile("partA", f2)
        .withBaseFilesInPartitions("partB").get("partB");
    String f3 = "f3";
    String f4 = "f4";
    testTable.forDeltaCommit("001")
        .withMarkerFile("partB", f1, IOType.CREATE)
        .withMarkerFile("partA", f3, IOType.CREATE)
        .withMarkerFile("partA", f2, IOType.APPEND)
        .withMarkerFile("partB", f4, IOType.APPEND);

    // when
    List<HoodieRollbackStat> stats = new SparkMarkerBasedRollbackStrategy(HoodieSparkTable.create(getConfig(), context, metaClient), context, getConfig(), "002")
        .execute(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));

    // then: ensure files are deleted, rollback block is appended (even if append does not exist)
    assertEquals(2, stats.size());
    // will have the log file
    List<FileStatus> partBFiles = testTable.listAllFiles("partB");
    assertEquals(1, partBFiles.size());
    assertTrue(partBFiles.get(0).getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension()));
    assertTrue(partBFiles.get(0).getLen() > 0);

    List<FileStatus> partAFiles = testTable.listAllFiles("partA");
    assertEquals(3, partAFiles.size());
    assertEquals(2, partAFiles.stream().filter(s -> s.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())).count());
    assertEquals(1, partAFiles.stream().filter(s -> s.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())).filter(f -> f.getLen() > 0).count());

    // only partB/f1_001 will be deleted
    assertEquals(1, stats.stream().mapToInt(r -> r.getSuccessDeleteFiles().size()).sum());
    // partA/f3_001 is non existent
    assertEquals(1, stats.stream().mapToInt(r -> r.getFailedDeleteFiles().size()).sum());
  }
}
