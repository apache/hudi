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

package org.apache.hudi.cli.integ;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.RollbacksCommand;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.CommitsCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
public class ITTestCommitsCommand extends AbstractShellIntegrationTest {

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    tablePath = basePath + File.separator + tableName;

    HoodieCLI.conf = jsc.hadoopConfiguration();
    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case of 'commit rollback' command.
   */
  @Test
  public void testRollbackCommit() throws IOException {
    //Create some commits files and parquet files
    String commitTime1 = "100";
    String commitTime2 = "101";
    String commitTime3 = "102";
    HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, tablePath);

    // three commit files
    HoodieTestUtils.createCommitFiles(tablePath, commitTime1, commitTime2, commitTime3);

    // generate commit files for commits
    for (String commitTime : Arrays.asList(commitTime1, commitTime2, commitTime3)) {
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime, "file-1");
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, commitTime, "file-2");
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, commitTime, "file-3");
    }

    CommandResult cr = getShell().executeCommand(String.format("commit rollback --commit %s --sparkMaster %s --sparkMemory %s",
        commitTime3, "local", "4G"));
    assertTrue(cr.isSuccess());

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    HoodieActiveTimeline rollbackTimeline = new RollbacksCommand.RollbackTimeline(metaClient);
    assertEquals(1, rollbackTimeline.getRollbackTimeline().countInstants(), "There should have 1 rollback instant.");

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    assertEquals(2, timeline.getCommitsTimeline().countInstants(), "There should have 2 instants.");
  }
}
