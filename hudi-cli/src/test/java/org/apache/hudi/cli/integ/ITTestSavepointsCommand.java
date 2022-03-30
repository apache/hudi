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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.SavepointsCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
public class ITTestSavepointsCommand extends AbstractShellIntegrationTest {

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    tablePath = basePath + Path.SEPARATOR + tableName;

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case of command 'savepoint create'.
   */
  @Test
  public void testSavepoint() {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(tablePath, instantTime, jsc.hadoopConfiguration());
    }

    String savepoint = "102";
    CommandResult cr = getShell().executeCommand(
        String.format("savepoint create --commit %s --sparkMaster %s", savepoint, "local"));

    assertAll("Command run failed",
        () -> assertTrue(cr.isSuccess()),
        () -> assertEquals(
            String.format("The commit \"%s\" has been savepointed.", savepoint), cr.getResult().toString()));

    // there is 1 savepoint instant
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.getSavePointTimeline().countInstants());
  }

  /**
   * Test case of command 'savepoint rollback'.
   */
  @Test
  public void testRollbackToSavepoint() throws IOException {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(tablePath, instantTime, jsc.hadoopConfiguration());
    }

    // generate one savepoint
    String savepoint = "102";
    HoodieTestDataGenerator.createSavepointFile(tablePath, savepoint, jsc.hadoopConfiguration());

    CommandResult cr = getShell().executeCommand(
        String.format("savepoint rollback --savepoint %s --sparkMaster %s", savepoint, "local"));

    assertAll("Command run failed",
        () -> assertTrue(cr.isSuccess()),
        () -> assertEquals(
            String.format("Savepoint \"%s\" rolled back", savepoint), cr.getResult().toString()));

    // there is 1 restore instant
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.getRestoreTimeline().countInstants());

    // 103 instant had rollback
    assertFalse(timeline.getCommitTimeline().containsInstant(
        new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "103")));
  }

  /**
   * Test case of command 'savepoint delete'.
   */
  @Test
  public void testDeleteSavepoint() throws IOException {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(tablePath, instantTime, jsc.hadoopConfiguration());
    }

    // generate two savepoint
    String savepoint1 = "100";
    String savepoint2 = "102";
    HoodieTestDataGenerator.createSavepointFile(tablePath, savepoint1, jsc.hadoopConfiguration());
    HoodieTestDataGenerator.createSavepointFile(tablePath, savepoint2, jsc.hadoopConfiguration());

    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(2, timeline.getSavePointTimeline().countInstants(), "There should 2 instants.");

    CommandResult cr = getShell().executeCommand(
        String.format("savepoint delete --commit %s --sparkMaster %s", savepoint1, "local"));

    assertAll("Command run failed",
        () -> assertTrue(cr.isSuccess()),
        () -> assertEquals(
            String.format("Savepoint \"%s\" deleted.", savepoint1), cr.getResult().toString()));

    // reload timeline
    timeline = timeline.reload();
    assertEquals(1, timeline.getSavePointTimeline().countInstants(), "There should 1 instants.");

    // after delete, 100 instant should not exist.
    assertFalse(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepoint1)));
  }
}
