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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.SavepointsCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestSavepointsCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = tableName();
    tablePath = tablePath(tableName);

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case of command 'savepoints show'.
   */
  @Test
  public void testShowSavepoints() throws IOException {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createSavepointFile(tablePath, instantTime, storageConf());
    }

    Object result = shell.evaluate(() -> "savepoints show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // generate expect result
    String[][] rows = Stream.of("100", "101", "102", "103").sorted(Comparator.reverseOrder())
        .map(instant -> new String[] {instant}).toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_SAVEPOINT_TIME}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of command 'savepoints refresh'.
   */
  @Test
  public void testRefreshMetaClient() throws IOException {
    HoodieTimeline timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "There should have no instant at first");

    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createSavepointFile(tablePath, instantTime, storageConf());
    }

    // Before refresh, no instant
    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
    assertEquals(0, timeline.countInstants(), "there should have no instant");

    Object result = shell.evaluate(() -> "savepoints refresh");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    timeline =
        HoodieCLI.getTableMetaClient().getActiveTimeline().getSavePointTimeline().filterCompletedInstants();

    // After refresh, there are 4 instants
    assertEquals(4, timeline.countInstants(), "there should have 4 instants");
  }
}
