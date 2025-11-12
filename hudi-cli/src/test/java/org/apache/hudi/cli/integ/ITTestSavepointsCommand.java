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
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
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
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestSavepointsCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;
  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    tablePath = basePath + StoragePath.SEPARATOR + tableName;

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", HoodieTableVersion.current().versionCode(),
        "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case of command 'savepoint create'.
   */
  @Test
  public void testSavepoint() {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(
          tablePath, instantTime, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    }

    String savepoint = "102";
    Object result = shell.evaluate(() ->
            String.format("savepoint create --commit %s --sparkMaster %s", savepoint, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertEquals(
            String.format("The commit \"%s\" has been savepointed.", savepoint), result.toString()));

    // there is 1 savepoint instant
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.getSavePointTimeline().countInstants());
  }

  /**
   * Test case of command 'savepoint rollback'.
   */
  @Disabled("HUDI-6571") // TODO: Fix this test. Probably need to fix HoodieTestDataGenerator to create non-empty commit metadata.
  public void testRollbackToSavepoint() throws IOException {
    // disable metadata table.
    Object result = shell.evaluate(() ->
        String.format("metadata delete"));

    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // generate four commits
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(
          tablePath, instantTime, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    }

    // generate one savepoint
    String savepoint = "102";
    HoodieTestDataGenerator.createSavepointFile(
        tablePath, savepoint, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    result = shell.evaluate(() ->
            String.format("savepoint rollback --savepoint %s --sparkMaster %s", savepoint, "local"));

    Object finalResult = result;
    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(finalResult)),
        () -> assertEquals(
            String.format("Savepoint \"%s\" rolled back", savepoint), finalResult.toString()));

    // there is 1 restore instant
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.getRestoreTimeline().countInstants());

    // 103 instant had rollback
    assertFalse(timeline.getCommitAndReplaceTimeline().containsInstant(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "103")));
  }

  /**
   * Test case of command 'savepoint rollback' with metadata table bootstrap.
   */
  @Disabled("HUDI-6571")
  public void testRollbackToSavepointWithMetadataTableEnable() throws Exception {
    // generate for savepoints
    for (int i = 101; i < 105; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(
          tablePath, instantTime, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    }

    // generate one savepoint at 102
    String savepoint = "102";
    HoodieTestDataGenerator.createSavepointFile(
        tablePath, savepoint, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    // re-bootstrap metadata table
    StoragePath metadataTableBasePath =
        new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(HoodieCLI.basePath));
    // then bootstrap metadata table at instant 104
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(HoodieCLI.basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
    SparkHoodieBackedTableMetadataWriter.create(HoodieCLI.conf, writeConfig, new HoodieSparkEngineContext(jsc)).close();

    assertTrue(HoodieCLI.storage.exists(metadataTableBasePath));

    // roll back to savepoint
    Object result = shell.evaluate(() ->
            String.format("savepoint rollback --savepoint %s --sparkMaster %s", savepoint, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertEquals(
            String.format("Savepoint \"%s\" rolled back", savepoint), result.toString()));

    // there is 1 restore instant
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.getRestoreTimeline().countInstants());

    // 103 and 104 instant had rollback
    assertFalse(timeline.getCommitAndReplaceTimeline().containsInstant(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "103")));
    assertFalse(timeline.getCommitAndReplaceTimeline().containsInstant(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "104")));
  }

  /**
   * Test case of command 'savepoint delete'.
   */
  @Test
  public void testDeleteSavepoint() throws IOException {
    // generate four savepoints
    for (int i = 100; i < 104; i++) {
      String instantTime = String.valueOf(i);
      HoodieTestDataGenerator.createCommitFile(
          tablePath, instantTime, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    }

    // generate two savepoint
    String savepoint1 = "100";
    String savepoint2 = "102";
    HoodieTestDataGenerator.createSavepointFile(
        tablePath, savepoint1, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    HoodieTestDataGenerator.createSavepointFile(
        tablePath, savepoint2, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(2, timeline.getSavePointTimeline().countInstants(), "There should 2 instants.");

    Object result = shell.evaluate(() ->
            String.format("savepoint delete --commit %s --sparkMaster %s", savepoint1, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertEquals(
            String.format("Savepoint \"%s\" deleted.", savepoint1),result.toString()));

    // reload timeline
    timeline = timeline.reload();
    assertEquals(1, timeline.getSavePointTimeline().countInstants(), "There should 1 instants.");

    // after delete, 100 instant should not exist.
    assertFalse(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepoint1)));
  }
}
