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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;
import org.apache.hudi.utilities.testutils.CapturingLogAppender;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.utilities.testutils.ToolTestUtils.stackMessages;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieDataTableValidator} against a small three-partition COW table, with and without
 * data files that the timeline does not account for.
 */
public class TestHoodieDataTableValidator extends HoodieSparkClientTestBase {

  private static final int RECORDS_PER_PARTITION = 4;

  private HoodieDataTableValidator.Config validatorConfig(boolean ignoreFailed) {
    HoodieDataTableValidator.Config cfg = new HoodieDataTableValidator.Config();
    cfg.basePath = basePath;
    cfg.parallelism = 2;
    cfg.ignoreFailed = ignoreFailed;
    return cfg;
  }

  private String writeOneCommit() {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      String instantTime = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> records = new ArrayList<>();
      for (String partition : Arrays.asList(
          DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH)) {
        records.addAll(dataGen.generateInsertsForPartition(instantTime, RECORDS_PER_PARTITION, partition));
      }
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      JavaRDD<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
      client.commit(instantTime, writeStatuses);
      return instantTime;
    }
  }

  /**
   * Copies an existing base file of the first partition to a new base file named after {@code instantTime} and a
   * brand new file id, which is exactly the shape of a data file the timeline does not account for.
   */
  private String addUnaccountedBaseFile(String instantTime) throws IOException {
    Path partitionDir = Paths.get(basePath, DEFAULT_FIRST_PARTITION_PATH);
    Path source;
    try (Stream<Path> files = Files.list(partitionDir)) {
      source = files.filter(p -> p.toString().endsWith(".parquet")).findFirst()
          .orElseThrow(() -> new IllegalStateException("no base file written under " + partitionDir));
    }
    String danglingName =
        FSUtils.makeBaseFileName(instantTime, "1-0-1", UUID.randomUUID().toString(), ".parquet");
    Files.copy(source, partitionDir.resolve(danglingName));
    return danglingName;
  }

  @Test
  public void testValidationPassesOnAHealthyTable() {
    writeOneCommit();
    HoodieDataTableValidator validator = new HoodieDataTableValidator(jsc, validatorConfig(false));
    // the validator reports through an exception only, so a clean table is asserted by the absence of one
    assertDoesNotThrow(validator::run);
  }

  @Test
  public void testMissingPropsFileFails() {
    HoodieDataTableValidator.Config cfg = validatorConfig(false);
    cfg.propsFilePath = tempDir.resolve("does-not-exist.properties").toAbsolutePath().toString();
    assertThrows(HoodieIOException.class, () -> new HoodieDataTableValidator(jsc, cfg));
  }

  /**
   * A base file whose instant time precedes the first instant of the active timeline is dangling; whether that
   * fails the job depends on --ignore-failed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDanglingFileBeforeTheActiveTimeline(boolean ignoreFailed) throws IOException {
    writeOneCommit();
    String danglingFile = addUnaccountedBaseFile("00000000000001");

    HoodieDataTableValidator validator = new HoodieDataTableValidator(jsc, validatorConfig(ignoreFailed));
    if (ignoreFailed) {
      // the run survives, but the finding still has to be reported
      List<String> messages;
      try (CapturingLogAppender logs = CapturingLogAppender.attachTo(HoodieDataTableValidator.class)) {
        assertDoesNotThrow(validator::run);
        messages = logs.messages();
      }
      assertTrue(messages.contains(
          "Data table validation failed due to dangling files count 1, found before active timeline"),
          messages.toString());
      assertTrue(messages.stream().anyMatch(m -> m.startsWith("Dangling file: ") && m.endsWith(danglingFile)),
          "the dangling file must be named in " + messages);
      assertTrue(messages.contains("Data table validation failed."), messages.toString());
    } else {
      HoodieException thrown = assertThrows(HoodieException.class, validator::run);
      assertTrue(thrown.getCause() instanceof HoodieValidationException, "got " + thrown.getCause());
      assertTrue(thrown.getCause().getMessage().contains("dangling files 1"), thrown.getCause().getMessage());
    }
  }

  /**
   * A base file carrying the instant time of a completed commit, but absent from that commit's metadata, is an
   * extra file and fails the second check.
   */
  @Test
  public void testExtraFileForCompletedCommitFailsValidation() throws IOException {
    String instantTime = writeOneCommit();
    addUnaccountedBaseFile(instantTime);

    HoodieDataTableValidator validator = new HoodieDataTableValidator(jsc, validatorConfig(false));
    HoodieException thrown = assertThrows(HoodieException.class, validator::run);
    assertTrue(thrown.getCause() instanceof HoodieValidationException, "got " + thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("dangling files 1"), thrown.getCause().getMessage());
    // the table itself is untouched by validation
    assertEquals(1, HoodieTableMetaClient.reload(metaClient).getActiveTimeline()
        .filterCompletedInstants().countInstants());
  }

  /**
   * In continuous mode the async service keeps validating until it fails; with --ignore-failed off the very
   * first round throws, which is what stops the job.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void testContinuousModeStopsOnValidationFailure() throws IOException {
    writeOneCommit();
    addUnaccountedBaseFile("00000000000001");

    HoodieDataTableValidator.Config cfg = validatorConfig(false);
    cfg.continuous = true;
    cfg.minValidateIntervalSeconds = 1;
    HoodieDataTableValidator validator = new HoodieDataTableValidator(jsc, cfg);

    HoodieException thrown = assertThrows(HoodieException.class, validator::run);
    assertTrue(stackMessages(thrown).contains("dangling files 1"), stackMessages(thrown));
  }

  @Test
  public void testConfigEqualsHashCodeAndToString() {
    HoodieDataTableValidator.Config cfg = validatorConfig(true);
    cfg.basePath = "/tmp/table";
    cfg.continuous = true;
    cfg.minValidateIntervalSeconds = 30;

    assertEquals(cfg, cfg);
    assertNotEquals(cfg, null);
    assertNotEquals(cfg, "not a config");
    // a Config straight out of JCommander has no base path yet
    assertEquals(new HoodieDataTableValidator.Config(), new HoodieDataTableValidator.Config());
    assertEquals(new HoodieDataTableValidator.Config().hashCode(), new HoodieDataTableValidator.Config().hashCode());
    // --help is not compared, so it must not be hashed either
    HoodieDataTableValidator.Config askedForHelp = new HoodieDataTableValidator.Config();
    askedForHelp.help = true;
    assertEquals(new HoodieDataTableValidator.Config(), askedForHelp);
    assertEquals(new HoodieDataTableValidator.Config().hashCode(), askedForHelp.hashCode());

    HoodieDataTableValidator.Config same = validatorConfig(true);
    same.basePath = "/tmp/table";
    same.continuous = true;
    same.minValidateIntervalSeconds = 30;
    assertEquals(cfg, same);
    assertEquals(cfg.hashCode(), same.hashCode());

    same.minValidateIntervalSeconds = 60;
    assertNotEquals(cfg, same);
    assertNotEquals(cfg.hashCode(), same.hashCode());

    String printed = cfg.toString();
    assertTrue(printed.contains("--base-path /tmp/table"));
    assertTrue(printed.contains("--continuous true"));
    assertTrue(printed.contains("--ignore-failed true"));
    assertTrue(printed.contains("--min-validate-interval-seconds 30"));
  }
}
