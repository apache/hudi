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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link CleansCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestCleansCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private URL propsFilePath;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = storageConf();

    String tableName = tableName();
    String tablePath = tablePath(tableName);
    propsFilePath = TestCleansCommand.class.getClassLoader().getResource("clean.properties");

    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    StorageConfiguration<?> conf = HoodieCLI.conf;

    metaClient = HoodieCLI.getTableMetaClient();
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath(), storageConf());
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(storage,
        HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, tablePath);

    // Create four commits
    for (int i = 100; i < 104; i++) {
      String timestamp = String.valueOf(i);
      // Requested Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);
      // Inflight Compaction
      HoodieTestCommitMetadataGenerator.createCompactionAuxiliaryMetadata(tablePath,
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, timestamp), conf);

      Map<String, String> extraCommitMetadata =
          Collections.singletonMap(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, timestamp, conf, fileId1, fileId2,
          Option.empty(), Option.empty(), extraCommitMetadata);
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // reload the timeline and get all the commits before archive
    metaClient.getActiveTimeline().reload();
  }

  /**
   * Test case for show all cleans.
   */
  @Test
  public void testShowCleans() throws Exception {
    // Check properties file exists.
    assertNotNull(propsFilePath, "Not found properties file");

    // First, run clean
    SparkMain.clean(jsc(), HoodieCLI.basePath, propsFilePath.getPath(), new ArrayList<>());
    assertEquals(1, metaClient.getActiveTimeline().reload().getCleanerTimeline().countInstants(),
        "Loaded 1 clean and the count should match");

    Object result = shell.evaluate(() -> "cleans show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    HoodieInstant clean = metaClient.getActiveTimeline().reload().getCleanerTimeline().getInstantsAsStream().findFirst().orElse(null);
    assertNotNull(clean);

    TableHeader header =
        new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_CLEAN_TIME)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_EARLIEST_COMMAND_RETAINED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_DELETED)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_TIME_TAKEN);
    List<Comparable[]> rows = new ArrayList<>();

    // EarliestCommandRetained should be 102, since hoodie.clean.commits.retained=2
    // Total Time Taken need read from metadata
    rows.add(new Comparable[] {clean.requestedTime(), "102", "2", getLatestCleanTimeTakenInMillis().toString()});

    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for show partitions of a clean instant.
   */
  @Test
  public void testShowCleanPartitions() {
    // Check properties file exists.
    assertNotNull(propsFilePath, "Not found properties file");

    // First, run clean with two partition
    SparkMain.clean(jsc(), HoodieCLI.basePath, propsFilePath.toString(), new ArrayList<>());
    assertEquals(1, metaClient.getActiveTimeline().reload().getCleanerTimeline().countInstants(),
        "Loaded 1 clean and the count should match");

    HoodieInstant clean = metaClient.getActiveTimeline().reload().getCleanerTimeline().getInstantsAsStream().findFirst().get();

    Object result = shell.evaluate(() -> "clean showpartitions --clean " + clean.requestedTime());
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_CLEANING_POLICY)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_SUCCESSFULLY_DELETED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FAILED_DELETIONS);

    // There should be two partition path
    List<Comparable[]> rows = new ArrayList<>();
    rows.add(new Comparable[] {HoodieTestCommitMetadataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS, "1", "0"});
    rows.add(new Comparable[] {HoodieTestCommitMetadataGenerator.DEFAULT_THIRD_PARTITION_PATH,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS, "0", "0"});
    rows.add(new Comparable[] {HoodieTestCommitMetadataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS, "1", "0"});

    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Get time taken of latest instant.
   */
  private Long getLatestCleanTimeTakenInMillis() throws IOException {
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    HoodieInstant clean = timeline.getReverseOrderedInstants().findFirst().orElse(null);
    if (clean != null) {
      HoodieCleanMetadata cleanMetadata = timeline.readCleanMetadata(clean);
      return cleanMetadata.getTimeTakenInMillis();
    }
    return -1L;
  }
}
