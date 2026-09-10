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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;
import org.apache.hudi.utilities.testutils.CapturingLogAppender;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.utilities.testutils.ToolTestUtils.latestBaseFileCount;
import static org.apache.hudi.utilities.testutils.ToolTestUtils.stackMessages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieDropPartitionsTool} against a small three-partition COW table.
 */
public class TestHoodieDropPartitionsTool extends HoodieSparkClientTestBase {

  private static final int RECORDS_PER_PARTITION = 4;

  private HoodieDropPartitionsTool.Config toolConfig(String mode, String partitions) {
    HoodieDropPartitionsTool.Config cfg = new HoodieDropPartitionsTool.Config();
    cfg.basePath = basePath;
    cfg.tableName = metaClient.getTableConfig().getTableName();
    cfg.runningMode = mode;
    cfg.partitions = partitions;
    cfg.parallelism = 2;
    cfg.configs.add(HoodieWriteConfig.TBL_NAME.key() + "=" + cfg.tableName);
    return cfg;
  }

  /**
   * Writes two insert commits: the first spreads records over all three partitions, the second adds a
   * second file slice to the first partition.
   */
  private void writeThreePartitionTable() {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      String firstCommit = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> firstBatch = new ArrayList<>();
      for (String partition : Arrays.asList(
          DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH)) {
        firstBatch.addAll(dataGen.generateInsertsForPartition(firstCommit, RECORDS_PER_PARTITION, partition));
      }
      writeBatchAndCommit(client, firstCommit, firstBatch);

      String secondCommit = WriteClientTestUtils.createNewInstantTime();
      writeBatchAndCommit(client, secondCommit,
          dataGen.generateInsertsForPartition(secondCommit, RECORDS_PER_PARTITION, DEFAULT_FIRST_PARTITION_PATH));
    }
  }

  private void writeBatchAndCommit(SparkRDDWriteClient client, String instantTime, List<HoodieRecord> records) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    JavaRDD<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
    client.commit(instantTime, writeStatuses);
  }

  private List<String> latestFileIds(String partition) {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
        context, reloaded, HoodieMetadataConfig.newBuilder().enable(false).build())) {
      return fsView.getLatestBaseFiles(partition).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    }
  }

  private List<String> completedInstants() {
    return HoodieTableMetaClient.reload(metaClient).getActiveTimeline().filterCompletedInstants()
        .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  @Test
  public void testDryRunReportsTheFilesItWouldDeleteAndLeavesTheTableUntouched() {
    writeThreePartitionTable();
    List<String> instantsBefore = completedInstants();
    // what the tool prints must be the file ids the two partitions really hold
    Set<String> expectedReport = new HashSet<>(Arrays.asList(
        "Partitions : " + DEFAULT_FIRST_PARTITION_PATH + ", corresponding data file IDs : "
            + latestFileIds(DEFAULT_FIRST_PARTITION_PATH),
        "Partitions : " + DEFAULT_SECOND_PARTITION_PATH + ", corresponding data file IDs : "
            + latestFileIds(DEFAULT_SECOND_PARTITION_PATH)));

    HoodieDropPartitionsTool.Config cfg = toolConfig("dry_run",
        DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH);
    List<String> messages;
    try (CapturingLogAppender logs = CapturingLogAppender.attachTo(HoodieDropPartitionsTool.class)) {
      new HoodieDropPartitionsTool(jsc, cfg).run();
      messages = logs.messages();
    }

    assertTrue(messages.contains("Data files and partitions to delete : "), messages.toString());
    assertEquals(expectedReport,
        messages.stream().filter(m -> m.startsWith("Partitions : ")).collect(Collectors.toSet()));
    assertTrue(messages.stream().noneMatch(m -> m.contains(DEFAULT_THIRD_PARTITION_PATH)),
        "the partition that was not named must not be reported: " + messages);

    assertEquals(instantsBefore, completedInstants(), "dry run must not add any instant");
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_SECOND_PARTITION_PATH));
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_THIRD_PARTITION_PATH));
  }

  @Test
  public void testDeleteMasksOnlyTheRequestedPartitions() throws IOException {
    writeThreePartitionTable();
    int instantsBefore = completedInstants().size();

    HoodieDropPartitionsTool.Config cfg = toolConfig("delete",
        DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH);
    new HoodieDropPartitionsTool(jsc, cfg).run();

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertEquals(instantsBefore + 1, completedInstants().size(), "delete must add exactly one instant");
    HoodieInstant replaceInstant = reloaded.getActiveTimeline().getCompletedReplaceTimeline().lastInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata =
        reloaded.getActiveTimeline().readReplaceCommitMetadata(replaceInstant);
    assertEquals(
        new HashSet<>(Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH)),
        replaceMetadata.getPartitionToReplaceFileIds().keySet());
    // the file group of the first partition, written by both commits, is masked
    assertEquals(1, replaceMetadata.getPartitionToReplaceFileIds().get(DEFAULT_FIRST_PARTITION_PATH).size());

    assertEquals(0, latestBaseFileCount(context, metaClient, DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(0, latestBaseFileCount(context, metaClient, DEFAULT_SECOND_PARTITION_PATH));
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_THIRD_PARTITION_PATH),
        "the partition that was not named must survive");
  }

  /**
   * The tool takes its write properties either from --props or from repeated --hoodie-conf, and only defaults
   * hoodie.meta.fields.mode from the table when the operator did not name it. Both sources are checked by asking
   * for a meta-fields mode the table does not have and expecting the write config gate to reject it.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWritePropertiesComeFromPropsFileAndHoodieConf(boolean usePropsFile) throws IOException {
    writeThreePartitionTable();

    HoodieDropPartitionsTool.Config cfg = toolConfig("dry_run", DEFAULT_THIRD_PARTITION_PATH);
    String metaFieldsOverride = "hoodie.meta.fields.mode=NONE";
    if (usePropsFile) {
      // the file carries the mode, the --hoodie-conf entry already on the config carries the table name, so
      // both sources have to be merged for this run to reach the write config gate
      Path propsFile = tempDir.resolve("drop-partitions.properties");
      Files.write(propsFile, Collections.singletonList(metaFieldsOverride), StandardCharsets.UTF_8);
      cfg.propsFilePath = propsFile.toAbsolutePath().toString();
    } else {
      cfg.configs.add(metaFieldsOverride);
    }

    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);
    Throwable thrown = assertThrows(HoodieException.class, tool::run);
    assertTrue(stackMessages(thrown).contains("hoodie.meta.fields.mode"),
        "expected the meta fields mode from the config source to reach the write config, got: " + thrown);
  }

  @Test
  public void testUnsupportedModeFails() {
    writeThreePartitionTable();
    HoodieDropPartitionsTool.Config cfg = toolConfig("purge", DEFAULT_THIRD_PARTITION_PATH);
    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);

    HoodieException thrown = assertThrows(HoodieException.class, tool::run);
    assertTrue(thrown.getMessage().contains("Unable to delete table partitions in " + basePath));
    assertTrue(thrown.getCause() instanceof IllegalArgumentException, "got " + thrown.getCause());
    assertEquals(0, HoodieTableMetaClient.reload(metaClient).getActiveTimeline()
        .getCompletedReplaceTimeline().countInstants());
  }

  /**
   * A missing --hive-database is caught before the delete runs, so the partitions are still there afterwards.
   */
  @Test
  public void testHiveSyncConfigIsVerifiedBeforeTheDrop() {
    writeThreePartitionTable();
    HoodieDropPartitionsTool.Config cfg = toolConfig("delete", DEFAULT_THIRD_PARTITION_PATH);
    cfg.syncToHive = true;
    cfg.hiveDataBase = null;
    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);

    HoodieException thrown = assertThrows(HoodieException.class, tool::run);
    assertTrue(thrown.getCause() instanceof IllegalArgumentException, "got " + thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("--hive-database"));
    assertEquals(0, HoodieTableMetaClient.reload(metaClient).getActiveTimeline()
        .getCompletedReplaceTimeline().countInstants(), "nothing may be dropped once the hive configs are bad");
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_THIRD_PARTITION_PATH));
  }

  /**
   * With the hive configs in place the sync props are built and the sync is attempted for real; pointing it at a
   * port nothing listens on keeps the test free of a metastore. The drop is committed before that attempt, so a
   * metastore that is down costs the sync, not the partitions.
   */
  @Test
  public void testHiveSyncFailureLeavesTheDropCommitted() {
    // the tool feeds the FileSystem's hadoop conf into the HiveConf, which is the only way in for these
    jsc.hadoopConfiguration().set("hive.metastore.connect.retries", "1");
    jsc.hadoopConfiguration().set("hive.metastore.client.connect.retry.delay", "0s");
    jsc.hadoopConfiguration().set("hive.metastore.failure.retries", "0");
    writeThreePartitionTable();
    HoodieDropPartitionsTool.Config cfg = toolConfig("delete", DEFAULT_THIRD_PARTITION_PATH);
    cfg.syncToHive = true;
    cfg.hiveDataBase = "db";
    cfg.hiveTableName = "tbl";
    cfg.hivePartitionsField = "partition_path";
    cfg.hiveHMSUris = "thrift://localhost:1";
    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);

    HoodieException thrown = assertThrows(HoodieException.class, tool::run);
    assertTrue(thrown.getCause() instanceof HoodieHiveSyncException, "got " + thrown.getCause());
    assertTrue(stackMessages(thrown).contains("Failed to create HiveMetaStoreClient"), stackMessages(thrown));
    assertTrue(stackMessages(thrown).contains("Could not connect to meta store using any of the URIs provided"),
        stackMessages(thrown));

    assertEquals(1, HoodieTableMetaClient.reload(metaClient).getActiveTimeline()
        .getCompletedReplaceTimeline().countInstants(), "the drop is committed before hive sync runs");
    assertEquals(0, latestBaseFileCount(context, metaClient, DEFAULT_THIRD_PARTITION_PATH));
  }

  /**
   * Dry run touches no partition, so it does not need the hive configs to be sound: it still prints its listing.
   */
  @Test
  public void testDryRunDoesNotNeedHiveConfigs() {
    writeThreePartitionTable();
    List<String> instantsBefore = completedInstants();
    HoodieDropPartitionsTool.Config cfg = toolConfig("dry_run", DEFAULT_THIRD_PARTITION_PATH);
    cfg.syncToHive = true;
    cfg.hiveDataBase = null;

    List<String> messages;
    try (CapturingLogAppender logs = CapturingLogAppender.attachTo(HoodieDropPartitionsTool.class)) {
      new HoodieDropPartitionsTool(jsc, cfg).run();
      messages = logs.messages();
    }

    assertEquals(
        Collections.singleton("Partitions : " + DEFAULT_THIRD_PARTITION_PATH + ", corresponding data file IDs : "
            + latestFileIds(DEFAULT_THIRD_PARTITION_PATH)),
        messages.stream().filter(m -> m.startsWith("Partitions : ")).collect(Collectors.toSet()));
    assertEquals(instantsBefore, completedInstants(), "dry run must not add any instant");
  }

  /**
   * The partition fields are written into the sync props verbatim; empty, hive sync silently skips every
   * partition, so the tool refuses the run rather than dropping partitions the metastore never hears about.
   */
  @Test
  public void testHiveSyncWithoutPartitionFieldIsRejectedBeforeTheDrop() {
    writeThreePartitionTable();
    HoodieDropPartitionsTool.Config cfg = toolConfig("delete", DEFAULT_THIRD_PARTITION_PATH);
    cfg.syncToHive = true;
    cfg.hiveDataBase = "db";
    cfg.hiveTableName = "tbl";
    // cfg.hivePartitionsField is left at its default, the empty string
    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);

    HoodieException thrown = assertThrows(HoodieException.class, tool::run);
    assertTrue(thrown.getCause() instanceof IllegalArgumentException, "got " + thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("--hive-partition-field"),
        thrown.getCause().getMessage());
    assertEquals(0, HoodieTableMetaClient.reload(metaClient).getActiveTimeline()
        .getCompletedReplaceTimeline().countInstants(), "nothing may be dropped once the hive configs are bad");
    assertEquals(1, latestBaseFileCount(context, metaClient, DEFAULT_THIRD_PARTITION_PATH));
  }

  @Test
  public void testConfigEqualsHashCodeAndToString() {
    HoodieDropPartitionsTool.Config left = new HoodieDropPartitionsTool.Config();
    left.basePath = "/tmp/table";
    left.runningMode = "delete";
    left.tableName = "t1";
    left.partitions = "p1,p2";
    left.configs = new ArrayList<>(Collections.singletonList("k=v"));

    HoodieDropPartitionsTool.Config right = new HoodieDropPartitionsTool.Config();
    right.basePath = "/tmp/table";
    right.runningMode = "delete";
    right.tableName = "t1";
    right.partitions = "p1,p2";
    right.configs = new ArrayList<>(Collections.singletonList("k=v"));

    assertEquals(left, left);
    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());
    assertNotEquals(left, null);
    assertNotEquals(left, "not a config");
    // a Config straight out of JCommander has no base path yet
    assertEquals(new HoodieDropPartitionsTool.Config(), new HoodieDropPartitionsTool.Config());
    assertEquals(new HoodieDropPartitionsTool.Config().hashCode(), new HoodieDropPartitionsTool.Config().hashCode());
    // --help is not compared, so it must not be hashed either
    HoodieDropPartitionsTool.Config askedForHelp = new HoodieDropPartitionsTool.Config();
    askedForHelp.help = true;
    assertEquals(new HoodieDropPartitionsTool.Config(), askedForHelp);
    assertEquals(new HoodieDropPartitionsTool.Config().hashCode(), askedForHelp.hashCode());

    right.hiveDataBase = "db";
    assertNotEquals(left, right);
    assertNotEquals(left.hashCode(), right.hashCode());

    String printed = left.toString();
    assertTrue(printed.contains("--base-path /tmp/table"));
    assertTrue(printed.contains("--partitions p1,p2"));
    assertTrue(printed.contains("--hoodie-conf [k=v]"));
    assertTrue(printed.contains("--hive-user-name Masked"), "credentials must not be printed");
  }
}
