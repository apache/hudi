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

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link TimelineCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestTimelineCommand extends CLIFunctionalTestHarness {

  // Column offsets of the data table part of the rendered timeline, row number included.
  private static final int COL_INSTANT = 1;
  private static final int COL_ACTION = 2;
  private static final int COL_STATE = 3;
  private static final int COL_REQUESTED_TIME = 4;
  private static final int COL_INFLIGHT_TIME = 5;
  private static final int COL_COMPLETED_TIME = 6;
  // Column offsets of the metadata table part, only rendered with --with-metadata-table.
  private static final int COL_MT_ACTION = 7;
  private static final int COL_MT_STATE = 8;

  // The commit left in the requested state, and the rollback scheduled against it.
  private static final String REQUESTED_COMMIT = "103";
  private static final String PENDING_ROLLBACK_INSTANT = "104";
  private static final String ROLLED_BACK_COMMIT = "102";

  private static final String DATE_NO_SECONDS = "\\d{2}-\\d{2} \\d{2}:\\d{2}";
  private static final String DATE_WITH_SECONDS = "\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";

  @Autowired
  private Shell shell;

  private String tablePath;
  private HoodieTableMetaClient metaClient;
  private String rollbackInstantTime;

  /**
   * Builds a table whose active timeline holds two completed commits, a completed rollback of a
   * third commit, one commit left in the requested state and a rollback scheduled against that
   * commit, with the metadata table enabled so that the metadata table timeline is populated too.
   */
  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = storageConf();
    String tableName = tableName();
    tablePath = tablePath(tableName);

    createTableAndConnect(tablePath, tableName, HoodieTableType.COPY_ON_WRITE, HoodieAvroPayload.class.getName());
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());

    Map<String, String> partitionAndFileId = new HashMap<>();
    partitionAndFileId.put(DEFAULT_FIRST_PARTITION_PATH, "file-1");
    partitionAndFileId.put(DEFAULT_SECOND_PARTITION_PATH, "file-2");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withMetadataConfig(
            // Column Stats Index is disabled, since this table is built with empty commit metadata
            HoodieMetadataConfig.newBuilder().withMetadataIndexColumnStats(false).build())
        .withRollbackUsingMarkers(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .build();

    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
        metaClient.getStorageConf(), config, context)) {
      HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context))
          .withPartitionMetaFiles(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH)
          .addCommit("100").withBaseFilesInPartitions(partitionAndFileId).getLeft()
          .addCommit("101").withBaseFilesInPartitions(partitionAndFileId).getLeft()
          .addInflightCommit(ROLLED_BACK_COMMIT);
      testTable.withBaseFilesInPartitions(partitionAndFileId);

      try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
        client.rollback(ROLLED_BACK_COMMIT);
      }
      // left behind on the timeline so that the incomplete timeline is not empty
      testTable.addRequestedCommit(REQUESTED_COMMIT);

      // A rollback that is scheduled but has not run yet. The completed rollback above deleted the
      // commit it targeted, so on this table the scheduled one is what makes the "rolled back by"
      // annotation reachable: it leaves its target on the timeline. Added straight through the test
      // table so that it stays on the data table timeline only.
      HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();
      rollbackPlan.setRollbackRequests(Collections.emptyList());
      rollbackPlan.setInstantToRollback(new HoodieInstantInfo(REQUESTED_COMMIT, HoodieTimeline.COMMIT_ACTION));
      testTable.addRequestedRollback(PENDING_ROLLBACK_INSTANT, rollbackPlan);
      testTable.addInflightRollback(PENDING_ROLLBACK_INSTANT);
    }

    HoodieCLI.refreshTableMetadata();
    metaClient = HoodieCLI.getTableMetaClient();
    rollbackInstantTime = metaClient.getActiveTimeline().getRollbackTimeline()
        .filterCompletedInstants().lastInstant().get().requestedTime();
  }

  @Test
  public void testShowActive() {
    Object result = shell.evaluate(() -> "timeline show active");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    List<List<String>> rows = renderedRows(result.toString());
    assertEquals(instantTimes(metaClient), rows.stream().map(r -> r.get(COL_INSTANT)).collect(Collectors.toSet()));
    assertEquals(metaClient.getActiveTimeline().countInstants(), rows.size());

    List<String> commit100 = rowOf(rows, "100");
    assertEquals("commit", commit100.get(COL_ACTION));
    assertEquals(HoodieInstant.State.COMPLETED.toString(), commit100.get(COL_STATE));
    // a completed commit has all three instant files, so all three modification times are rendered
    assertTrue(commit100.get(COL_REQUESTED_TIME).matches(DATE_NO_SECONDS), commit100.toString());
    assertTrue(commit100.get(COL_INFLIGHT_TIME).matches(DATE_NO_SECONDS), commit100.toString());
    assertTrue(commit100.get(COL_COMPLETED_TIME).matches(DATE_NO_SECONDS), commit100.toString());

    List<String> commit103 = rowOf(rows, REQUESTED_COMMIT);
    assertEquals("commit", commit103.get(COL_ACTION));
    assertEquals(HoodieInstant.State.REQUESTED.toString(), commit103.get(COL_STATE));
    // only the requested file exists for it, the other two states render as a dash
    assertTrue(commit103.get(COL_REQUESTED_TIME).matches(DATE_NO_SECONDS), commit103.toString());
    assertEquals("-", commit103.get(COL_INFLIGHT_TIME));
    assertEquals("-", commit103.get(COL_COMPLETED_TIME));

    assertEquals("rollback", rowOf(rows, rollbackInstantTime).get(COL_ACTION));
  }

  @Test
  public void testShowActiveWithLimitAndSorting() {
    Object result = shell.evaluate(() -> "timeline show active --limit 2 --sortBy Instant --desc true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    List<List<String>> rows = renderedRows(result.toString());
    assertEquals(2, rows.size());
    List<String> allInstants = new ArrayList<>(instantTimes(metaClient));
    allInstants.sort(String::compareTo);
    // descending order on the instant time, cut to the first two rows
    assertEquals(allInstants.get(allInstants.size() - 1), rows.get(0).get(COL_INSTANT));
    assertEquals(allInstants.get(allInstants.size() - 2), rows.get(1).get(COL_INSTANT));
  }

  @Test
  public void testShowActiveHeaderOnly() {
    Object result = shell.evaluate(() -> "timeline show active --headeronly true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    assertTrue(renderedRows(result.toString()).isEmpty(), result.toString());
    assertTrue(result.toString().contains("Instant"), result.toString());
    assertTrue(result.toString().contains(EMPTY_TABLE_CELL), result.toString());
  }

  @Test
  public void testShowActiveWithRollbackInfoAndSeconds() {
    Object result = shell.evaluate(
        () -> "timeline show active --show-rollback-info true --show-time-seconds true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    List<List<String>> rows = renderedRows(result.toString());
    // the completed rollback is annotated with the commit it rolled back, read from its metadata
    assertEquals("rollback Rolls back " + ROLLED_BACK_COMMIT, rowOf(rows, rollbackInstantTime).get(COL_ACTION));
    // the scheduled one with the commit its plan targets
    assertEquals("rollback Rolls back " + REQUESTED_COMMIT,
        rowOf(rows, PENDING_ROLLBACK_INSTANT).get(COL_ACTION));
    // and that commit is annotated back with the rollback scheduled against it
    assertEquals("commit Rolled back by " + PENDING_ROLLBACK_INSTANT,
        rowOf(rows, REQUESTED_COMMIT).get(COL_ACTION));
    // instants that no rollback refers to carry no annotation
    assertEquals("commit", rowOf(rows, "100").get(COL_ACTION));
    assertTrue(rowOf(rows, "100").get(COL_COMPLETED_TIME).matches(DATE_WITH_SECONDS),
        rowOf(rows, "100").toString());
  }

  @Test
  public void testShowIncomplete() {
    Object result = shell.evaluate(() -> "timeline show incomplete");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    List<List<String>> rows = renderedRows(result.toString());
    assertEquals(2, rows.size(), result.toString());

    List<String> requestedCommit = rowOf(rows, REQUESTED_COMMIT);
    assertEquals("commit", requestedCommit.get(COL_ACTION));
    assertEquals(HoodieInstant.State.REQUESTED.toString(), requestedCommit.get(COL_STATE));
    assertEquals("-", requestedCommit.get(COL_COMPLETED_TIME));

    List<String> pendingRollback = rowOf(rows, PENDING_ROLLBACK_INSTANT);
    assertEquals("rollback", pendingRollback.get(COL_ACTION));
    assertEquals(HoodieInstant.State.INFLIGHT.toString(), pendingRollback.get(COL_STATE));
    assertEquals("-", pendingRollback.get(COL_COMPLETED_TIME));
  }

  @Test
  public void testShowActiveWithMetadataTable() {
    Object result = shell.evaluate(
        () -> "timeline show active --with-metadata-table true --show-rollback-info true --limit 50");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    HoodieTableMetaClient mtMetaClient = metadataTableMetaClient();
    Set<String> expected = instantTimes(metaClient);
    expected.addAll(instantTimes(mtMetaClient));

    List<List<String>> rows = renderedRows(result.toString());
    assertEquals(expected, rows.stream().map(r -> r.get(COL_INSTANT)).collect(Collectors.toSet()));

    // the data table columns of a data table only instant, and its empty metadata table columns
    List<String> commit103 = rowOf(rows, REQUESTED_COMMIT);
    assertEquals("commit Rolled back by " + PENDING_ROLLBACK_INSTANT, commit103.get(COL_ACTION));
    assertEquals("-", commit103.get(COL_MT_ACTION));
    assertEquals("-", commit103.get(COL_MT_STATE));

    // every metadata table instant is rendered with its action and state in the metadata columns
    for (HoodieInstant instant : mtMetaClient.getActiveTimeline().getInstants()) {
      List<String> row = rowOf(rows, instant.requestedTime());
      assertEquals(instant.getAction(), row.get(COL_MT_ACTION));
      assertEquals(instant.getState().toString(), row.get(COL_MT_STATE));
    }
  }

  @Test
  public void testMetadataShowActive() {
    Object result = shell.evaluate(() -> "metadata timeline show active --limit 50");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    HoodieTableMetaClient mtMetaClient = metadataTableMetaClient();
    List<List<String>> rows = renderedRows(result.toString());
    assertFalse(rows.isEmpty(), result.toString());
    assertEquals(instantTimes(mtMetaClient), rows.stream().map(r -> r.get(COL_INSTANT)).collect(Collectors.toSet()));
    for (HoodieInstant instant : mtMetaClient.getActiveTimeline().getInstants()) {
      List<String> row = rowOf(rows, instant.requestedTime());
      assertEquals(instant.getAction(), row.get(COL_ACTION));
      assertEquals(instant.getState().toString(), row.get(COL_STATE));
    }
  }

  @Test
  public void testMetadataShowIncomplete() {
    Object result = shell.evaluate(() -> "metadata timeline show incomplete");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // the metadata table is written synchronously, so nothing is left incomplete on its timeline
    assertEquals(0, metadataTableMetaClient().getActiveTimeline().filterInflightsAndRequested().countInstants());
    assertTrue(renderedRows(result.toString()).isEmpty(), result.toString());
  }

  private HoodieTableMetaClient metadataTableMetaClient() {
    return HoodieTableMetaClient.builder()
        .setConf(HoodieCLI.conf.newInstance())
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(tablePath))
        .build();
  }

  private static Set<String> instantTimes(HoodieTableMetaClient metaClient) {
    return HoodieTableMetaClient.reload(metaClient).getActiveTimeline().getInstantsAsStream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toSet());
  }

  private static List<String> rowOf(List<List<String>> rows, String instantTime) {
    return rows.stream().filter(r -> r.get(COL_INSTANT).equals(instantTime)).findFirst()
        .orElseThrow(() -> new AssertionError("No rendered row for instant " + instantTime + " in " + rows));
  }
}
