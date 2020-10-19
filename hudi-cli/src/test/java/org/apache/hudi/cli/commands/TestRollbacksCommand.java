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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.RollbacksCommand}.
 */
public class TestRollbacksCommand extends AbstractShellIntegrationTest {

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    String tablePath = basePath + File.separator + tableName;
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    //Create some commits files and parquet files
    String commitTime1 = "100";
    String commitTime2 = "101";
    String commitTime3 = "102";
    HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, tablePath);

    // two commit files
    HoodieTestUtils.createCommitFiles(tablePath, commitTime1, commitTime2);
    // one .inflight commit file
    HoodieTestUtils.createInflightCommitFiles(tablePath, commitTime3);

    // generate commit files for commits
    for (String commitTime : Arrays.asList(commitTime1, commitTime2, commitTime3)) {
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime, "file-1");
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, commitTime, "file-2");
      HoodieTestUtils.createDataFile(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, commitTime, "file-3");
    }

    // generate two rollback
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    try (HoodieWriteClient client = getHoodieWriteClient(config)) {
      // Rollback inflight commit3 and commit2
      client.rollback(commitTime3);
      client.rollback(commitTime2);
    }
  }

  /**
   * Test case for command 'show rollbacks'.
   */
  @Test
  public void testShowRollbacks() {
    CommandResult cr = getShell().executeCommand("show rollbacks");
    assertTrue(cr.isSuccess());

    // get rollback instants
    HoodieActiveTimeline activeTimeline = new RollbacksCommand.RollbackTimeline(HoodieCLI.getTableMetaClient());
    Stream<HoodieInstant> rollback = activeTimeline.getRollbackTimeline().filterCompletedInstants().getInstants();

    List<Comparable[]> rows = new ArrayList<>();
    rollback.sorted().forEach(instant -> {
      try {
        // get pair of rollback time and instant time
        HoodieRollbackMetadata metadata = TimelineMetadataUtils
            .deserializeAvroMetadata(activeTimeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
        metadata.getCommitsRollback().forEach(c -> {
          Comparable[] row = new Comparable[5];
          row[0] = metadata.getStartRollbackTime();
          row[1] = c;
          // expect data
          row[2] = 3;
          row[3] = metadata.getTimeTakenInMillis();
          row[4] = 3;
          rows.add(row);
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ROLLBACK_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_DELETED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_PARTITIONS);
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for command 'show rollback'.
   */
  @Test
  public void testShowRollback() throws IOException {
    // get instant
    HoodieActiveTimeline activeTimeline = new RollbacksCommand.RollbackTimeline(HoodieCLI.getTableMetaClient());
    Stream<HoodieInstant> rollback = activeTimeline.getRollbackTimeline().filterCompletedInstants().getInstants();
    HoodieInstant instant = rollback.findFirst().orElse(null);
    assertNotNull(instant, "The instant can not be null.");

    CommandResult cr = getShell().executeCommand("show rollback --instant " + instant.getTimestamp());
    assertTrue(cr.isSuccess());

    List<Comparable[]> rows = new ArrayList<>();
    // get metadata of instant
    HoodieRollbackMetadata metadata = TimelineMetadataUtils.deserializeAvroMetadata(
        activeTimeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
    // generate expect result
    metadata.getPartitionMetadata().forEach((key, value) -> Stream
        .concat(value.getSuccessDeleteFiles().stream().map(f -> Pair.of(f, true)),
            value.getFailedDeleteFiles().stream().map(f -> Pair.of(f, false)))
        .forEach(fileWithDeleteStatus -> {
          Comparable[] row = new Comparable[5];
          row[0] = metadata.getStartRollbackTime();
          row[1] = metadata.getCommitsRollback().toString();
          row[2] = key;
          row[3] = fileWithDeleteStatus.getLeft();
          row[4] = fileWithDeleteStatus.getRight();
          rows.add(row);
        }));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ROLLBACK_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELETED_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_SUCCEEDED);
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }
}
