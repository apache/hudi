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
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.testutils.AbstractShellIntegrationTest;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.CommitsCommand}.
 */
public class TestCommitsCommand extends AbstractShellIntegrationTest {

  private String tableName;
  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    tableName = "test_table";
    tablePath = basePath + File.separator + tableName;

    HoodieCLI.conf = jsc.hadoopConfiguration();
    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  private LinkedHashMap<String, Integer[]> generateData() throws Exception {
    // generate data and metadata
    LinkedHashMap<String, Integer[]> data = new LinkedHashMap<>();
    data.put("102", new Integer[] {15, 10});
    data.put("101", new Integer[] {20, 10});
    data.put("100", new Integer[] {15, 15});

    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, key, jsc.hadoopConfiguration(),
          Option.of(value[0]), Option.of(value[1]));
    }

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should have 3 commits");
    return data;
  }

  private String generateExpectData(int records, Map<String, Integer[]> data) throws IOException {
    FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
    List<String> partitionPaths =
        FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, tablePath);

    int partitions = partitionPaths.size();
    // default pre-commit is not null, file add always be 0 and update always be partition nums
    int fileAdded = 0;
    int fileUpdated = partitions;
    int errors = 0;

    // generate expect result
    List<Comparable[]> rows = new ArrayList<>();
    data.forEach((key, value) -> {
      for (int i = 0; i < records; i++) {
        // there are more than 1 partitions, so need to * partitions
        rows.add(new Comparable[]{key, partitions * HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
            fileAdded, fileUpdated, partitions, partitions * value[0], partitions * value[1], errors});
      }
    });

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    final TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_PARTITIONS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPDATE_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false,
        -1, false, rows);
  }

  /**
   * Test case of 'commits show' command.
   */
  @Test
  public void testShowCommits() throws Exception {
    Map<String, Integer[]> data = generateData();

    CommandResult cr = getShell().executeCommand("commits show");
    assertTrue(cr.isSuccess());

    String expected = generateExpectData(1, data);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of 'commits showarchived' command.
   */
  @Test
  public void testShowArchivedCommits() throws Exception {
    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().retainCommits(1).archiveCommitsWith(2, 3).build())
        .forTable("test-trip-table").build();

    // generate data and metadata
    Map<String, Integer[]> data = new LinkedHashMap<>();
    data.put("104", new Integer[] {20, 10});
    data.put("103", new Integer[] {15, 15});
    data.put("102", new Integer[] {25, 45});
    data.put("101", new Integer[] {35, 15});

    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, key, jsc.hadoopConfiguration(),
          Option.of(value[0]), Option.of(value[1]));
    }

    // archive
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context, metaClient);
    HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(cfg, table);
    archiveLog.archiveIfRequired(context);

    CommandResult cr = getShell().executeCommand(String.format("commits showarchived --startTs %s --endTs %s", "100", "104"));
    assertTrue(cr.isSuccess());

    // archived 101 and 102 instant, generate expect data
    assertEquals(2, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should 2 instants not be archived!");

    // archived 101 and 102 instants, remove 103 and 104 instant
    data.remove("103");
    data.remove("104");
    String expected = generateExpectData(3, data);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of 'commit showpartitions' command.
   */
  @Test
  public void testShowCommitPartitions() throws Exception {
    Map<String, Integer[]> data = generateData();

    String commitInstant = "101";
    CommandResult cr = getShell().executeCommand(String.format("commit showpartitions --commit %s", commitInstant));
    assertTrue(cr.isSuccess());

    Integer[] value = data.get(commitInstant);
    List<Comparable[]> rows = new ArrayList<>();
    // prevCommit not null, so add 0, update 1
    Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
        rows.add(new Comparable[] {partition, 0, 1, 0, value[1], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES, 0})
    );

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_INSERTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of 'commit showfiles' command.
   */
  @Test
  public void testShowCommitFiles() throws Exception {
    Map<String, Integer[]> data = generateData();

    String commitInstant = "101";
    CommandResult cr = getShell().executeCommand(String.format("commit showfiles --commit %s", commitInstant));
    assertTrue(cr.isSuccess());

    Integer[] value = data.get(commitInstant);
    List<Comparable[]> rows = new ArrayList<>();
    Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
        rows.add(new Comparable[] {partition, HoodieTestCommitMetadataGenerator.DEFAULT_FILEID,
            HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT,
            value[1], value[0], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
            // default 0 errors and blank file with 0 size
            0, 0}));
    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_SIZE);

    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of 'commits compare' command.
   */
  @Test
  public void testCompareCommits() throws Exception {
    Map<String, Integer[]> data = generateData();

    String tableName2 = "test_table2";
    String tablePath2 = basePath + File.separator + tableName2;
    HoodieTestUtils.init(jsc.hadoopConfiguration(), tablePath2, getTableType());

    data.remove("102");
    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath2, key, jsc.hadoopConfiguration(),
          Option.of(value[0]), Option.of(value[1]));
    }

    CommandResult cr = getShell().executeCommand(String.format("commits compare --path %s", tablePath2));
    assertTrue(cr.isSuccess());

    // the latest instant of test_table2 is 101
    List<String> commitsToCatchup = metaClient.getActiveTimeline().findInstantsAfter("101", Integer.MAX_VALUE)
        .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    String expected = String.format("Source %s is ahead by %d commits. Commits to catch up - %s",
        tableName, commitsToCatchup.size(), commitsToCatchup);
    assertEquals(expected, cr.getResult().toString());
  }

  /**
   * Test case of 'commits sync' command.
   */
  @Test
  public void testSyncCommits() throws Exception {
    Map<String, Integer[]> data = generateData();

    String tableName2 = "test_table2";
    String tablePath2 = basePath + File.separator + tableName2;
    HoodieTestUtils.init(jsc.hadoopConfiguration(), tablePath2, getTableType(), tableName2);

    data.remove("102");
    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath2, key, jsc.hadoopConfiguration(),
          Option.of(value[0]), Option.of(value[1]));
    }

    CommandResult cr = getShell().executeCommand(String.format("commits sync --path %s", tablePath2));
    assertTrue(cr.isSuccess());

    String expected = String.format("Load sync state between %s and %s", tableName, tableName2);
    assertEquals(expected, cr.getResult().toString());
  }
}
