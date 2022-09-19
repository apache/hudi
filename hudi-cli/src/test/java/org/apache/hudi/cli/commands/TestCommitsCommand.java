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
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.HoodieTestReplaceCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createCompactionCommitInMetadataTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.CommitsCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestCommitsCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String tableName1;
  private String tableName2;
  private String tablePath1;
  private String tablePath2;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void init() throws IOException {
    tableName1 = tableName("_1");
    tableName2 = tableName("_2");
    tablePath1 = tablePath(tableName1);
    tablePath2 = tablePath(tableName2);
    HoodieCLI.conf = hadoopConf();
    // Create table and connect
    new TableCommand().createTable(
        tablePath1, tableName1, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
    metaClient = HoodieCLI.getTableMetaClient();
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
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath1, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
    }

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should have 3 commits");
    return data;
  }

  /*
   * generates both replace commit and commit data
   * */
  private LinkedHashMap<HoodieInstant, Integer[]> generateMixedData() throws Exception {
    // generate data and metadata
    LinkedHashMap<HoodieInstant, Integer[]> replaceCommitData = new LinkedHashMap<>();
    replaceCommitData.put(new HoodieInstant(false, HoodieTimeline.REPLACE_COMMIT_ACTION, "103"), new Integer[] {15, 10});

    LinkedHashMap<HoodieInstant, Integer[]> commitData = new LinkedHashMap<>();
    commitData.put(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102"), new Integer[] {15, 10});
    commitData.put(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101"), new Integer[] {20, 10});

    for (Map.Entry<HoodieInstant, Integer[]> entry : commitData.entrySet()) {
      String key = entry.getKey().getTimestamp();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath1, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
    }

    for (Map.Entry<HoodieInstant, Integer[]> entry : replaceCommitData.entrySet()) {
      String key = entry.getKey().getTimestamp();
      Integer[] value = entry.getValue();
      HoodieTestReplaceCommitMetadataGenerator.createReplaceCommitFileWithMetadata(tablePath1, key,
          Option.of(value[0]), Option.of(value[1]), metaClient);
    }

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should be 3 commits");

    LinkedHashMap<HoodieInstant, Integer[]> data = replaceCommitData;
    data.putAll(commitData);

    return data;
  }

  private String generateExpectData(int records, Map<String, Integer[]> data) throws IOException {
    FileSystem fs = FileSystem.get(hadoopConf());
    List<String> partitionPaths =
        FSUtils.getAllPartitionFoldersThreeLevelsDown(fs, tablePath1);

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
        rows.add(new Comparable[] {key, partitions * HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
            fileAdded, fileUpdated, partitions, partitions * value[0], partitions * value[1], errors});
      }
    });

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeader(), fieldNameToConverterMap, "", false,
        -1, false, rows);
  }

  /**
   * Test case of 'commits show' command.
   */
  @Test
  public void testShowCommits() throws Exception {
    Map<String, Integer[]> data = generateData();

    Object result = shell.evaluate(() -> "commits show");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    String expected = generateExpectData(1, data);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  @Test
  public void testShowCommitsIncludingArchivedTimeline() throws Exception {
    Map<String, Integer[]> data = generateDataAndArchive(true);
    data.remove("101");
    data.remove("102");

    Object result = shell.evaluate(() -> "commits show --includeExtraMetadata true --includeArchivedTimeline true --partition 2015/03/16");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    String expected = generateExpectDataWithExtraMetadata(1, data);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  private String generateExpectDataWithExtraMetadata(int records, Map<String, Integer[]> data) throws IOException {
    List<Comparable[]> rows = new ArrayList<>();
    data.forEach((key, value) -> {
      for (int i = 0; i < records; i++) {
        // there are more than 1 partitions, so need to * partitions
        rows.add(new Comparable[] {HoodieTimeline.COMMIT_ACTION, key, "2015/03/16", HoodieTestCommitMetadataGenerator.DEFAULT_FILEID,
            HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT, key.equals("104") ? "20" : "15", "0", "0", key.equals("104") ? "10" : "15",
            "0", HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_BLOCKS, "0", "0", HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_LOG_RECORDS,
            "0", HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES});
      }
    });

    final Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN, entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    final TableHeader header = HoodieTableHeaderFields.getTableHeaderWithExtraMetadata();

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false,
        -1, false, rows);
  }

  /**
   * Test case of 'commits showarchived' command.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testShowArchivedCommits(boolean enableMetadataTable) throws Exception {
    Map<String, Integer[]> data = generateDataAndArchive(enableMetadataTable);

    Object result = shell.evaluate(() -> String.format("commits showarchived --startTs %s --endTs %s", "100", "104"));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // archived 101 and 102 instant, generate expect data
    assertEquals(2, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should 2 instants not be archived!");

    // archived 101 and 102 instants, remove 103 and 104 instant
    data.remove("103");
    data.remove("104");
    String expected = generateExpectData(1, data);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  private Map<String, Integer[]> generateDataAndArchive(boolean enableMetadataTable) throws Exception {
    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath1)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
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
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath1, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
    }

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf(), metaClient.getFs(), tablePath1, "104");
    }

    // archive
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
    archiver.archiveIfRequired(context());
    return data;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testShowArchivedCommitsWithMultiCommitsFile(boolean enableMetadataTable) throws Exception {
    // Generate archive
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath1)
        .withSchema(HoodieTestCommitMetadataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .forTable("test-trip-table").build();

    // generate data and metadata
    Map<String, Integer[]> data = new LinkedHashMap<>();

    for (int i = 194; i >= 154; i--) {
      data.put(String.valueOf(i), new Integer[] {i, i});
    }

    if (enableMetadataTable) {
      // Simulate a compaction commit in metadata table timeline
      // so the archival in data table can happen
      createCompactionCommitInMetadataTable(hadoopConf(), metaClient.getFs(), tablePath1, "194");
    }

    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath1, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
      // archive
      metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
      HoodieSparkTable table = HoodieSparkTable.create(cfg, context(), metaClient);

      // need to create multi archive files
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(cfg, table);
      archiver.archiveIfRequired(context());
    }

    Object result = shell.evaluate(() -> String.format("commits showarchived --startTs %s --endTs %s", "160", "174"));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitsTimeline().countInstants(),
        "There should 3 instants not be archived!");

    Map<String, Integer[]> data2 = new LinkedHashMap<>();
    for (int i = 174; i >= 161; i--) {
      data2.put(String.valueOf(i), new Integer[] {i, i});
    }
    String expected = generateExpectData(1, data2);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case of 'commit showpartitions' command.
   */
  @Test
  public void testShowCommitPartitions() throws Exception {
    Map<String, Integer[]> data = generateData();

    String commitInstant = "101";
    Object result = shell.evaluate(() -> String.format("commit showpartitions --commit %s", commitInstant));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    Integer[] value = data.get(commitInstant);
    List<Comparable[]> rows = new ArrayList<>();
    // prevCommit not null, so add 0, update 1
    Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
        rows.add(new Comparable[] {HoodieTimeline.COMMIT_ACTION, partition, 0, 1, 0, value[1], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES, 0})
    );

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_INSERTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  @Test
  public void testShowCommitPartitionsWithReplaceCommits() throws Exception {
    Map<HoodieInstant, Integer[]> data = generateMixedData();

    for (HoodieInstant commitInstant : data.keySet()) {
      Object result = shell.evaluate(() ->
              String.format("commit showpartitions --commit %s", commitInstant.getTimestamp()));

      assertTrue(ShellEvaluationResultUtil.isSuccess(result));

      Integer[] value = data.get(commitInstant);
      List<Comparable[]> rows = new ArrayList<>();
      // prevCommit not null, so add 0, update 1
      Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
          HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
          rows.add(new Comparable[] {commitInstant.getAction(), partition, 0, 1, 0, value[1], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES, 0})
      );

      Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
      fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
          entry -> NumericUtils.humanReadableByteCount((Long.parseLong(entry.toString()))));

      TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_INSERTED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);

      String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
      expected = removeNonWordAndStripSpace(expected);
      String got = removeNonWordAndStripSpace(result.toString());
      assertEquals(expected, got);
    }
  }

  /**
   * Test case of 'commit showfiles' command.
   */
  @Test
  public void testShowCommitFiles() throws Exception {
    Map<String, Integer[]> data = generateData();

    String commitInstant = "101";
    Object result = shell.evaluate(() -> String.format("commit showfiles --commit %s", commitInstant));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    Integer[] value = data.get(commitInstant);
    List<Comparable[]> rows = new ArrayList<>();
    Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
        rows.add(new Comparable[] {HoodieTimeline.COMMIT_ACTION, partition, HoodieTestCommitMetadataGenerator.DEFAULT_FILEID,
            HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT,
            value[1], value[0], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
            // default 0 errors and blank file with 0 size
            0, 0}));
    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_SIZE);

    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  @Test
  public void testShowCommitFilesWithReplaceCommits() throws Exception {
    Map<HoodieInstant, Integer[]> data = generateMixedData();

    for (HoodieInstant commitInstant : data.keySet()) {
      Object result = shell.evaluate(() -> String.format("commit showfiles --commit %s", commitInstant.getTimestamp()));
      assertTrue(ShellEvaluationResultUtil.isSuccess(result));

      Integer[] value = data.get(commitInstant);
      List<Comparable[]> rows = new ArrayList<>();
      Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
          HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).stream().forEach(partition ->
          rows.add(new Comparable[] {commitInstant.getAction(), partition, HoodieTestCommitMetadataGenerator.DEFAULT_FILEID,
              HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT,
              value[1], value[0], HoodieTestCommitMetadataGenerator.DEFAULT_TOTAL_WRITE_BYTES,
              // default 0 errors and blank file with 0 size
              0, 0}));
      TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION_PATH)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_UPDATED)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
          .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_SIZE);

      String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1, false, rows);
      expected = removeNonWordAndStripSpace(expected);
      String got = removeNonWordAndStripSpace(result.toString());
      assertEquals(expected, got);
    }
  }

  /**
   * Test case of 'commits compare' command.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testCompareCommits(HoodieTableType tableType) throws Exception {
    Map<String, Integer[]> data = generateData();
    HoodieTestUtils.init(hadoopConf(), tablePath2, tableType);

    data.remove("102");
    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath2, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
    }

    Object result = shell.evaluate(() -> String.format("commits compare --path %s", tablePath2));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // the latest instant of test_table2 is 101
    List<String> commitsToCatchup = metaClient.getActiveTimeline().findInstantsAfter("101", Integer.MAX_VALUE)
        .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    String expected = String.format("Source %s is ahead by %d commits. Commits to catch up - %s",
        tableName1, commitsToCatchup.size(), commitsToCatchup);
    assertEquals(expected, result.toString());
  }

  /**
   * Test case of 'commits sync' command.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testSyncCommits(HoodieTableType tableType) throws Exception {
    Map<String, Integer[]> data = generateData();

    HoodieTestUtils.init(hadoopConf(), tablePath2, tableType, tableName2);

    data.remove("102");
    for (Map.Entry<String, Integer[]> entry : data.entrySet()) {
      String key = entry.getKey();
      Integer[] value = entry.getValue();
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath2, key, hadoopConf(),
          Option.of(value[0]), Option.of(value[1]));
    }

    Object result = shell.evaluate(() -> String.format("commits sync --path %s", tablePath2));
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    String expected = String.format("Load sync state between %s and %s", tableName1, tableName2);
    assertEquals(expected, result.toString());
  }
}
