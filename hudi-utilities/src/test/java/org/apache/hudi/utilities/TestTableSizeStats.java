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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;
import org.apache.hudi.utilities.testutils.CapturingLogAppender;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TableSizeStats}. The tool reports through its log, so the assertions read back the lines it
 * logged for the table and for every partition it decided to include.
 */
public class TestTableSizeStats extends HoodieSparkClientTestBase {

  private static final int RECORDS_PER_PARTITION = 4;
  private static final String PARTITION_STATS_PREFIX = "Partition stats [name: ";
  // the tool parses partition names as yyyy/M/d, so a partition from yesterday is one --num-days can select
  private static final String YESTERDAY_PARTITION_PATH =
      LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy/M/d"));

  private static Stream<Arguments> dateIntervalArgs() {
    return Stream.of(
        // everything on or after the start date: the 2016 partition and yesterday's
        Arguments.of("2016/1/1", null, 0L,
            Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, YESTERDAY_PARTITION_PATH)),
        // only the 2015 partitions are before the end date
        Arguments.of(null, "2016/1/1", 0L,
            Arrays.asList(DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH)),
        // half open interval [start, end): the start date is included, the end date is not
        Arguments.of("2015/3/16", "2015/3/17", 0L, Collections.singletonList(DEFAULT_SECOND_PARTITION_PATH)),
        // --num-days walks back from today: only yesterday's partition falls inside a ten day window
        Arguments.of(null, null, 10L, Collections.singletonList(YESTERDAY_PARTITION_PATH)));
  }

  private TableSizeStats.Config statsConfig() {
    TableSizeStats.Config cfg = new TableSizeStats.Config();
    cfg.basePath = basePath;
    cfg.parallelism = 2;
    return cfg;
  }

  private void writeOneCommit(String... partitions) {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      String instantTime = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> records = new ArrayList<>();
      for (String partition : partitions) {
        records.addAll(dataGen.generateInsertsForPartition(instantTime, RECORDS_PER_PARTITION, partition));
      }
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      JavaRDD<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
      client.commit(instantTime, writeStatuses);
    }
  }

  private void writeDefaultPartitions() {
    writeOneCommit(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH);
  }

  private List<String> runAndCollectLogs(TableSizeStats.Config cfg) {
    try (CapturingLogAppender logs = CapturingLogAppender.attachTo(TableSizeStats.class)) {
      new TableSizeStats(jsc, cfg).run();
      return logs.messages();
    }
  }

  private static Set<String> partitionStatHeaders(List<String> messages) {
    return messages.stream().filter(m -> m.startsWith(PARTITION_STATS_PREFIX)).collect(Collectors.toSet());
  }

  private static String lineAfter(List<String> messages, String header) {
    int index = messages.indexOf(header);
    assertTrue(index >= 0 && index + 1 < messages.size(), "missing log line [" + header + "] in " + messages);
    return messages.get(index + 1);
  }

  @Test
  public void testTableAndPartitionStatsCoverEveryPartition() {
    writeDefaultPartitions();
    TableSizeStats.Config cfg = statsConfig();
    cfg.tableStats = true;
    cfg.partitionStats = true;

    List<String> messages = runAndCollectLogs(cfg);

    Set<String> expectedHeaders = Stream.of(
            DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH)
        .map(p -> PARTITION_STATS_PREFIX + p + "]").collect(Collectors.toSet());
    assertEquals(expectedHeaders, partitionStatHeaders(messages));
    for (String header : expectedHeaders) {
      assertEquals("Number of files: 1", lineAfter(messages, header));
    }

    String tableHeader = "Table stats [path: " + basePath + "]";
    assertEquals("Number of files: 3", lineAfter(messages, tableHeader));
    assertTrue(messages.stream().anyMatch(m -> m.matches("Total size: \\d+\\.\\d{2} (B|KB|MB|GB|TB)")),
        "expected a formatted total size in " + messages);
  }

  @Test
  public void testTotalSizeOnlyWhenTableStatsAreOff() {
    writeDefaultPartitions();
    List<String> messages = runAndCollectLogs(statsConfig());

    assertEquals(Collections.emptySet(), partitionStatHeaders(messages),
        "partition stats must stay off unless asked for");
    assertTrue(messages.stream().noneMatch(m -> m.startsWith("Table stats [path: ")));
    assertTrue(messages.stream().anyMatch(m -> m.matches("Total size: \\d+\\.\\d{2} (B|KB|MB|GB|TB)")),
        "expected a formatted total size in " + messages);
  }

  @ParameterizedTest
  @MethodSource("dateIntervalArgs")
  public void testOnlyPartitionsInsideTheDateIntervalAreCounted(String startDate, String endDate, long numDays,
                                                                List<String> expectedPartitions) {
    writeOneCommit(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH,
        YESTERDAY_PARTITION_PATH);
    TableSizeStats.Config cfg = statsConfig();
    cfg.partitionStats = true;
    cfg.startDate = startDate;
    cfg.endDate = endDate;
    cfg.numDays = numDays;

    List<String> messages = runAndCollectLogs(cfg);

    Set<String> expectedHeaders = expectedPartitions.stream()
        .map(p -> PARTITION_STATS_PREFIX + p + ", has date: yes]").collect(Collectors.toSet());
    assertEquals(expectedHeaders, partitionStatHeaders(messages));
  }

  @Test
  public void testBasePathsAreReadFromThePropsFile() throws IOException {
    writeDefaultPartitions();
    Path propsFile = tempDir.resolve("base-paths.properties");
    Files.write(propsFile, Collections.singletonList(basePath), StandardCharsets.UTF_8);

    TableSizeStats.Config cfg = statsConfig();
    cfg.basePath = null;
    cfg.propsFilePath = propsFile.toAbsolutePath().toString();
    cfg.tableStats = true;

    List<String> messages = runAndCollectLogs(cfg);
    assertEquals("Number of files: 3", lineAfter(messages, "Table stats [path: " + basePath + "]"));
  }

  /**
   * --props-path is read twice: once by the constructor as a hoodie properties file, and again by run() as the
   * list of base paths. A file that is missing from the start never reaches the second read.
   */
  @Test
  public void testMissingPropsFileFailsInTheConstructor() {
    TableSizeStats.Config cfg = statsConfig();
    cfg.propsFilePath = tempDir.resolve("missing-" + UUID.randomUUID() + ".properties").toAbsolutePath().toString();
    HoodieIOException thrown =
        assertThrows(HoodieIOException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getMessage().contains("Properties file does not exist"), thrown.getMessage());
  }

  @Test
  public void testPropsFileRemovedAfterTheConstructorFailsTheRun() throws IOException {
    Path propsFile = tempDir.resolve("base-paths.properties");
    Files.write(propsFile, Collections.singletonList(basePath), StandardCharsets.UTF_8);
    TableSizeStats.Config cfg = statsConfig();
    cfg.propsFilePath = propsFile.toAbsolutePath().toString();

    TableSizeStats stats = new TableSizeStats(jsc, cfg);
    Files.delete(propsFile);

    HoodieException thrown = assertThrows(HoodieException.class, stats::run);
    assertTrue(thrown.getCause().getMessage().contains("Cannot read properties from dfs from file"),
        thrown.getCause().getMessage());
  }

  @Test
  public void testMissingBasePathFails() {
    TableSizeStats.Config cfg = statsConfig();
    cfg.basePath = null;
    HoodieException thrown = assertThrows(HoodieException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getCause().getMessage().contains("Base path needs to be set."), thrown.getCause().toString());
  }

  @Test
  public void testDateIntervalOnPartitionsWithoutDatesFails() {
    writeOneCommit("country=us");
    TableSizeStats.Config cfg = statsConfig();
    cfg.numDays = 10;

    HoodieException thrown = assertThrows(HoodieException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getCause().getMessage().contains("Cannot apply --start-date, --end-date, or --num-days"),
        thrown.getCause().getMessage());
    assertTrue(thrown.getCause().getMessage().contains("country=us"), thrown.getCause().getMessage());
  }

  @Test
  public void testStartDateAfterEndDateFails() {
    TableSizeStats.Config cfg = statsConfig();
    cfg.startDate = "2017/1/1";
    cfg.endDate = "2016/1/1";
    HoodieException thrown = assertThrows(HoodieException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getCause().getMessage().contains("Starting date must be before ending date"),
        thrown.getCause().getMessage());
  }

  @Test
  public void testNegativeNumDaysFails() {
    TableSizeStats.Config cfg = statsConfig();
    cfg.numDays = -1;
    HoodieException thrown = assertThrows(HoodieException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getCause().getMessage().contains("--num-days must specify a positive value"),
        thrown.getCause().getMessage());
  }

  @Test
  public void testUnparseableEndDateFails() {
    TableSizeStats.Config cfg = statsConfig();
    cfg.endDate = "yesterday";
    HoodieException thrown = assertThrows(HoodieException.class, () -> new TableSizeStats(jsc, cfg).run());
    assertTrue(thrown.getCause().getMessage().contains("Unable to parse --end-date"),
        thrown.getCause().getMessage());
  }

  @Test
  public void testConfigEqualsHashCodeAndToString() {
    TableSizeStats.Config left = new TableSizeStats.Config();
    left.basePath = "/tmp/table";
    left.numDays = 3;
    left.tableStats = true;
    left.configs = new ArrayList<>(Collections.singletonList("k=v"));

    TableSizeStats.Config right = new TableSizeStats.Config();
    right.basePath = "/tmp/table";
    right.numDays = 3;
    right.tableStats = true;
    right.configs = new ArrayList<>(Collections.singletonList("k=v"));

    assertEquals(left, left);
    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());
    assertNotEquals(left, null);
    assertNotEquals(left, "not a config");
    // a Config straight out of JCommander has no base path yet
    assertEquals(new TableSizeStats.Config(), new TableSizeStats.Config());
    assertEquals(new TableSizeStats.Config().hashCode(), new TableSizeStats.Config().hashCode());

    right.endDate = "2016/1/1";
    assertNotEquals(left, right);
    assertNotEquals(left.hashCode(), right.hashCode());

    String printed = left.toString();
    assertTrue(printed.contains("--base-path /tmp/table"));
    assertTrue(printed.contains("--num-days 3"));
    assertTrue(printed.contains("--enable-table-stats true"));
    assertTrue(printed.contains("--hoodie-conf [k=v]"));
  }
}
