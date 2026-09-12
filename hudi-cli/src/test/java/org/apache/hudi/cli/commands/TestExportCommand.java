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
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ExportCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestExportCommand extends CLIFunctionalTestHarness {

  private static final String[] COMMIT_TIMES = new String[] {"101", "102", "103"};

  @Autowired
  private Shell shell;

  private String tablePath;
  private Path exportFolder;

  @BeforeEach
  public void init() throws Exception {
    HoodieCLI.conf = storageConf();
    String tableName = tableName();
    tablePath = tablePath(tableName);
    exportFolder = Files.createDirectories(Paths.get(basePath(), "exported-instants"));

    createTableAndConnect(tablePath, tableName, HoodieTableType.COPY_ON_WRITE, HoodieAvroPayload.class.getName());
    for (String commitTime : COMMIT_TIMES) {
      HoodieTestCommitMetadataGenerator.createCommitFileWithMetadata(tablePath, commitTime, storageConf());
    }
    HoodieCLI.refreshTableMetadata();
  }

  /**
   * Exports the whole timeline. The instant count is passed as the limit and the ordering is
   * descending on purpose: that is the one shape in which the export does not walk the archived
   * timeline, whose reader cannot open the LSM timeline history directory of a table of version
   * eight or above. The limit is not honoured for active instants either. Both are tracked in
   * https://github.com/apache/hudi/issues/19879; once fixed, this test can drop the workaround.
   */
  @Test
  public void testExportInstants() throws Exception {
    Object result = shell.evaluate(
        () -> "export instants --desc true --limit " + COMMIT_TIMES.length + " --localFolder " + exportFolder);
    assertTrue(ShellEvaluationResultUtil.isSuccess(result), String.valueOf(result));
    assertEquals("Exported " + COMMIT_TIMES.length + " Instants to " + exportFolder, result.toString());

    // one file per completed instant, named after the instant file it was read from
    assertEquals(instantFileNames(COMMIT_TIMES), exportedFiles());

    // The export copies the instant file off the timeline as it stands, in whatever format the
    // table writes its commit metadata in, so it is read back through the table's own serde and
    // compared with what the fixture wrote.
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    Set<String> writtenPartitions = new HashSet<>(
        Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH));
    for (HoodieInstant instant : metaClient.getActiveTimeline().filterCompletedInstants().getInstants()) {
      Map<String, List<HoodieWriteStat>> writeStats = readExportedCommit(metaClient, instant).getPartitionToWriteStats();
      assertEquals(writtenPartitions, writeStats.keySet());
      for (List<HoodieWriteStat> partitionStats : writeStats.values()) {
        assertEquals(1, partitionStats.size());
        assertEquals(HoodieTestCommitMetadataGenerator.DEFAULT_NUM_WRITES, partitionStats.get(0).getNumWrites());
        assertEquals(HoodieTestCommitMetadataGenerator.DEFAULT_PRE_COMMIT, partitionStats.get(0).getPrevCommit());
      }
    }
  }

  /**
   * Reads back the file the export wrote for one instant.
   *
   * @param metaClient Meta client of the exported table.
   * @param instant    Instant whose exported file to read.
   * @return The commit metadata the file holds.
   */
  private HoodieCommitMetadata readExportedCommit(HoodieTableMetaClient metaClient, HoodieInstant instant)
      throws IOException {
    String fileName = metaClient.getInstantFileNameGenerator().getFileName(instant);
    try (InputStream exported = Files.newInputStream(exportFolder.resolve(fileName))) {
      return metaClient.getCommitMetadataSerDe().deserialize(instant, exported, () -> false, HoodieCommitMetadata.class);
    }
  }

  @Test
  public void testExportInstantsToInvalidFolder() {
    String missingFolder = Paths.get(basePath(), "no-such-folder").toString();
    Object result = shell.evaluate(() -> "export instants --localFolder " + missingFolder);
    assertFalse(ShellEvaluationResultUtil.isSuccess(result));
    assertEquals(HoodieException.class, result.getClass());
    assertTrue(result.toString().contains(missingFolder + " is not a valid local directory"), result.toString());
    assertFalse(Files.exists(Paths.get(missingFolder)));
  }

  private Set<String> exportedFiles() {
    try (Stream<Path> files = Files.list(exportFolder)) {
      return files.map(file -> file.getFileName().toString()).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The names of the instant files of the given commits, which are the names the export uses.
   *
   * @param commitTimes Requested times of the commits.
   * @return The instant file names.
   */
  private Set<String> instantFileNames(String... commitTimes) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    List<String> wanted = Arrays.asList(commitTimes);
    return metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(instant -> wanted.contains(instant.requestedTime()))
        .map(instant -> metaClient.getInstantFileNameGenerator().getFileName(instant))
        .collect(Collectors.toSet());
  }
}
