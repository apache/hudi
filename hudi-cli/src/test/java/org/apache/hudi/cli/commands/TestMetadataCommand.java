/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.Assertions;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestMetadataCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;
  private String tableName;
  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    tableName = tableName();
    tablePath = tablePath(tableName);
    HoodieCLI.conf = storageConf();
  }

  @Test
  public void testMetadataDelete() throws Exception {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName(tableName())
        .setArchiveLogFolder(HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue())
        .setPayloadClassName("org.apache.hudi.common.model.HoodieAvroPayload")
        .setPartitionFields("partition_path")
        .setRecordKeyFields("_row_key")
        .setKeyGeneratorClassProp(SimpleKeyGenerator.class.getCanonicalName())
        .initTable(HoodieCLI.conf.newInstance(), tablePath);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath).withSchema(TRIP_EXAMPLE_SCHEMA).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      String newCommitTime = "001";
      int numRecords = 10;
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc().parallelize(result));
      Assertions.assertNoWriteErrors(result);
    }

    // verify that metadata partitions are filled in as part of table config.
    HoodieTableMetaClient metaClient = createMetaClient(jsc(), tablePath);
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().isEmpty());

    new TableCommand().connect(tablePath,  false, 0, 0, 0,
        "WAIT_TO_ADJUST_SKEW", 200L, false);
    Object result = shell.evaluate(() -> "metadata delete");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().isEmpty());
  }

  @Test
  public void testGetRecordIndexInfo() throws Exception {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName(tableName())
        .setArchiveLogFolder(HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue())
        .setPayloadClassName("org.apache.hudi.common.model.HoodieAvroPayload")
        .setPartitionFields("partition_path")
        .setRecordKeyFields("_row_key")
        .setKeyGeneratorClassProp(SimpleKeyGenerator.class.getCanonicalName())
        .initTable(HoodieCLI.conf.newInstance(), tablePath);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .withEnableGlobalRecordLevelIndex(true).build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tablePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMetadataConfig(metadataConfig)
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      String newCommitTime = client.startCommit();
      int numRecords = 10;

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc().parallelize(result));
      Assertions.assertNoWriteErrors(result);

      // Get a record key from the inserted records
      String recordKey = records.get(0).getRecordKey();

      // Connect to the table
      new TableCommand().connect(tablePath, false, 0, 0, 0,
          "WAIT_TO_ADJUST_SKEW", 200L, false);

      // Verify record index is enabled in table config
      HoodieTableMetaClient metaClient = createMetaClient(jsc(), tablePath);
      assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX));

      // Validate entries in the Global RLI.
      validateRecordIndexOutput(recordKey, Option.empty(), newCommitTime, DEFAULT_FIRST_PARTITION_PATH);

      // Test non-existent record key
      validateNonExistentRecordKey("non_existent_key", Option.empty());
    }
  }

  @Test
  public void testGetRecordIndexInfoForPartitionedRLI() throws Exception {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName(tableName())
        .setArchiveLogFolder(HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue())
        .setPayloadClassName("org.apache.hudi.common.model.HoodieAvroPayload")
        .setPartitionFields("partition_path")
        .setRecordKeyFields("_row_key")
        .setKeyGeneratorClassProp(SimpleKeyGenerator.class.getCanonicalName())
        .initTable(HoodieCLI.conf.newInstance(), tablePath);

    HoodieTestDataGenerator firstDataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
    HoodieTestDataGenerator secondDataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_SECOND_PARTITION_PATH});
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .withEnableRecordLevelIndex(true).build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tablePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMetadataConfig(metadataConfig)
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {

      String firstCommitTime = client.startCommit();
      int numRecords = 10;
      List<HoodieRecord> records = firstDataGen.generateInserts(firstCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, firstCommitTime).collect();
      client.commit(firstCommitTime, jsc().parallelize(result));
      Assertions.assertNoWriteErrors(result);

      // Get a record key from the inserted records
      String firstRecordKey = records.get(0).getRecordKey();
      String firstPartitionPath = records.get(0).getPartitionPath();

      String secondCommitTime = client.startCommit();
      records = secondDataGen.generateInserts(secondCommitTime, numRecords);
      writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      result = client.upsert(writeRecords, secondCommitTime).collect();
      client.commit(secondCommitTime, jsc().parallelize(result));
      Assertions.assertNoWriteErrors(result);

      // Get a record key from the inserted records
      String secondRecordKey = records.get(0).getRecordKey();
      String secondPartitionPath = records.get(0).getPartitionPath();

      // Connect to the table
      new TableCommand().connect(tablePath, false, 0, 0, 0,
          "WAIT_TO_ADJUST_SKEW", 200L, false);

      // Verify record index is enabled in table config
      HoodieTableMetaClient metaClient = createMetaClient(jsc(), tablePath);
      assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX));

      // Validate entries in the Non-Global RLI.
      validateRecordIndexOutput(firstRecordKey, Option.of(firstPartitionPath), firstCommitTime, DEFAULT_FIRST_PARTITION_PATH);
      validateRecordIndexOutput(secondRecordKey, Option.of(secondPartitionPath), secondCommitTime, DEFAULT_SECOND_PARTITION_PATH);

      // Test non-existent record key with partition path
      validateNonExistentRecordKey("non_existent_key", Option.of(firstPartitionPath));
    }
  }

  @Test
  public void testMetadataStatsAndFileListing() throws Exception {
    writeOneCommit(true);
    connectToTable();

    // The command opens the reader with metadata metrics off, so there is nothing to report on,
    // but the stat table is still rendered. Tracked in https://github.com/apache/hudi/issues/19880;
    // once fixed, assert on the rows instead.
    Object stats = shell.evaluate(() -> "metadata stats");
    assertTrue(ShellEvaluationResultUtil.isSuccess(stats));
    assertTrue(stats.toString().contains("stat key"), stats.toString());
    assertTrue(renderedRows(stats.toString()).isEmpty(), stats.toString());

    Object partitions = shell.evaluate(() -> "metadata list-partitions");
    assertTrue(ShellEvaluationResultUtil.isSuccess(partitions));
    Set<String> written = writtenPartitions();
    assertFalse(written.isEmpty());
    assertEquals(written, renderedRows(partitions.toString()).stream()
        .map(row -> row.get(0)).collect(Collectors.toSet()));

    // The files of one partition, as the metadata table has them.
    Object files = shell.evaluate(() -> "metadata list-files --partition " + DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(ShellEvaluationResultUtil.isSuccess(files));
    Set<String> baseFiles = baseFilesOf(DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(baseFiles.isEmpty());
    assertEquals(baseFiles.size(), renderedRows(files.toString()).size(), files.toString());
    for (String baseFile : baseFiles) {
      assertTrue(files.toString().contains(baseFile), files.toString());
    }

    // Without --partition the lookup key is the non-partitioned name ("."), which the files index
    // of a partitioned table has no record for, so the lookup misses and nothing is listed.
    Object rootFiles = shell.evaluate(() -> "metadata list-files");
    assertTrue(ShellEvaluationResultUtil.isSuccess(rootFiles));
    assertTrue(renderedRows(rootFiles.toString()).isEmpty(), rootFiles.toString());
  }

  @Test
  public void testMetadataValidateFiles() throws Exception {
    writeOneCommit(true);
    connectToTable();

    // Every file on disk is in the metadata table, so nothing is reported without --verbose.
    Object matching = shell.evaluate(() -> "metadata validate-files");
    assertTrue(ShellEvaluationResultUtil.isSuccess(matching));
    assertTrue(renderedRows(matching.toString()).isEmpty(), matching.toString());

    // --verbose reports every file, present on both sides and of the same size.
    Object verbose = shell.evaluate(() -> "metadata validate-files --verbose true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(verbose));
    List<List<String>> rows = renderedRows(verbose.toString());
    int filesOnDisk = writtenPartitions().stream().mapToInt(partition -> baseFilesOf(partition).size()).sum();
    assertEquals(filesOnDisk, rows.size(), verbose.toString());
    for (List<String> row : rows) {
      assertEquals("true", row.get(2), row.toString());
      assertEquals("true", row.get(3), row.toString());
      assertEquals(row.get(4), row.get(5), row.toString());
    }

    // A base file the metadata table does not know about is reported even without --verbose.
    String strayFile = writeStrayBaseFile(DEFAULT_FIRST_PARTITION_PATH);
    Object mismatching = shell.evaluate(() -> "metadata validate-files");
    assertTrue(ShellEvaluationResultUtil.isSuccess(mismatching));
    List<List<String>> mismatchingRows = renderedRows(mismatching.toString());
    assertEquals(1, mismatchingRows.size(), mismatching.toString());
    assertEquals(DEFAULT_FIRST_PARTITION_PATH, mismatchingRows.get(0).get(0));
    assertEquals(strayFile, mismatchingRows.get(0).get(1));
    assertEquals("true", mismatchingRows.get(0).get(2));
    assertEquals("false", mismatchingRows.get(0).get(3));
  }

  @Test
  public void testMetadataCreateAndInit() throws Exception {
    // Written with the metadata table off, so that it does not exist yet.
    writeOneCommit(false);
    connectToTable();
    StoragePath metadataPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(tablePath));
    assertFalse(HoodieCLI.storage.exists(metadataPath));

    // There is nothing to update yet.
    Object tooEarly = shell.evaluate(() -> "metadata init");
    assertFalse(ShellEvaluationResultUtil.isSuccess(tooEarly));
    assertTrue(tooEarly.toString().contains("does not exist"), tooEarly.toString());

    Object created = shell.evaluate(() -> "metadata create");
    assertTrue(ShellEvaluationResultUtil.isSuccess(created));
    assertTrue(created.toString().startsWith("Created Metadata Table in " + metadataPath), created.toString());
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(HoodieCLI.conf.newInstance()).setBasePath(metadataPath.toString()).build();
    int instantsAfterCreate = metadataMetaClient.getActiveTimeline().countInstants();
    assertTrue(instantsAfterCreate > 0);

    // The second create finds the directory of the first one.
    Object again = shell.evaluate(() -> "metadata create");
    assertFalse(ShellEvaluationResultUtil.isSuccess(again));
    assertTrue(again.toString().contains("not empty"), again.toString());

    // Read only init opens the table without writing to it.
    Object opened = shell.evaluate(() -> "metadata init --readonly true");
    assertTrue(ShellEvaluationResultUtil.isSuccess(opened));
    assertTrue(opened.toString().startsWith("Opened Metadata Table in " + metadataPath), opened.toString());
    assertEquals(instantsAfterCreate,
        HoodieTableMetaClient.reload(metadataMetaClient).getActiveTimeline().countInstants());

    Object initialized = shell.evaluate(() -> "metadata init");
    assertTrue(ShellEvaluationResultUtil.isSuccess(initialized));
    assertTrue(initialized.toString().startsWith("Initialized Metadata Table in " + metadataPath),
        initialized.toString());
  }

  @Test
  public void testMetadataSetDirectory() throws Exception {
    String customDirectory = tablePath + "-metadata";
    try {
      // An empty directory leaves the default location of the metadata table in place.
      Object unset = shell.evaluate(() -> "metadata set");
      assertTrue(ShellEvaluationResultUtil.isSuccess(unset));
      assertEquals("Ok", unset.toString());
      assertEquals(HoodieTableMetadata.getMetadataTableBasePath(tablePath),
          MetadataCommand.getMetadataTableBasePath(tablePath));

      Object set = shell.evaluate(() -> "metadata set --metadataDir " + customDirectory);
      assertTrue(ShellEvaluationResultUtil.isSuccess(set));
      assertEquals("Ok", set.toString());
      assertEquals(customDirectory, MetadataCommand.getMetadataTableBasePath(tablePath));

      // The directory is global to the session and can only be set once.
      Object reset = shell.evaluate(() -> "metadata set --metadataDir " + customDirectory + "-other");
      assertFalse(ShellEvaluationResultUtil.isSuccess(reset));
      assertEquals(customDirectory, MetadataCommand.getMetadataTableBasePath(tablePath));
    } finally {
      clearMetadataBaseDirectory();
    }
  }

  /**
   * Writes one commit of ten records over two partitions into a new table at {@link #tablePath}.
   *
   * @param enableMetadataTable Whether the write maintains the metadata table.
   */
  private void writeOneCommit(boolean enableMetadataTable) throws Exception {
    writeOneCommit(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build());
  }

  /**
   * Writes one commit of ten records over two partitions into a new table at {@link #tablePath}.
   *
   * @param metadataConfig Metadata table configuration of the write.
   */
  private void writeOneCommit(HoodieMetadataConfig metadataConfig) throws Exception {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName(tableName)
        .setArchiveLogFolder(HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue())
        .setPayloadClassName("org.apache.hudi.common.model.HoodieAvroPayload")
        .setPartitionFields("partition_path")
        .setRecordKeyFields("_row_key")
        .setKeyGeneratorClassProp(SimpleKeyGenerator.class.getCanonicalName())
        .initTable(HoodieCLI.conf.newInstance(), tablePath);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tablePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMetadataConfig(metadataConfig)
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      String commitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 10);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, commitTime).collect();
      client.commit(commitTime, jsc().parallelize(result));
      Assertions.assertNoWriteErrors(result);
    }
  }

  private void connectToTable() throws IOException {
    new TableCommand().connect(tablePath, false, 0, 0, 0, "WAIT_TO_ADJUST_SKEW", 200L, false);
  }

  private Set<String> writtenPartitions() {
    return Stream.of(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH)
        .filter(partition -> Files.isDirectory(Paths.get(tablePath, partition)))
        .collect(Collectors.toSet());
  }

  private Set<String> baseFilesOf(String partition) {
    try (Stream<Path> files = Files.list(Paths.get(tablePath, partition))) {
      return files.map(file -> file.getFileName().toString())
          .filter(name -> name.endsWith(BASE_FILE_EXTENSION))
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds a base file the metadata table has never heard of to a partition of the table.
   *
   * @param partition Partition to add the file to.
   * @return The name of the file created.
   */
  private String writeStrayBaseFile(String partition) throws IOException {
    String existing = baseFilesOf(partition).iterator().next();
    // <fileId>_<writeToken>_<instantTime>.parquet, only the file id has to be new
    String strayFile = UUID.randomUUID() + existing.substring(existing.indexOf('_'));
    Files.createFile(Paths.get(tablePath, partition, strayFile));
    return strayFile;
  }

  private static void clearMetadataBaseDirectory() throws Exception {
    Field field = MetadataCommand.class.getDeclaredField("metadataBaseDirectory");
    field.setAccessible(true);
    field.set(null, null);
  }

  private void validateRecordIndexOutput(String recordKey, Option<String> partitionPathOp,
                                         String expectedInstantTime, String expectedPartitionPath) {
    // Execute the metadata lookup-record-index command
    Object shellResult;
    if (partitionPathOp.isPresent()) {
      shellResult = shell.evaluate(() -> "metadata lookup-record-index --record_key " + recordKey
          + " --partition_path " + partitionPathOp.get());
    } else {
      shellResult = shell.evaluate(() -> "metadata lookup-record-index --record_key " + recordKey);
    }

    // The result should either succeed or return an info message about the key not being found
    // We just verify the command doesn't crash with an unexpected error
    String output = shellResult.toString();
    assertTrue(output.contains(recordKey)
            && output.contains(expectedInstantTime)
            && output.contains(expectedPartitionPath)
            && output.contains("Record key")
            && output.contains("Partition path")
            && output.contains("File Id")
            && output.contains("Instant time"),
        "Command output should contain either the record key, an info message, or mention Record key. Got: " + output);
  }

  private void validateNonExistentRecordKey(String recordKey, Option<String> partitionPathOp) {
    Object shellResult;
    if (partitionPathOp.isPresent()) {
      shellResult = shell.evaluate(() -> "metadata lookup-record-index --record_key " + recordKey
          + " --partition_path " + partitionPathOp.get());
    } else {
      shellResult = shell.evaluate(() -> "metadata lookup-record-index --record_key " + recordKey);
    }

    String output = shellResult.toString();
    assertTrue(output.contains("[INFO] Record key " + recordKey) && output.contains("not found in Record Index"),
        "Command output should indicate record key not found. Got: " + output);
    if (partitionPathOp.isPresent()) {
      assertTrue(output.contains("in partition " + partitionPathOp.get()),
          "Command output should mention the partition path. Got: " + output);
    }
  }
}
