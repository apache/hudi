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
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.Assertions;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.DROP_PARTITION_COLUMNS;
import static org.apache.hudi.common.table.HoodieTableConfig.NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_CHECKSUM;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_LAYOUT_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.generateChecksum;
import static org.apache.hudi.common.table.HoodieTableConfig.validateChecksum;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link RepairsCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestRepairsCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private String tablePath;
  private FileSystem fs;

  @BeforeEach
  public void init() throws IOException {
    String tableName = tableName();
    tablePath = tablePath(tableName);
    fs = HadoopFSUtils.getFs(tablePath, storageConf());

    // Create table and connect
    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        HoodieTableConfig.TIMELINE_HISTORY_PATH.defaultValue(), TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  @AfterEach
  public void cleanUp() throws IOException {
    fs.close();
  }

  /**
   * Test case for dry run 'repair addpartitionmeta'.
   */
  @Test
  public void testAddPartitionMetaWithDryRun() throws IOException {
    // create commit instant
    Files.createFile(Paths.get(tablePath, ".hoodie/timeline/", "100.commit"));

    // create partition path
    String partition1 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).toString();
    String partition2 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).toString();
    String partition3 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH).toString();
    assertTrue(fs.mkdirs(new Path(partition1)));
    assertTrue(fs.mkdirs(new Path(partition2)));
    assertTrue(fs.mkdirs(new Path(partition3)));

    // default is dry run.
    Object result = shell.evaluate(() -> "repair addpartitionmeta");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // expected all 'No'.
    String[][] rows = FSUtils.getAllPartitionFoldersThreeLevelsDown(new HoodieHadoopStorage(fs), tablePath)
        .stream()
        .map(partition -> new String[] {partition, "No", "None"})
        .toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for real run 'repair addpartitionmeta'.
   */
  @Test
  public void testAddPartitionMetaWithRealRun() throws IOException {
    // create commit instant
    Files.createFile(Paths.get(tablePath, ".hoodie", "100.commit"));

    // create partition path
    String partition1 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).toString();
    String partition2 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).toString();
    String partition3 = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH).toString();
    assertTrue(fs.mkdirs(new Path(partition1)));
    assertTrue(fs.mkdirs(new Path(partition2)));
    assertTrue(fs.mkdirs(new Path(partition3)));

    Object result = shell.evaluate(() -> "repair addpartitionmeta --dryrun false");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    List<String> paths = FSUtils.getAllPartitionFoldersThreeLevelsDown(new HoodieHadoopStorage(fs), tablePath);
    // after dry run, the action will be 'Repaired'
    String[][] rows = paths.stream()
        .map(partition -> new String[] {partition, "No", "Repaired"})
        .toArray(String[][]::new);
    String expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);

    result = shell.evaluate(() -> "repair addpartitionmeta");

    // after real run, Metadata is present now.
    rows = paths.stream()
        .map(partition -> new String[] {partition, "Yes", "None"})
        .toArray(String[][]::new);
    expected = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_PARTITION_PATH,
        HoodieTableHeaderFields.HEADER_METADATA_PRESENT, HoodieTableHeaderFields.HEADER_ACTION}, rows);
    expected = removeNonWordAndStripSpace(expected);
    got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for 'repair overwrite-hoodie-props'.
   */
  @Test
  public void testOverwriteHoodieProperties() throws IOException {
    URL newProps = this.getClass().getClassLoader().getResource("table-config.properties");
    assertNotNull(newProps, "New property file must exist");

    Object cmdResult = shell.evaluate(() -> "repair overwrite-hoodie-props --new-props-file " + newProps.getPath());
    assertTrue(ShellEvaluationResultUtil.isSuccess(cmdResult));

    Map<String, String> oldProps = HoodieCLI.getTableMetaClient().getTableConfig().propsMap();

    // after overwrite, the stored value in .hoodie is equals to which read from properties.
    HoodieTableConfig tableConfig = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient()).getTableConfig();
    Map<String, String> result = tableConfig.propsMap();
    // validate table checksum
    assertTrue(result.containsKey(TABLE_CHECKSUM.key()));
    assertTrue(validateChecksum(tableConfig.getProps()));
    Properties expectProps = new Properties();
    expectProps.load(new FileInputStream(newProps.getPath()));

    Map<String, String> expected = expectProps.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
    expected.putIfAbsent(TABLE_CHECKSUM.key(), String.valueOf(generateChecksum(tableConfig.getProps())));
    expected.putIfAbsent(DROP_PARTITION_COLUMNS.key(), String.valueOf(DROP_PARTITION_COLUMNS.defaultValue()));
    assertEquals(expected, result);

    // check result
    List<String> allPropsStr = Arrays.asList(NAME.key(), TYPE.key(), VERSION.key(),
        TIMELINE_HISTORY_PATH.key(), TIMELINE_LAYOUT_VERSION.key(), TABLE_CHECKSUM.key(), DROP_PARTITION_COLUMNS.key());
    String[][] rows = allPropsStr.stream().sorted().map(key -> new String[] {key,
            oldProps.getOrDefault(key, "null"), result.getOrDefault(key, "null")})
        .toArray(String[][]::new);
    String expect = HoodiePrintHelper.print(new String[] {HoodieTableHeaderFields.HEADER_HOODIE_PROPERTY,
        HoodieTableHeaderFields.HEADER_OLD_VALUE, HoodieTableHeaderFields.HEADER_NEW_VALUE}, rows);
    expect = removeNonWordAndStripSpace(expect);
    String got = removeNonWordAndStripSpace(cmdResult.toString());
    assertEquals(expect, got);
  }

  /**
   * Test case for 'repair corrupted clean files'.
   */
  @Test
  public void testRemoveCorruptedPendingCleanAction() throws IOException {
    HoodieCLI.conf = storageConf();

    StorageConfiguration<?> conf = HoodieCLI.conf;

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    // Create four requested files
    for (int i = 100; i < 104; i++) {
      String timestamp = String.valueOf(i);
      // Write corrupted requested Clean File
      HoodieTestCommitMetadataGenerator.createEmptyCleanRequestedFile(tablePath, timestamp, conf);
    }

    // reload meta client
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // first, there are four instants
    assertEquals(4, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants());

    Object result = shell.evaluate(() -> "repair corrupted clean files");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // reload meta client
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants());
  }

  /**
   * Testcase for "repair cleanup empty commit metadata"
   *
   */
  @Test
  public void testShowFailedCommits() {
    HoodieCLI.conf = storageConf();

    StorageConfiguration<?> conf = HoodieCLI.conf;

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    for (int i = 1; i < 20; i++) {
      String timestamp = String.valueOf(i);
      // Write corrupted requested Clean File
      HoodieTestCommitMetadataGenerator.createCommitFile(tablePath, timestamp, conf);
    }

    metaClient.getActiveTimeline().getInstantsAsStream().filter(hoodieInstant -> Integer.parseInt(hoodieInstant.requestedTime()) % 4 == 0).forEach(hoodieInstant -> {
      metaClient.getActiveTimeline().deleteInstantFileIfExists(hoodieInstant);
      if (hoodieInstant.isCompleted()) {
        metaClient.getActiveTimeline().createCompleteInstant(hoodieInstant);
      } else {
        metaClient.getActiveTimeline().createNewInstant(hoodieInstant);
      }
    });

    final TestLogAppender appender = new TestLogAppender();
    final Logger logger = (Logger) LogManager.getLogger(RepairsCommand.class);
    try {
      appender.start();
      logger.addAppender(appender);
      Object result = shell.evaluate(() -> "repair show empty commit metadata");
      assertTrue(ShellEvaluationResultUtil.isSuccess(result));
      final List<LogEvent> log = appender.getLog();
      assertEquals(log.size(),4);
      log.forEach(LoggingEvent -> {
        assertEquals(LoggingEvent.getLevel(), Level.WARN);
        assertTrue(LoggingEvent.getMessage().getFormattedMessage().contains("Empty Commit: "));
        assertTrue(LoggingEvent.getMessage().getFormattedMessage().contains("COMPLETED]"));
      });
    } finally {
      logger.removeAppender(appender);
    }


  }

  @Test
  public void testRepairDeprecatedPartition() throws IOException {
    tablePath = tablePath + "/repair_test/";
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

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config, true)) {
      String newCommitTime = "001";
      int numRecords = 10;
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, newCommitTime).collect();
      Assertions.assertNoWriteErrors(result);

      newCommitTime = "002";
      // Generate HoodieRecords w/ null values for partition path field.
      List<HoodieRecord> records1 = dataGen.generateInserts(newCommitTime, numRecords);
      List<HoodieRecord> records2 = new ArrayList<>();
      records1.forEach(entry -> {
        HoodieKey hoodieKey = new HoodieKey(entry.getRecordKey(), PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH);
        GenericRecord genericRecord = (GenericRecord) entry.getData();
        genericRecord.put("partition_path", null);
        records2.add(new HoodieAvroIndexedRecord(hoodieKey, genericRecord));
      });

      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      // ingest records2 which has null for partition path fields, but goes into "default" partition.
      JavaRDD<HoodieRecord> writeRecords2 = context().getJavaSparkContext().parallelize(records2, 1);
      List<WriteStatus> result2 = client.bulkInsert(writeRecords2, newCommitTime).collect();
      Assertions.assertNoWriteErrors(result2);

      SQLContext sqlContext = context().getSqlContext();
      long totalRecs = sqlContext.read().format("hudi").load(tablePath).count();
      assertEquals(totalRecs, 20);

      // Execute repair deprecated partition command
      assertEquals(0, SparkMain.repairDeprecatedPartition(jsc(), tablePath));

      // there should not be any records w/ default partition
      totalRecs = sqlContext.read().format("hudi").load(tablePath)
      .filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " == '" + PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH + "'").count();
      assertEquals(totalRecs, 0);

      // all records from default partition should have been migrated to __HIVE_DEFAULT_PARTITION__
      totalRecs = sqlContext.read().format("hudi").load(tablePath)
          .filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " == '" + PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH + "'").count();
      assertEquals(totalRecs, 10);
    }
  }

  @Test
  public void testRenamePartition() throws IOException {
    tablePath = tablePath + "/rename_partition_test/";
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

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config, true)) {
      String newCommitTime = "001";
      int numRecords = 20;
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context().getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, newCommitTime).collect();
      Assertions.assertNoWriteErrors(result);

      SQLContext sqlContext = context().getSqlContext();
      long totalRecs = sqlContext.read().format("hudi").load(tablePath).count();
      assertEquals(totalRecs, 20);
      long totalRecsInOldPartition = sqlContext.read().format("hudi").load(tablePath)
          .filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " == '" + DEFAULT_FIRST_PARTITION_PATH + "'").count();

      // Execute rename partition command
      assertEquals(0, SparkMain.renamePartition(jsc(), tablePath, DEFAULT_FIRST_PARTITION_PATH, "2016/03/18"));

      // there should not be any records in old partition
      totalRecs = sqlContext.read().format("hudi").load(tablePath)
          .filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " == '" + DEFAULT_FIRST_PARTITION_PATH + "'").count();
      assertEquals(totalRecs, 0);

      // all records from old partition should have been migrated to new partition
      totalRecs = sqlContext.read().format("hudi").load(tablePath)
          .filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " == \"" + "2016/03/18" + "\"").count();
      assertEquals(totalRecs, totalRecsInOldPartition);
    }
  }

  class TestLogAppender extends AbstractAppender {
    private final List<LogEvent> log = new ArrayList<>();

    protected TestLogAppender() {
      super(UUID.randomUUID().toString(), null, null, false, null);
    }

    @Override
    public void append(LogEvent event) {
      log.add(event);
    }

    public List<LogEvent> getLog() {
      return new ArrayList<LogEvent>(log);
    }
  }
}


