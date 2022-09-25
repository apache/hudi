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

package org.apache.hudi.cli.integ;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.RepairsCommand;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link RepairsCommand#deduplicate}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestRepairsCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;

  private String duplicatedPartitionPath;
  private String duplicatedPartitionPathWithUpdates;
  private String duplicatedPartitionPathWithUpserts;
  private String duplicatedNoPartitionPath;
  private String repairedOutputPath;

  private HoodieFileFormat fileFormat;

  @BeforeEach
  public void init() throws Exception {
    duplicatedPartitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    duplicatedPartitionPathWithUpdates = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    duplicatedPartitionPathWithUpserts = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    duplicatedNoPartitionPath = HoodieTestDataGenerator.NO_PARTITION_PATH;
    repairedOutputPath = Paths.get(basePath, "tmp").toString();

    HoodieCLI.conf = jsc.hadoopConfiguration();
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    // generate 200 records
    HoodieRecord[] hoodieRecords1 = SchemaTestUtil.generateHoodieTestRecords(0, 100, schema).toArray(new HoodieRecord[100]);
    HoodieRecord[] hoodieRecords2 = SchemaTestUtil.generateHoodieTestRecords(100, 100, schema).toArray(new HoodieRecord[100]);

    // generate duplicates
    HoodieRecord[] dupRecords = Arrays.copyOf(hoodieRecords1, 10);

    // init cow table
    String cowTablePath = Paths.get(basePath, HoodieTableType.COPY_ON_WRITE.name()).toString();

    // Create cow table and connect
    new TableCommand().createTable(
        cowTablePath, "cow_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieSparkWriteableTestTable cowTable = HoodieSparkWriteableTestTable.of(HoodieCLI.getTableMetaClient(), schema);

    cowTable.addCommit("20160401010101")
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "1", hoodieRecords1)
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "2", hoodieRecords2)
        .getFileIdWithLogFile(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);

    cowTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "4", hoodieRecords1)
            .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "6", hoodieRecords1);

    cowTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "5", dupRecords);
    cowTable.addCommit("20160401010202")
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "3", dupRecords);
    cowTable.withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "7", dupRecords)
            .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "8", dupRecords);

    // init mor table
    String morTablePath = Paths.get(basePath, HoodieTableType.MERGE_ON_READ.name()).toString();
    // Create mor table and connect
    new TableCommand().createTable(
        morTablePath, "mor_table", HoodieTableType.MERGE_ON_READ.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
    HoodieSparkWriteableTestTable morTable = HoodieSparkWriteableTestTable.of(HoodieCLI.getTableMetaClient(), schema);

    morTable.addDeltaCommit("20160401010101");
    morTable.withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "1", hoodieRecords1)
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "2", hoodieRecords2)
        .getFileIdWithLogFile(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);

    morTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "4", hoodieRecords1)
        .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "6", hoodieRecords1)
        .withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "5", dupRecords);
    morTable.addDeltaCommit("20160401010202");
    morTable.withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "3", dupRecords)
        .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "7", dupRecords)
        .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "8", dupRecords);

    // init cow table for non-partitioned table tests
    String cowNonPartitionedTablePath = Paths.get(basePath, "cow_table_non_partitioned").toString();

    // Create cow table and connect
    new TableCommand().createTable(
        cowNonPartitionedTablePath, "cow_table_non_partitioned", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieSparkWriteableTestTable cowNonPartitionedTable = HoodieSparkWriteableTestTable.of(HoodieCLI.getTableMetaClient(), schema);

    cowNonPartitionedTable.addCommit("20160401010101")
        .withInserts(HoodieTestDataGenerator.NO_PARTITION_PATH, "1", hoodieRecords1)
        .getFileIdWithLogFile(HoodieTestDataGenerator.NO_PARTITION_PATH);

    cowNonPartitionedTable.addCommit("20160401010202")
        .withInserts(HoodieTestDataGenerator.NO_PARTITION_PATH, "2", dupRecords);

    fileFormat = metaClient.getTableConfig().getBaseFileFormat();
  }

  /**
   * Test case for dry run deduplicate.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testDeduplicateWithInserts(HoodieTableType tableType) throws IOException {
    String tablePath = Paths.get(basePath, tableType.name()).toString();
    connectTableAndReloadMetaClient(tablePath);
    // get fs and check number of latest files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(Paths.get(tablePath, duplicatedPartitionPath).toString())));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 210 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(210, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s",
        partitionPath, repairedOutputPath, "local");
    Object resultForCmd = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForCmd));
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, resultForCmd.toString());

    // After deduplicate, there are 200 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(200, result.count());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testDeduplicateWithUpdates(HoodieTableType tableType) throws IOException {
    String tablePath = Paths.get(basePath, tableType.name()).toString();
    connectTableAndReloadMetaClient(tablePath);
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(Paths.get(tablePath, duplicatedPartitionPathWithUpdates).toString())));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(2, filteredStatuses.size(), "There should be 2 files.");

    // Before deduplicate, all files contain 110 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(110, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s --dedupeType %s",
        partitionPath, repairedOutputPath, "local", "update_type");
    Object resultForCmd = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForCmd));
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, resultForCmd.toString());

    // After deduplicate, there are 100 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(100, result.count());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testDeduplicateWithUpserts(HoodieTableType tableType) throws IOException {
    String tablePath = Paths.get(basePath, tableType.name()).toString();
    connectTableAndReloadMetaClient(tablePath);
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(Paths.get(tablePath, duplicatedPartitionPathWithUpserts).toString())));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 120 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(120, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s --dedupeType %s",
        partitionPath, repairedOutputPath, "local", "upsert_type");
    Object resultForCmd = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForCmd));
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, resultForCmd.toString());

    // After deduplicate, there are 100 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(100, result.count());
  }

  /**
   * Test case dry run deduplicate for non-partitioned dataset.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testDeduplicateNoPartitionWithInserts(HoodieTableType tableType) throws IOException {
    String tablePath = Paths.get(basePath, "cow_table_non_partitioned").toString();
    connectTableAndReloadMetaClient(tablePath);
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(Paths.get(tablePath, duplicatedNoPartitionPath).toString())));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(2, filteredStatuses.size(), "There should be 2 files.");

    // Before deduplicate, all files contain 110 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(110, df.count());

    // use default value without specifying duplicatedPartitionPath
    String cmdStr = String.format("repair deduplicate --repairedOutputPath %s --sparkMaster %s",
        repairedOutputPath, "local");
    Object resultForCmd = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForCmd));
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, resultForCmd.toString());

    // After deduplicate, there are 100 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(100, result.count());
  }

  /**
   * Test case for real run deduplicate.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testDeduplicateWithReal(HoodieTableType tableType) throws IOException {
    String tablePath = Paths.get(basePath, tableType.name()).toString();
    connectTableAndReloadMetaClient(tablePath);
    // get fs and check number of latest files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(Paths.get(tablePath, duplicatedPartitionPath).toString())));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 210 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(210, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s"
        + " --sparkMaster %s --dryrun %s", partitionPath, repairedOutputPath, "local", false);
    Object resultForCmd = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForCmd));
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + partitionPath, resultForCmd.toString());

    // After deduplicate, there are 200 records under partition path
    FileStatus[] fileStatus = fs.listStatus(new Path(Paths.get(tablePath, duplicatedPartitionPath).toString()));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(200, result.count());
  }

  private void connectTableAndReloadMetaClient(String tablePath) throws IOException {
    new TableCommand().connect(tablePath, TimelineLayoutVersion.VERSION_1, false, 0, 0, 0);
    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
  }

  private Dataset readFiles(String[] files) {
    if (HoodieFileFormat.PARQUET.equals(fileFormat)) {
      return sqlContext.read().parquet(files);
    } else if (HoodieFileFormat.ORC.equals(fileFormat)) {
      return sqlContext.read().orc(files);
    }
    throw new UnsupportedOperationException(fileFormat.name() + " format not supported yet.");
  }
}
