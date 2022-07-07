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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.RepairsCommand;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
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

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

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
public class ITTestRepairsCommand extends HoodieCLIIntegrationTestBase {

  private String duplicatedPartitionPath;
  private String duplicatedPartitionPathWithUpdates;
  private String duplicatedPartitionPathWithUpserts;
  private String repairedOutputPath;
  private HoodieFileFormat fileFormat;

  @BeforeEach
  public void init() throws Exception {
    final String tablePath = Paths.get(basePath, "test_table").toString();
    duplicatedPartitionPath = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).toString();
    duplicatedPartitionPathWithUpdates = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).toString();
    duplicatedPartitionPathWithUpserts = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH).toString();
    repairedOutputPath = Paths.get(basePath, "tmp").toString();

    HoodieCLI.conf = jsc.hadoopConfiguration();

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    // generate 200 records
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(HoodieCLI.getTableMetaClient(), schema);

    HoodieRecord[] hoodieRecords1 = SchemaTestUtil.generateHoodieTestRecords(0, 100, schema).toArray(new HoodieRecord[100]);
    HoodieRecord[] hoodieRecords2 = SchemaTestUtil.generateHoodieTestRecords(100, 100, schema).toArray(new HoodieRecord[100]);
    testTable.addCommit("20160401010101")
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "1", hoodieRecords1)
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "2", hoodieRecords2)
        .getFileIdWithLogFile(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);

    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "4", hoodieRecords1)
            .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "6", hoodieRecords1);

    // read records and get 10 to generate duplicates
    HoodieRecord[] dupRecords = Arrays.copyOf(hoodieRecords1, 10);
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "5", dupRecords);
    testTable.addCommit("20160401010202")
        .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "3", dupRecords);
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "7", dupRecords)
            .withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "8", dupRecords);

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    fileFormat = metaClient.getTableConfig().getBaseFileFormat();
  }

  /**
   * Test case for dry run deduplicate.
   */
  @Test
  public void testDeduplicateWithInserts() throws IOException {
    // get fs and check number of latest files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(duplicatedPartitionPath)));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 210 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(210, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s",
        partitionPath, repairedOutputPath, "local");
    CommandResult cr = getShell().executeCommand(cmdStr);
    assertTrue(cr.isSuccess());
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, cr.getResult().toString());

    // After deduplicate, there are 200 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(200, result.count());
  }

  @Test
  public void testDeduplicateWithUpdates() throws IOException {
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(duplicatedPartitionPathWithUpdates)));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(2, filteredStatuses.size(), "There should be 2 files.");

    // Before deduplicate, all files contain 110 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(110, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s --dedupeType %s",
        partitionPath, repairedOutputPath, "local", "update_type");
    CommandResult cr = getShell().executeCommand(cmdStr);
    assertTrue(cr.isSuccess());
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, cr.getResult().toString());

    // After deduplicate, there are 100 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(100, result.count());
  }

  @Test
  public void testDeduplicateWithUpserts() throws IOException {
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(duplicatedPartitionPathWithUpserts)));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 120 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(120, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s --sparkMaster %s --dedupeType %s",
        partitionPath, repairedOutputPath, "local", "upsert_type");
    CommandResult cr = getShell().executeCommand(cmdStr);
    assertTrue(cr.isSuccess());
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + repairedOutputPath, cr.getResult().toString());

    // After deduplicate, there are 100 records
    FileStatus[] fileStatus = fs.listStatus(new Path(repairedOutputPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(100, result.count());
  }

  /**
   * Test case for real run deduplicate.
   */
  @Test
  public void testDeduplicateWithReal() throws IOException {
    // get fs and check number of latest files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(duplicatedPartitionPath)));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 210 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = readFiles(files);
    assertEquals(210, df.count());

    String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String cmdStr = String.format("repair deduplicate --duplicatedPartitionPath %s --repairedOutputPath %s"
        + " --sparkMaster %s --dryrun %s", partitionPath, repairedOutputPath, "local", false);
    CommandResult cr = getShell().executeCommand(cmdStr);
    assertTrue(cr.isSuccess());
    assertEquals(RepairsCommand.DEDUPLICATE_RETURN_PREFIX + partitionPath, cr.getResult().toString());

    // After deduplicate, there are 200 records under partition path
    FileStatus[] fileStatus = fs.listStatus(new Path(duplicatedPartitionPath));
    files = Arrays.stream(fileStatus).map(status -> status.getPath().toString()).toArray(String[]::new);
    Dataset result = readFiles(files);
    assertEquals(200, result.count());
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
