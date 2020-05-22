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
import org.apache.hudi.cli.AbstractShellIntegrationTest;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.RepairsCommand;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.spark.sql.Dataset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link RepairsCommand#deduplicate}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
public class ITTestRepairsCommand extends AbstractShellIntegrationTest {

  private String duplicatedPartitionPath;
  private String duplicatedPartitionPathWithUpdates;
  private String duplicatedPartitionPathWithUpserts;
  private String repairedOutputPath;

  @BeforeEach
  public void init() throws IOException, URISyntaxException {
    String tablePath = basePath + File.separator + "test_table";
    duplicatedPartitionPath = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    duplicatedPartitionPathWithUpdates = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
    duplicatedPartitionPathWithUpserts = tablePath + File.separator + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    repairedOutputPath = basePath + File.separator + "tmp";

    HoodieCLI.conf = jsc.hadoopConfiguration();

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    // generate 200 records
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    String fileName1 = "1_0_20160401010101.parquet";
    String fileName2 = "2_0_20160401010101.parquet";
    String secondPartitionFileName1 = "1_1_20160401010101.parquet";
    String secondPartitionFileName2 = "2_1_20160401010101.parquet";
    String thirdPartitionFileName1 = "1_2_20160401010101.parquet";
    String thirdPartitionFileName2 = "2_2_20160401010202.parquet";
    String thirdPartitionFileName3 = "3_2_20160401010202.parquet";

    List<HoodieRecord> hoodieRecords1 = SchemaTestUtil.generateHoodieTestRecords(0, 100, schema);
    HoodieClientTestUtils.writeParquetFile(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        fileName1, hoodieRecords1, schema, null, false);
    HoodieClientTestUtils.writeParquetFile(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        secondPartitionFileName1, hoodieRecords1, schema, null, false);
    HoodieClientTestUtils.writeParquetFile(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH,
        thirdPartitionFileName1, hoodieRecords1, schema, null, false);
    List<HoodieRecord> hoodieRecords2 = SchemaTestUtil.generateHoodieTestRecords(100, 100, schema);
    HoodieClientTestUtils.writeParquetFile(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        fileName2, hoodieRecords2, schema, null, false);

    // generate commit file
    String fileId1 = UUID.randomUUID().toString();
    String testWriteToken = "1-0-1";
    String commitTime = FSUtils.getCommitTime(fileName1);
    Files.createFile(Paths.get(duplicatedPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime, 1, testWriteToken)));
    Files.createFile(Paths.get(tablePath + "/.hoodie/" + commitTime + ".commit"));

    // read records and get 10 to generate duplicates
    Dataset df = sqlContext.read().parquet(duplicatedPartitionPath);
    Dataset secondPartitionDf = sqlContext.read().parquet(duplicatedPartitionPathWithUpdates);
    Dataset thirdPartitionDf = sqlContext.read().parquet(duplicatedPartitionPathWithUpserts);
    secondPartitionDf.limit(10).withColumn("_hoodie_commit_time", lit("20160401010101"))
      .write().parquet(duplicatedPartitionPathWithUpdates + File.separator + secondPartitionFileName2);
    thirdPartitionDf.limit(10).withColumn("_hoodie_commit_time", lit("20160401010202")).withColumn("_hoodie_file_name", lit(thirdPartitionFileName2))
      .write().parquet(duplicatedPartitionPathWithUpserts + File.separator + thirdPartitionFileName2);
    thirdPartitionDf.limit(10).withColumn("_hoodie_commit_time", lit("20160401010202")).withColumn("_hoodie_file_name", lit(thirdPartitionFileName3))
      .write().parquet(duplicatedPartitionPathWithUpserts + File.separator + thirdPartitionFileName3);

    String fileName3 = "3_0_20160401010202.parquet";
    commitTime = FSUtils.getCommitTime(fileName3);
    df.limit(10).withColumn("_hoodie_commit_time", lit(commitTime))
        .write().parquet(duplicatedPartitionPath + File.separator + fileName3);
    Files.createFile(Paths.get(tablePath + "/.hoodie/" + commitTime + ".commit"));

    metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
  }

  /**
   * Test case for dry run deduplicate.
   */
  @Test
  public void testDeduplicate() throws IOException {
    // get fs and check number of latest files
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
        fs.listStatus(new Path(duplicatedPartitionPath)));
    List<String> filteredStatuses = fsView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
    assertEquals(3, filteredStatuses.size(), "There should be 3 files.");

    // Before deduplicate, all files contain 210 records
    String[] files = filteredStatuses.toArray(new String[0]);
    Dataset df = sqlContext.read().parquet(files);
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
    Dataset result = sqlContext.read().parquet(files);
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
    Dataset df = sqlContext.read().parquet(files);
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
    Dataset result = sqlContext.read().parquet(files);
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
    Dataset df = sqlContext.read().parquet(files);
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
    Dataset result = sqlContext.read().parquet(files);
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
    Dataset df = sqlContext.read().parquet(files);
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
    Dataset result = sqlContext.read().parquet(files);
    assertEquals(200, result.count());
  }
}
