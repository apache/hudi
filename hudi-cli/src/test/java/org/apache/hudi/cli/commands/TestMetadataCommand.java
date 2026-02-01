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
import org.apache.hudi.testutils.Assertions;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient;
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
    }
  }

  @Test
  public void testGetRecordIndexInfoForNonGlobalRLI() throws Exception {
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
    }
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
}
