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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
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
}
