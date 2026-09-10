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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for the clustering entry point of {@link SparkMain}, which the clustering commands
 * reach through a spark-submit of their own.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestClusteringCommand extends CLIFunctionalTestHarness {

  private String tableName;
  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    HoodieCLI.conf = storageConf();
    tableName = tableName();
    tablePath = tablePath(tableName);

    new TableCommand().createTable(
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", HoodieTableVersion.current().versionCode(), HoodieAvroPayload.class.getName());
  }

  @Test
  public void testSparkMainClusterScheduleAndExecute() throws Exception {
    writeCommits();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    assertEquals(0, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    int returnCode = SparkMain.cluster(jsc(), tablePath, tableName, null, 1, "1g", 0,
        UtilHelpers.SCHEDULE_AND_EXECUTE, null, Collections.emptyList());

    assertEquals(0, returnCode);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // the plan it scheduled ran to completion, leaving a replace commit and nothing pending
    assertEquals(0, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());
    assertEquals(1, metaClient.getActiveTimeline().getCompletedReplaceTimeline().countInstants());
  }

  /**
   * Writes two commits of small files into the table, which is what the clustering plan strategy
   * looks for.
   */
  private void writeCommits() {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH});
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(1, 1).forTable(tableName).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      String firstCommit = client.startCommit();
      writeAndCommit(client, dataGen.generateInserts(firstCommit, 10), firstCommit);
      String secondCommit = client.startCommit();
      writeAndCommit(client, dataGen.generateInserts(secondCommit, 10), secondCommit);
    }
  }

  private void writeAndCommit(SparkRDDWriteClient client, List<HoodieRecord> records, String commitTime) {
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> result = client.insert(writeRecords, commitTime).collect();
    client.commit(commitTime, jsc().parallelize(result));
    Assertions.assertNoWriteErrors(result);
  }
}
