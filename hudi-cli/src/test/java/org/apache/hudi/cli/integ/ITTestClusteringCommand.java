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

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.ClusteringCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestClusteringCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;

  @BeforeEach
  public void init() throws IOException {
    tableName = "test_table_" + ITTestClusteringCommand.class.getName();
    basePath = Paths.get(basePath, tableName).toString();

    HoodieCLI.conf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    // Create table and connect
    new TableCommand().createTable(
        basePath, tableName, HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");

    initMetaClient();
  }

  /**
   * Test case for command 'clustering schedule'.
   */
  @Test
  public void testScheduleClustering() throws IOException {
    // generate commits
    generateCommits();

    Object result = scheduleClustering();
    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(
            result.toString().startsWith("Succeeded to schedule clustering for")));

    // there is 1 requested clustering
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    assertEquals(1, timeline.filterPendingReplaceTimeline().countInstants());
  }

  /**
   * Test case for command 'clustering run'.
   */
  @Test
  public void testClustering() throws IOException {
    // generate commits
    generateCommits();

    Object result1 = scheduleClustering();
    assertTrue(ShellEvaluationResultUtil.isSuccess(result1));

    // get clustering instance
    HoodieActiveTimeline timeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Option<String> instanceOpt =
        timeline.filterPendingReplaceTimeline().firstInstant().map(HoodieInstant::getTimestamp);
    assertTrue(instanceOpt.isPresent(), "Must have pending clustering.");
    final String instance = instanceOpt.get();

    Object result2 = shell.evaluate(() ->
            String.format("clustering run --parallelism %s --clusteringInstant %s --sparkMaster %s",
            2, instance, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result2)),
        () -> assertTrue(
            result2.toString().startsWith("Succeeded to run clustering for ")));

    // assert clustering complete
    assertTrue(HoodieCLI.getTableMetaClient().getActiveTimeline().reload()
        .filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList()).contains(instance),
        "Pending clustering must be completed");

    assertTrue(HoodieCLI.getTableMetaClient().getActiveTimeline().reload()
            .getCompletedReplaceTimeline().getInstantsAsStream()
            .map(HoodieInstant::getTimestamp).collect(Collectors.toList()).contains(instance),
        "Pending clustering must be completed");
  }

  /**
   * Test case for command 'clustering scheduleAndExecute'.
   */
  @Test
  public void testClusteringScheduleAndExecute() throws IOException {
    // generate commits
    generateCommits();

    Object result = shell.evaluate(() ->
            String.format("clustering scheduleAndExecute --parallelism %s --sparkMaster %s", 2, "local"));

    assertAll("Command run failed",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertTrue(
            result.toString().startsWith("Succeeded to run clustering for scheduleAndExecute")));

    // assert clustering complete
    assertTrue(HoodieCLI.getTableMetaClient().getActiveTimeline().reload()
            .getCompletedReplaceTimeline().getInstantsAsStream()
            .map(HoodieInstant::getTimestamp).count() > 0,
        "Completed clustering couldn't be 0");
  }

  private Object scheduleClustering() {
    // generate requested clustering
    return shell.evaluate(() ->
            String.format("clustering schedule --hoodieConfigs hoodie.clustering.inline.max.commits=1 --sparkMaster %s", "local"));
  }

  private void generateCommits() throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // Create the write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2).forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();

    try (SparkRDDWriteClient<HoodieAvroPayload> client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg)) {
      insert(jsc, client, dataGen, "001");
      insert(jsc, client, dataGen, "002");
    }
  }

  private List<HoodieRecord> insert(JavaSparkContext jsc, SparkRDDWriteClient<HoodieAvroPayload> client,
      HoodieTestDataGenerator dataGen, String newCommitTime) throws IOException {
    // inserts
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    operateFunc(SparkRDDWriteClient::insert, client, writeRecords, newCommitTime);
    return records;
  }

  private JavaRDD<WriteStatus> operateFunc(
      HoodieClientTestBase.Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      SparkRDDWriteClient<HoodieAvroPayload> client, JavaRDD<HoodieRecord> writeRecords, String commitTime)
      throws IOException {
    return writeFn.apply(client, writeRecords, commitTime);
  }
}
