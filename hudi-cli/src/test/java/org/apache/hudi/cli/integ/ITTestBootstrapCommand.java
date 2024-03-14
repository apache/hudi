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
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.functional.TestBootstrap;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class of {@link org.apache.hudi.cli.commands.BootstrapCommand}.
 */
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class ITTestBootstrapCommand extends HoodieCLIIntegrationTestBase {

  @Autowired
  private Shell shell;
  private static final int NUM_OF_RECORDS = 100;
  private static final String PARTITION_FIELD = "datestr";
  private static final String RECORD_KEY_FIELD = "_row_key";

  private String tableName;
  private String sourcePath;
  private String tablePath;
  private List<String> partitions;

  @BeforeEach
  public void init() {
    String srcName = "source";
    tableName = "test-table";
    sourcePath = basePath + StoragePath.SEPARATOR + srcName;
    tablePath = basePath + StoragePath.SEPARATOR + tableName;

    // generate test data
    partitions = Arrays.asList("2018", "2019", "2020");
    long timestamp = Instant.now().toEpochMilli();
    for (int i = 0; i < partitions.size(); i++) {
      Dataset<Row> df = TestBootstrap.generateTestRawTripDataset(timestamp,
          i * NUM_OF_RECORDS, i * NUM_OF_RECORDS + NUM_OF_RECORDS, null, jsc, sqlContext);
      df.write().parquet(sourcePath + StoragePath.SEPARATOR + PARTITION_FIELD + "=" + partitions.get(i));
    }
  }

  /**
   * Test case for command 'bootstrap'.
   */
  @Test
  public void testBootstrapRunCommand() throws IOException {
    // test bootstrap run command
    String cmdStr = String.format(
        "bootstrap run --targetPath %s --tableName %s --tableType %s --srcPath %s --rowKeyField %s --partitionPathField %s --sparkMaster %s",
        tablePath, tableName, HoodieTableType.COPY_ON_WRITE.name(), sourcePath, RECORD_KEY_FIELD, PARTITION_FIELD, "local");
    Object resultForBootstrapRun = shell.evaluate(() -> cmdStr);
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForBootstrapRun));

    // Connect & check Hudi table exist
    new TableCommand().connect(tablePath, TimelineLayoutVersion.VERSION_1, false, 2000, 300000, 7);
    metaClient = HoodieCLI.getTableMetaClient();
    assertEquals(1, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "Should have 1 commit.");

    // test "bootstrap index showpartitions"
    Object resultForIndexedPartitions = shell.evaluate(() -> "bootstrap index showpartitions");
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForIndexedPartitions));

    String[] header = new String[] {"Indexed partitions"};
    String[][] rows = new String[partitions.size()][1];
    for (int i = 0; i < partitions.size(); i++) {
      rows[i][0] = PARTITION_FIELD + "=" + partitions.get(i);
    }
    String expect = HoodiePrintHelper.print(header, rows);
    expect = removeNonWordAndStripSpace(expect);
    String got = removeNonWordAndStripSpace(resultForIndexedPartitions.toString());
    assertEquals(expect, got);

    // test "bootstrap index showMapping"
    Object resultForIndexedMapping = shell.evaluate(() -> "bootstrap index showmapping");
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForIndexedMapping));

    Object resultForIndexedMappingWithPartition = shell.evaluate(() -> String.format(
            "bootstrap index showmapping --partitionPath %s=%s", PARTITION_FIELD, partitions.get(0)));
    assertTrue(ShellEvaluationResultUtil.isSuccess(resultForIndexedMappingWithPartition));
  }
}
