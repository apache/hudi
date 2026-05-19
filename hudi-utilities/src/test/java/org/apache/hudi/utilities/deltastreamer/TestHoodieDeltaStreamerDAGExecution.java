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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("HUDI-6505")
public class TestHoodieDeltaStreamerDAGExecution extends HoodieDeltaStreamerTestBase {

  @ParameterizedTest
  @CsvSource(value = {
      "upsert",
      "insert",
      "bulk_insert"
  })
  public void testWriteOperationDoesNotTriggerRepeatedDAG(String operation) throws Exception {
    // Configure 3 transformers of same type. 2nd transformer has no suffix
    StageListener stageListener = new StageListener("org.apache.hudi.client.BaseHoodieClient.finalizeWrite");
    sparkSession.sparkContext().addSparkListener(stageListener);
    runDeltaStreamer(WriteOperationType.fromValue(operation), false, Option.empty());
    assertEquals(1, stageListener.triggerCount);
  }

  @Test
  public void testClusteringDoesNotTriggerRepeatedDAG() throws Exception {
    // Configure 3 transformers of same type. 2nd transformer has no suffix
    StageListener stageListener = new StageListener("org.apache.hudi.table.action.commit.BaseCommitActionExecutor.executeClustering");
    sparkSession.sparkContext().addSparkListener(stageListener);
    List<String> configs = getTableServicesConfigs(100, "false", "true", "1", "", "");
    runDeltaStreamer(WriteOperationType.UPSERT, false, Option.of(configs));
    assertEquals(1, stageListener.triggerCount);
  }

  @Test
  public void testCompactionDoesNotTriggerRepeatedDAG() throws Exception {
    // Configure 3 transformers of same type. 2nd transformer has no suffix
    StageListener stageListener = new StageListener("collect at SparkRDDTableServiceClient");
    sparkSession.sparkContext().addSparkListener(stageListener);
    List<String> configs = Arrays.asList("hoodie.compact.inline.max.delta.commits=1", "hoodie.compact.inline=true");
    runDeltaStreamer(WriteOperationType.UPSERT, true, Option.of(configs));
    assertEquals(1, stageListener.triggerCount);
  }

  private void runDeltaStreamer(WriteOperationType operationType, boolean shouldGenerateUpdates, Option<List<String>> configsOpt) throws Exception {
    // Create source using TRIP_SCHEMA
    boolean useSchemaProvider = true;
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    HoodieTestDataGenerator dataGenerator = prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, true, "source.avsc", "source.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "");
    String tableBasePath = basePath + "/runDeltaStreamer" + testNum;
    FileIOUtils.deleteDirectory(new File(tableBasePath));
    HoodieDeltaStreamer.Config config = TestHelpers.makeConfig(tableBasePath, operationType,
        ParquetDFSSource.class.getName(), null, PROPS_FILENAME_TEST_PARQUET, false,
        useSchemaProvider, 100000, false, null, HoodieTableType.MERGE_ON_READ.name(), "timestamp", null);
    configsOpt.ifPresent(cfgs -> config.configs.addAll(cfgs));
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(config, jsc);

    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    testNum++;
    deltaStreamer.shutdownGracefully();

    if (shouldGenerateUpdates) {
      prepareParquetDFSUpdates(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null, dataGenerator, "001");
      HoodieDeltaStreamer updateDs = new HoodieDeltaStreamer(config, jsc);
      updateDs.sync();
      updateDs.shutdownGracefully();
    }
  }

  /** ********** Stage Event Listener ************* */
  private static class StageListener extends SparkListener {
    int triggerCount = 0;
    private final String eventToTrack;

    private StageListener(String eventToTrack) {
      this.eventToTrack = eventToTrack;
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
      if (stageCompleted.stageInfo().name().contains(eventToTrack)) {
        triggerCount += 1;
      }
    }
  }
}
