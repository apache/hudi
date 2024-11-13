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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestStreamSync extends HoodieDeltaStreamerTestBase {

  @Test
  void testStreamSyncStartCommit() throws Exception {
    String parquetSourceRoot = basePath + "parquetFilesDfs" + "startCommit";
    String propsFilenameTestParquet = "test-parquet-dfs-source.properties";
    int parquetRecordsCount = 100;
    HoodieTestDataGenerator dataGenerator = prepareParquetDFSFiles(parquetRecordsCount, parquetSourceRoot, "1.parquet", false, null, null);
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.compact.inline", "true");
    extraProps.setProperty("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    extraProps.setProperty("hoodie.datasource.compaction.async.enable", "false");
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", propsFilenameTestParquet,
        parquetSourceRoot, false, "partition_path", "", extraProps, false);
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config deltaCfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, propsFilenameTestParquet, false,
        false, 100000, false, null, "MERGE_ON_READ", "timestamp", null);
    deltaCfg.retryLastPendingInlineCompactionJob = false;

    // trigger deltacommit.
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(deltaCfg, jsc);
    HoodieStreamer.StreamSyncService streamSyncService = (HoodieStreamer.StreamSyncService) deltaStreamer.getIngestionService();
    streamSyncService.getStreamSync().syncOnce();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    prepareParquetDFSUpdates(100, parquetSourceRoot, "2.parquet", false, null, null, dataGenerator, "001");
    streamSyncService.getStreamSync().syncOnce();

    TypedProperties streamProps = UtilHelpers.readConfig(hadoopConf, new Path(basePath + "/" + propsFilenameTestParquet), Collections.emptyList()).getProps();
    streamProps.setProperty("hoodie.compact.inline.max.delta.commits", "1");
    streamProps.setProperty("hoodie.base.path", tableBasePath);
    streamProps.setProperty("hoodie.datasource.write.operation", WriteOperationType.UPSERT.value());
    streamProps.setProperty(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "timestamp");
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), HoodieWriteConfig.newBuilder().withProperties(streamProps).build())) {
      Assertions.assertTrue(writeClient.scheduleCompaction(Option.empty()).isPresent());
    }
    Assertions.assertThrows(IllegalArgumentException.class, () -> streamSyncService.getStreamSync().startCommit("0"));
    String validInstantTime = HoodieActiveTimeline.createNewInstantTime();
    Assertions.assertDoesNotThrow(() -> streamSyncService.getStreamSync().startCommit(validInstantTime));
  }
}