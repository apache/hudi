/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.JsonDeserializationFunction;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration test for Flink Hoodie stream sink of consistent bucket index.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestConsistentBucketStreamWrite extends TestLogger {

  private static final Map<String, String> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", "[id1,par1,id1,Danny,23,1000,par1, id2,par1,id2,Stephen,33,2000,par1]");
    EXPECTED.put("par2", "[id3,par2,id3,Julian,53,3000,par2, id4,par2,id4,Fabian,31,4000,par2]");
    EXPECTED.put("par3", "[id5,par3,id5,Sophia,18,5000,par3, id6,par3,id6,Emma,20,6000,par3]");
    EXPECTED.put("par4", "[id7,par4,id7,Bob,44,7000,par4, id8,par4,id8,Han,56,8000,par4]");
  }

  @TempDir
  File tempFile;

  @Test
  public void testWriteMOR() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, "CONSISTENT_HASHING");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    testWriteToHoodie(conf, "mor_write", 1, EXPECTED);
  }

  @Test
  public void testWriteMORWithResizePlan() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, "CONSISTENT_HASHING");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key(), "8");
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    // Enable inline resize scheduling
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    // Manually set the max commits to trigger clustering quickly
    conf.setString(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key(), "1");
    // Manually set the split threshold to trigger split in the clustering
    conf.set(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, 1);
    conf.setString(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), String.valueOf(1 / 1024.0 / 1024.0));
    testWriteToHoodie(conf, "mor_write", 1, EXPECTED);
  }

  @Test
  public void testBulkInsert() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, "CONSISTENT_HASHING");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "bulk_insert");

    // expect HoodieException for bulk insert
    assertThrows(
        HoodieException.class,
        () -> testWriteToHoodie(conf, "bulk_insert", 1, EXPECTED));
  }

  @Test
  public void testOverwrite() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, "CONSISTENT_HASHING");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "INSERT_OVERWRITE");

    // expect HoodieException for overwrite
    assertThrows(
        HoodieException.class,
        () -> testWriteToHoodie(conf, "overwrite", 1, EXPECTED));
  }

  private void testWriteToHoodie(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, String> expected) throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream<RowData> dataStream =
        execEnv
            // use continuous file source to trigger checkpoint
            .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
            .name("continuous_file_source")
            .setParallelism(1)
            .map(JsonDeserializationFunction.getInstance(rowType))
            .setParallelism(4);

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    // bulk_insert mode
    if (OptionsResolver.isBulkInsertOperation(conf)) {
      Pipelines.bulkInsert(conf, rowType, dataStream);
    } else {
      DataStream<HoodieFlinkInternalRow> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, dataStream);
      DataStream<RowData> pipeline = Pipelines.hoodieStreamWrite(conf, rowType, hoodieRecordDataStream);
      execEnv.addOperator(pipeline.getTransformation());
    }
    JobClient client = execEnv.executeAsync(jobName);
    if (client.getJobStatus().get() != JobStatus.FAILED) {
      try {
        TimeUnit.SECONDS.sleep(30);
        client.cancel();
      } catch (Throwable var1) {
        // ignored
      }
    }
    TestData.checkWrittenDataMOR(tempFile, expected, 4);
  }
}
