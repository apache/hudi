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

package org.apache.hudi.sink;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.sink.v2.utils.PipelinesV2;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for Flink Hoodie stream V2 sink.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestDataStreamV2Write {

  private static final Map<String, List<String>> EXPECTED = new HashMap<String, List<String>>() {{
      put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
      put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
      put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
      put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));
    }};

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteCopyOnWrite(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, indexType);
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 1);
    conf.set(FlinkOptions.PRE_COMBINE, true);

    writeAndCheckExpected(conf, "cow_write", 2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testMergeOnReadWithCompaction(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.INDEX_TYPE, indexType);
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    writeAndCheckExpected(conf, "mor_write_with_compact", 1);
  }

  @Test
  public void testAppendWrite() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.OPERATION, "insert");

    writeAndCheckExpected(conf, "append_write", 1);
  }

  private void writeAndCheckExpected(
      Configuration conf,
      String jobName,
      int checkpoints) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf).getAvroSchema())
            .getLogicalType();

    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    boolean isMor = conf.get(FlinkOptions.TABLE_TYPE).equals(HoodieTableType.MERGE_ON_READ.name());

    DataStream<RowData> dataStream;

    dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(rowType))
        .setParallelism(4);

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());

    PipelinesV2.sink(dataStream, conf, rowType, false, false);

    execute(execEnv, isMor, jobName);
    TestData.checkWrittenDataCOW(tempFile, EXPECTED);
  }

  private void execute(StreamExecutionEnvironment execEnv, boolean isMor, String jobName) throws Exception {
    if (isMor) {
      JobClient client = execEnv.executeAsync(jobName);
      if (client.getJobStatus().get() != JobStatus.FAILED) {
        try {
          TimeUnit.SECONDS.sleep(35); // wait long enough for the compaction to finish
          client.cancel();
        } catch (Throwable var1) {
          // ignored
        }
      }
    } else {
      // wait for the streaming job to finish
      execEnv.execute(jobName);
    }
  }
}
