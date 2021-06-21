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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bootstrap.BootstrapFunction;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.compact.CompactionPlanSourceFunction;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for Flink Hoodie stream sink.
 */
public class StreamWriteITCase extends TestLogger {

  protected static final Logger LOG = LoggerFactory.getLogger(StreamWriteITCase.class);

  private static final Map<String, List<String>> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));
  }

  @TempDir
  File tempFile;

  @Test
  public void testWriteToHoodie() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
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
    StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
        new StreamWriteOperatorFactory<>(conf);

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream<HoodieRecord> hoodieDataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4)
        .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class));

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      hoodieDataStream = hoodieDataStream.transform("index_bootstrap",
          TypeInformation.of(HoodieRecord.class),
          new ProcessOperator<>(new BootstrapFunction<>(conf)));
    }

    DataStream<Object> pipeline = hoodieDataStream
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        .keyBy(HoodieRecord::getRecordKey)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner")
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write");
    execEnv.addOperator(pipeline.getTransformation());

    JobClient client = execEnv.executeAsync(execEnv.getStreamGraph(conf.getString(FlinkOptions.TABLE_NAME)));
    // wait for the streaming job to finish
    client.getJobExecutionResult().get();

    TestData.checkWrittenFullData(tempFile, EXPECTED);
  }

  @Test
  public void testHoodieFlinkCompactor() throws Exception {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";
    tableEnv.executeSql(insertInto).await();

    // wait for the asynchronous commit to finish
    TimeUnit.SECONDS.sleep(3);

    // Make configuration and setAvroSchema.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tempFile.getAbsolutePath();
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);
    conf.setString(FlinkOptions.TABLE_TYPE.key(), "MERGE_ON_READ");

    // create metaClient
    HoodieTableMetaClient metaClient = CompactionUtil.createMetaClient(conf);

    // set the table name
    conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

    // set table schema
    CompactionUtil.setAvroSchema(conf, metaClient);

    // judge whether have operation
    // To compute the compaction instant time and do compaction.
    String compactionInstantTime = CompactionUtil.getCompactionInstantTime(metaClient);
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf, null);
    writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());

    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);

    env.addSource(new CompactionPlanSourceFunction(table, instant, compactionPlan, compactionInstantTime))
        .name("compaction_source")
        .uid("uid_compaction_source")
        .rebalance()
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(conf)))
        .setParallelism(compactionPlan.getOperations().size())
        .addSink(new CompactionCommitSink(conf))
        .name("clean_commits")
        .uid("uid_clean_commits")
        .setParallelism(1);

    env.execute("flink_hudi_compaction");
    TestData.checkWrittenFullData(tempFile, EXPECTED);
  }

  @Test
  public void testMergeOnReadWriteWithCompaction() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
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
    StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
        new StreamWriteOperatorFactory<>(conf);

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    TextInputFormat format = new TextInputFormat(new Path(sourcePath));
    format.setFilesFilter(FilePathFilter.createDefaultFilter());
    TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
    format.setCharsetName("UTF-8");

    DataStream<HoodieRecord> hoodieDataStream = execEnv
        // use PROCESS_CONTINUOUSLY mode to trigger checkpoint
        .readFile(format, sourcePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4)
        .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class));

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      hoodieDataStream = hoodieDataStream.transform("index_bootstrap",
          TypeInformation.of(HoodieRecord.class),
          new ProcessOperator<>(new BootstrapFunction<>(conf)));
    }

    DataStream<Object> pipeline = hoodieDataStream
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        .keyBy(HoodieRecord::getRecordKey)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner")
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write");

    pipeline.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits").uid("uid_clean_commits");

    pipeline.transform("compact_plan_generate",
        TypeInformation.of(CompactionPlanEvent.class),
        new CompactionPlanOperator(conf))
        .uid("uid_compact_plan_generate")
        .setParallelism(1) // plan generate must be singleton
        .rebalance()
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(conf)))
        .addSink(new CompactionCommitSink(conf))
        .name("compact_commit")
        .setParallelism(1);

    JobClient client = execEnv.executeAsync(execEnv.getStreamGraph(conf.getString(FlinkOptions.TABLE_NAME)));
    if (client.getJobStatus().get() != JobStatus.FAILED) {
      try {
        TimeUnit.SECONDS.sleep(20); // wait long enough for the compaction to finish
        client.cancel();
      } catch (Throwable var1) {
        // ignored
      }
    }

    TestData.checkWrittenFullData(tempFile, EXPECTED);
  }
}
