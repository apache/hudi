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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        new RowDataTypeInfo(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream<Object> dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4)
        .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class))
        // Key-by partition path, to avoid multiple subtasks write to a partition at the same time
        .keyBy(HoodieRecord::getPartitionPath)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner")
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", null, operatorFactory)
        .uid("uid_hoodie_stream_write");
    execEnv.addOperator(dataStream.getTransformation());

    JobClient client = execEnv.executeAsync(execEnv.getStreamGraph(conf.getString(FlinkOptions.TABLE_NAME)));
    // wait for the streaming job to finish
    client.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

    TestData.checkWrittenFullData(tempFile, EXPECTED);
  }

  @Test
  public void testWriteToHoodieLegacy() throws Exception {
    FlinkStreamerConfig streamerConf = TestConfigurations.getDefaultStreamerConf(tempFile.getAbsolutePath());
    Configuration conf = FlinkOptions.fromStreamerConfig(streamerConf);
    StreamerUtil.initTableIfNotExists(conf);
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getConfig().setGlobalJobParameters(streamerConf);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        new RowDataTypeInfo(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4)
        .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class))
        .transform(InstantGenerateOperator.NAME, TypeInformation.of(HoodieRecord.class), new InstantGenerateOperator())
        .name("instant_generator")
        .uid("instant_generator_id")

        // Keyby partition path, to avoid multiple subtasks writing to a partition at the same time
        .keyBy(HoodieRecord::getPartitionPath)
        // use the bucket assigner to generate bucket IDs
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner")
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        // write operator, where the write operation really happens
        .transform(KeyedWriteProcessOperator.NAME, TypeInformation.of(new TypeHint<Tuple3<String, List<WriteStatus>, Integer>>() {
        }), new KeyedWriteProcessOperator(new KeyedWriteProcessFunction()))
        .name("write_process")
        .uid("write_process_uid")
        .setParallelism(4)

        // Commit can only be executed once, so make it one parallelism
        .addSink(new CommitSink())
        .name("commit_sink")
        .uid("commit_sink_uid")
        .setParallelism(1);

    JobClient client = execEnv.executeAsync(execEnv.getStreamGraph(conf.getString(FlinkOptions.TABLE_NAME)));
    // wait for the streaming job to finish
    client.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

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
        new RowDataTypeInfo(rowType),
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

    execEnv
        // use PROCESS_CONTINUOUSLY mode to trigger checkpoint
        .readFile(format, sourcePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4)
        .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class))
        // Key-by partition path, to avoid multiple subtasks write to a partition at the same time
        .keyBy(HoodieRecord::getPartitionPath)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner")
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write")
        .transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
        .uid("uid_compact_plan_generate")
        .setParallelism(1) // plan generate must be singleton
        .keyBy(event -> event.getOperation().hashCode())
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new KeyedProcessOperator<>(new CompactFunction(conf)))
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
