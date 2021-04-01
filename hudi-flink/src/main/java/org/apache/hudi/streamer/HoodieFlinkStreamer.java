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

package org.apache.hudi.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CommitSink;
import org.apache.hudi.sink.InstantGenerateOperator;
import org.apache.hudi.sink.KeyedWriteProcessFunction;
import org.apache.hudi.sink.KeyedWriteProcessOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.JsonStringToHoodieRecordMapFunction;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.List;
import java.util.Objects;

/**
 * An Utility which can incrementally consume data from Kafka and apply it to the target table.
 * currently, it only support COW table and insert, upsert operation.
 */
public class HoodieFlinkStreamer {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    env.enableCheckpointing(cfg.checkpointInterval);
    env.getConfig().setGlobalJobParameters(cfg);
    // We use checkpoint to trigger write operation, including instant generating and committing,
    // There can only be one checkpoint at one time.
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    if (cfg.flinkCheckPointPath != null) {
      env.setStateBackend(new FsStateBackend(cfg.flinkCheckPointPath));
    }

    Configuration conf = FlinkOptions.fromStreamerConfig(cfg);
    int numWriteTask = conf.getInteger(FlinkOptions.WRITE_TASKS);

    TypedProperties props = StreamerUtil.appendKafkaProps(cfg);

    // add data source config
    props.put(HoodieWriteConfig.WRITE_PAYLOAD_CLASS, cfg.payloadClassName);
    props.put(HoodieWriteConfig.PRECOMBINE_FIELD_PROP, cfg.sourceOrderingField);

    StreamerUtil.initTableIfNotExists(conf);
    // Read from kafka source
    DataStream<HoodieRecord> inputRecords =
        env.addSource(new FlinkKafkaConsumer<>(cfg.kafkaTopic, new SimpleStringSchema(), props))
            .filter(Objects::nonNull)
            .map(new JsonStringToHoodieRecordMapFunction(props))
            .name("kafka_to_hudi_record")
            .uid("kafka_to_hudi_record_uid");

    inputRecords.transform(InstantGenerateOperator.NAME, TypeInformation.of(HoodieRecord.class), new InstantGenerateOperator())
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
        .setParallelism(numWriteTask)

        // Commit can only be executed once, so make it one parallelism
        .addSink(new CommitSink())
        .name("commit_sink")
        .uid("commit_sink_uid")
        .setParallelism(1);

    env.execute(cfg.targetTableName);
  }
}
