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

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.StreamerUtils;

import com.beust.jcommander.JCommander;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * A utility which can incrementally consume data from Kafka and apply it to the target table.
 * It has the similar functionality with SQL data source except that the source is bind to Kafka
 * and the format is bind to JSON.
 */
public class HoodieFlinkStreamer {
  public static void main(String[] args) throws Exception {

    final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    Configuration envConf = new Configuration();
    envConf.set(StateBackendOptions.STATE_BACKEND, cfg.stateBackend.getName());
    envConf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    if (cfg.flinkCheckPointPath != null) {
      envConf.set(CheckpointingOptions. CHECKPOINTS_DIRECTORY, cfg.flinkCheckPointPath);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(envConf);
    env.enableCheckpointing(cfg.checkpointInterval);
    env.getConfig().setGlobalJobParameters(cfg);
    // We use checkpoint to trigger write operation, including instant generating and committing,
    // There can only be one checkpoint at one time.
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    TypedProperties kafkaProps = DFSPropertiesConfiguration.getGlobalProps();
    kafkaProps.putAll(StreamerUtil.appendKafkaProps(cfg));

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
    // Read from kafka source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

    DataStream<RowData> dataStream = StreamerUtils.createKafkaStream(env, rowType, cfg.kafkaTopic, kafkaProps);
    if (cfg.transformerClassNames != null && !cfg.transformerClassNames.isEmpty()) {
      Option<Transformer> transformer = StreamerUtil.createTransformer(cfg.transformerClassNames);
      if (transformer.isPresent()) {
        dataStream = transformer.get().apply(dataStream);
      }
    }

    OptionsInference.setupSinkTasks(conf, env.getParallelism());
    OptionsInference.setupClientId(conf);
    DataStream<RowData> pipeline;
    // Append mode
    if (OptionsResolver.isAppendMode(conf)) {
      // append mode should not compaction operator
      conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
      pipeline = Pipelines.append(conf, rowType, dataStream);
      if (OptionsResolver.needsAsyncClustering(conf)) {
        Pipelines.cluster(conf, rowType, pipeline);
      } else if (OptionsResolver.isLazyFailedWritesCleanPolicy(conf)) {
        // add clean function to rollback failed writes for lazy failed writes cleaning policy
        Pipelines.clean(conf, pipeline);
      } else {
        Pipelines.dummySink(pipeline);
      }
    } else {
      DataStream<HoodieFlinkInternalRow> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, dataStream);
      pipeline = Pipelines.hoodieStreamWrite(conf, rowType, hoodieRecordDataStream);
      if (OptionsResolver.needsAsyncCompaction(conf)) {
        Pipelines.compact(conf, pipeline);
      } else {
        Pipelines.clean(conf, pipeline);
      }
    }

    env.execute(cfg.targetTableName);
  }
}
