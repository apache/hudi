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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.Properties;

/**
 * An Utility which can incrementally consume data from Kafka and apply it to the target table.
 * currently, it only supports COW table and insert, upsert operation.
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

    Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);

    // Read from kafka source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    int parallelism = env.getParallelism();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

    StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
        new StreamWriteOperatorFactory<>(conf);

    DataStream<RowData> dataStream = null;

    if (cfg.sourceType.equals(FlinkStreamerType.KAFKA.getName())) {
      Properties kafkaProps = StreamerUtil.appendKafkaProps(cfg);
      dataStream = env.addSource(new FlinkKafkaConsumer<>(
              cfg.kafkaTopic,
              new JsonRowDataDeserializationSchema(
                      rowType,
                      InternalTypeInfo.of(rowType),
                      false,
                      true,
                      TimestampFormat.ISO_8601
              ), kafkaProps))
              .name("kafka_source")
              .uid("uid_kafka_source");
    } else if (cfg.sourceType.equals(FlinkStreamerType.MYSQL_CDC.getName())) {
      SourceFunction<RowData> sourceFunction = MySqlSource.<RowData>builder()
              .hostname(cfg.mysqlCdcHost)
              .port(cfg.mysqlCdcPort)
              .databaseList(cfg.mysqlCdcDbs.toArray(new String[cfg.mysqlCdcDbs.size()]))
              .tableList(cfg.mysqlCdcTables.toArray(new String[cfg.mysqlCdcTables.size()]))
              .username(cfg.mysqlCdcUser)
              .password(cfg.mysqlCdcPassword)
              .serverId(cfg.mysqlCdcServerId)
              .deserializer(new RowDataDebeziumDeserializeSchema(rowType,
                      InternalTypeInfo.of(rowType),(rowData, rowKind) -> {}, ZoneId.systemDefault()))
              .build();
      dataStream  = env.addSource(sourceFunction)
              .name("mysql_source")
              .uid("uid_mysql_source");
    } else {
      final String errorMsg = String.format("Data source type [%s] is not supported.",
              cfg.sourceType);
      throw new HoodieException(errorMsg);
    }

    if (cfg.transformerClassNames != null && !cfg.transformerClassNames.isEmpty()) {
      Option<Transformer> transformer = StreamerUtil.createTransformer(cfg.transformerClassNames);
      if (transformer.isPresent()) {
        dataStream = transformer.get().apply(dataStream);
      }
    }

    DataStream<HoodieRecord> dataStream2 = dataStream
        .map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class));

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      dataStream2 = dataStream2
          .transform(
              "index_bootstrap",
              TypeInformation.of(HoodieRecord.class),
              new BootstrapOperator<>(conf))
          .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(parallelism))
          .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
    }

    DataStream<Object> pipeline = dataStream2
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        .keyBy(HoodieRecord::getRecordKey)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getOptional(FlinkOptions.BUCKET_ASSIGN_TASKS).orElse(parallelism))
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
    if (StreamerUtil.needsAsyncCompaction(conf)) {
      pipeline.transform("compact_plan_generate",
          TypeInformation.of(CompactionPlanEvent.class),
          new CompactionPlanOperator(conf))
          .uid("uid_compact_plan_generate")
          .setParallelism(1) // plan generate must be singleton
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new ProcessOperator<>(new CompactFunction(conf)))
          .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
          .addSink(new CompactionCommitSink(conf))
          .name("compact_commit")
          .setParallelism(1); // compaction commit should be singleton
    } else {
      pipeline.addSink(new CleanFunction<>(conf))
          .setParallelism(1)
          .name("clean_commits").uid("uid_clean_commits");
    }

    env.execute(cfg.targetTableName);
  }
}
