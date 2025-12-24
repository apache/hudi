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

package org.apache.hudi.table;

import org.apache.hudi.adapter.DataStreamSinkProviderAdapter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.clustering.LSMClusteringScheduleMode;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.ChangelogModes;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final Configuration conf;
  private final ResolvedSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, ResolvedSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  public HoodieTableSink(Configuration conf, ResolvedSchema schema, boolean overwrite) {
    this.conf = conf;
    this.schema = schema;
    this.overwrite = overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProviderAdapter) dataStream -> {

      // setup configuration
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

      if (conf.get(FlinkOptions.LSM_CLUSTERING_SCHEDULE_INTERVAL) == -1L) {
        conf.set(FlinkOptions.LSM_CLUSTERING_SCHEDULE_INTERVAL, dataStream.getExecutionEnvironment().getCheckpointConfig()
            .getCheckpointInterval() / 1000 * conf.getInteger(FlinkOptions.LSM_CLUSTERING_DELTA_COMMITS));
      }
      // set up default parallelism
      OptionsInference.setupSinkTasks(conf, dataStream.getExecutionConfig().getParallelism());

      RowType rowType = (RowType) schema.toSinkRowDataType().notNull().getLogicalType();

      // bulk_insert mode
      final String writeOperation = this.conf.get(FlinkOptions.OPERATION);
      if (WriteOperationType.fromValue(writeOperation) == WriteOperationType.BULK_INSERT) {
        return Pipelines.bulkInsert(conf, rowType, dataStream);
      }

      // Append mode
      if (OptionsResolver.isAppendMode(conf)) {
        DataStream<Object> pipeline = Pipelines.append(conf, rowType, dataStream, context.isBounded());
        if (OptionsResolver.isAsyncRollback(conf)) {
          pipeline = Pipelines.rollback(conf, pipeline);
        }
        // async archive
        if (OptionsResolver.needsAsyncArchive(conf)) {
          pipeline = Pipelines.archive(conf, pipeline);
        }
        if (OptionsResolver.needsAsyncClustering(conf)) {
          return Pipelines.cluster(conf, rowType, pipeline);
        } else {
          return Pipelines.dummySink(pipeline);
        }
      }

      DataStream<Object> pipeline;
      // Bucket Index and incremental rt chain
      DataStream<RowData> finalDataStream;
      String rtChainMode = conf.get(FlinkOptions.RT_CHAIN_MODE);
      if (rtChainMode == null) {
        rtChainMode = dataStream.getExecutionConfig().getGlobalJobParameters().toMap().get(FlinkOptions.RT_CHAIN_MODE.key());
      }
      if (rtChainMode != null && rtChainMode.equalsIgnoreCase("incremental")) {
        finalDataStream = Pipelines.backFillDeletedRecordsInRTIncrementalChain(dataStream, conf, rowType);
      } else {
        finalDataStream = dataStream;
      }

      if (OptionsResolver.isLSMBasedLogFormat(conf)) {
        // in LSM Based format, skip bootstrap and do parquet create directly
        pipeline = Pipelines.hoodieLsmStreamWrite(conf, rowType, dataStream);
        if (OptionsResolver.areTableServicesEnabled(conf)) {
          if (LSMClusteringScheduleMode.STREAM.name().equalsIgnoreCase(conf.getString(FlinkOptions.LSM_CLUSTERING_SCHEDULE_MODE))) {
            pipeline = Pipelines.lsmClusterPlan(conf, pipeline, rowType);
          }

          if (OptionsResolver.needsAsyncClustering(conf)) {
            return Pipelines.lsmClusterExecute(conf, rowType, pipeline);
          } else if (conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
            return Pipelines.clean(conf, pipeline);
          }
        } else {
          return Pipelines.dummySink(pipeline);
        }
      } else {
        // bootstrap
        final DataStream<HoodieRecord> hoodieRecordDataStream =
            Pipelines.bootstrap(conf, rowType, finalDataStream, context.isBounded(), overwrite);
        // write pipeline
        pipeline = Pipelines.hoodieStreamWrite(conf, hoodieRecordDataStream);
      }
      // async archive
      if (OptionsResolver.needsAsyncArchive(conf)) {
        pipeline = Pipelines.archive(conf, pipeline);
      }

      if (OptionsResolver.isAsyncRollback(conf)) {
        pipeline = Pipelines.rollback(conf, pipeline);
      }
      // compaction
      if (OptionsResolver.needsAsyncCompaction(conf)) {
        // use synchronous compaction for bounded source.
        if (context.isBounded()) {
          conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
        }
        if (conf.get(FlinkOptions.COMPACTION_SCANNER_TYPE).equals(FlinkOptions.SCANNER_WITH_FK)) {
          return Pipelines.compactWithFK(conf, pipeline);
        } else {
          return Pipelines.compact(conf, pipeline);
        }
      } else {
        return Pipelines.clean(conf, pipeline);
      }
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    if (conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED)) {
      return ChangelogModes.FULL;
    } else {
      return ChangelogModes.UPSERT;
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.schema, this.overwrite);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partitions) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite && partitions.size() > 0) {
      this.conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    }
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    // set up the operation as INSERT_OVERWRITE_TABLE first,
    // if there are explicit partitions, #applyStaticPartition would overwrite the option.
    this.conf.setString(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE_TABLE.value());
  }
}
