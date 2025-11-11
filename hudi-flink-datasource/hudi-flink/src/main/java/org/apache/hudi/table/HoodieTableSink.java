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
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.DataModificationInfos;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements
    DynamicTableSink,
    SupportsPartitioning,
    SupportsOverwrite,
    SupportsRowLevelDelete,
    SupportsRowLevelUpdate {

  private final Configuration conf;
  private final ResolvedSchema recordSchema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, ResolvedSchema recordSchema) {
    this.conf = conf;
    this.recordSchema = recordSchema;
  }

  public HoodieTableSink(Configuration conf, ResolvedSchema recordSchema, boolean overwrite) {
    this.conf = conf;
    this.recordSchema = recordSchema;
    this.overwrite = overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProviderAdapter) dataStream -> {

      // setup configuration
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
      // set up default parallelism
      OptionsInference.setupSinkTasks(conf, dataStream.getExecutionConfig().getParallelism());
      // set up client id
      OptionsInference.setupClientId(conf);
      // set up flink runtime configurations.
      OptionsInference.setupRuntimeConfigs(conf, dataStream.getExecutionEnvironment().getConfiguration());
      // initialize table, create if not exists.
      StreamerUtil.initTableFromClientIfNecessary(conf);
      // set up index related configs
      OptionsInference.setupIndexConfigs(conf);
      Schema writerSchema = StreamerUtil.deduceWriterSchema(conf);
      // setup write avro schema
      conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, writerSchema.toString());

      RowType recordRowType = (RowType) recordSchema.toSinkRowDataType().notNull().getLogicalType();
      RowType writerRowType = (RowType) AvroSchemaConverter.convertToDataType(writerSchema).getLogicalType();

      // bulk_insert mode
      if (OptionsResolver.isBulkInsertOperation(conf)) {
        if (!context.isBounded()) {
          throw new HoodieException(
              "The bulk insert should be run in batch execution mode.");
        }
        return Pipelines.dummySink(Pipelines.bulkInsert(conf, recordRowType, writerRowType, dataStream));
      }

      // Append mode
      if (OptionsResolver.isAppendMode(conf)) {
        // close compaction for append mode
        conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
        DataStream<RowData> pipeline = Pipelines.append(conf, recordRowType, writerRowType, dataStream);
        if (OptionsResolver.needsAsyncClustering(conf)) {
          return Pipelines.cluster(conf, writerRowType, pipeline);
        } else if (OptionsResolver.isLazyFailedWritesCleanPolicy(conf)) {
          // add clean function to rollback failed writes for lazy failed writes cleaning policy
          return Pipelines.clean(conf, pipeline);
        } else {
          return Pipelines.dummySink(pipeline);
        }
      }

      // process dataStream and write corresponding files
      DataStream<RowData> pipeline;
      final DataStream<HoodieFlinkInternalRow> hoodieRecordDataStream =
          Pipelines.bootstrap(conf, recordRowType, writerRowType, dataStream, context.isBounded(), overwrite);
      pipeline = Pipelines.hoodieStreamWrite(conf, writerRowType, hoodieRecordDataStream);
      // compaction
      if (OptionsResolver.needsAsyncCompaction(conf)) {
        // use synchronous compaction for bounded source.
        if (context.isBounded()) {
          conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
        }
        return Pipelines.compact(conf, pipeline);
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
    if (conf.get(FlinkOptions.CHANGELOG_ENABLED)) {
      return ChangelogModes.FULL;
    } else {
      return ChangelogModes.UPSERT;
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.recordSchema, this.overwrite);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partitions) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite && !partitions.isEmpty()) {
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    }
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    if (OptionsResolver.overwriteDynamicPartition(conf)) {
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE.value());
    } else {
      // if there are explicit partitions, #applyStaticPartition would overwrite the option.
      this.conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT_OVERWRITE_TABLE.value());
    }
  }

  @Override
  public RowLevelDeleteInfo applyRowLevelDelete(@Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
    this.conf.set(FlinkOptions.OPERATION, WriteOperationType.DELETE.value());
    return DataModificationInfos.DEFAULT_DELETE_INFO;
  }

  @Override
  public RowLevelUpdateInfo applyRowLevelUpdate(List<Column> list, @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
    this.conf.set(FlinkOptions.OPERATION, WriteOperationType.UPSERT.value());
    return DataModificationInfos.DEFAULT_UPDATE_INFO;
  }
}
