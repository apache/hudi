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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.bootstrap.BootstrapFunction;
import org.apache.hudi.sink.bulk.BulkInsertWriteOperator;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode$;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final Configuration conf;
  private final TableSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, TableSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  public HoodieTableSink(Configuration conf, TableSchema schema, boolean overwrite) {
    this.conf = conf;
    this.schema = schema;
    this.overwrite = overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) dataStream -> {

      // setup configuration
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

      RowType rowType = (RowType) schema.toRowDataType().notNull().getLogicalType();

      // bulk_insert mode
      final String writeOperation = this.conf.get(FlinkOptions.OPERATION);
      if (WriteOperationType.fromValue(writeOperation) == WriteOperationType.BULK_INSERT) {
        BulkInsertWriteOperator.OperatorFactory<RowData> operatorFactory = BulkInsertWriteOperator.getFactory(this.conf, rowType);

        final String[] partitionFields = FilePathUtils.extractPartitionKeys(this.conf);
        if (partitionFields.length > 0) {
          RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
          if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_BY_PARTITION)) {

            // shuffle by partition keys
            dataStream = dataStream.keyBy(rowDataKeyGen::getPartitionPath);
          }
          if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_BY_PARTITION)) {
            SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, partitionFields);
            // sort by partition keys
            dataStream = dataStream
                .transform("partition_key_sorter",
                    TypeInformation.of(RowData.class),
                    sortOperatorGen.createSortOperator())
                .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
            ExecNode$.MODULE$.setManagedMemoryWeight(dataStream.getTransformation(),
                conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
          }
        }
        return dataStream
            .transform("hoodie_bulk_insert_write",
                TypeInformation.of(Object.class),
                operatorFactory)
            // follow the parallelism of upstream operators to avoid shuffle
            .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS))
            .addSink(new CleanFunction<>(conf))
            .setParallelism(1)
            .name("clean_commits");
      }

      // stream write
      int parallelism = dataStream.getExecutionConfig().getParallelism();
      StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);

      DataStream<HoodieRecord> dataStream1 = dataStream
          .map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class));

      // bootstrap index
      // TODO: This is a very time-consuming operation, will optimization
      if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
        dataStream1 = dataStream1.rebalance()
            .transform(
                "index_bootstrap",
                TypeInformation.of(HoodieRecord.class),
                new ProcessOperator<>(new BootstrapFunction<>(conf)))
            .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(parallelism))
            .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
      }

      DataStream<Object> pipeline = dataStream1
          // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
          .keyBy(HoodieRecord::getRecordKey)
          .transform(
              "bucket_assigner",
              TypeInformation.of(HoodieRecord.class),
              new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
          .uid("uid_bucket_assigner_" + conf.getString(FlinkOptions.TABLE_NAME))
          .setParallelism(conf.getOptional(FlinkOptions.BUCKET_ASSIGN_TASKS).orElse(parallelism))
          // shuffle by fileId(bucket id)
          .keyBy(record -> record.getCurrentLocation().getFileId())
          .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
          .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
      // compaction
      if (StreamerUtil.needsAsyncCompaction(conf)) {
        return pipeline.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
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
        return pipeline.addSink(new CleanFunction<>(conf))
            .setParallelism(1)
            .name("clean_commits");
      }
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    // ignore RowKind.UPDATE_BEFORE
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
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
  public void applyStaticPartition(Map<String, String> partition) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite) {
      final String operationType;
      if (partition.size() > 0) {
        operationType = WriteOperationType.INSERT_OVERWRITE.value();
      } else {
        operationType = WriteOperationType.INSERT_OVERWRITE_TABLE.value();
      }
      this.conf.setString(FlinkOptions.OPERATION, operationType);
    }
  }

  @Override
  public void applyOverwrite(boolean b) {
    this.overwrite = b;
  }
}
