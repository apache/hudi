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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.bootstrap.batch.BatchBootstrapOperator;
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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode$;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities to generate all kinds of sub-pipelines.
 */
public class Pipelines {

  public static DataStreamSink<Object> bulkInsert(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    BulkInsertWriteOperator.OperatorFactory<RowData> operatorFactory = BulkInsertWriteOperator.getFactory(conf, rowType);

    final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
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

  public static DataStream<HoodieRecord> bootstrap(Configuration conf, RowType rowType, int defaultParallelism, DataStream<RowData> dataStream) {
    DataStream<HoodieRecord> dataStream1 = rowDataToHoodieRecord(conf, rowType, dataStream);

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      dataStream1 = dataStream1
          .transform(
              "index_bootstrap",
              TypeInformation.of(HoodieRecord.class),
              new BootstrapOperator<>(conf))
          .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(defaultParallelism))
          .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
    }

    return dataStream1;
  }

  public static DataStream<HoodieRecord> batchBootstrap(Configuration conf, RowType rowType, int defaultParallelism, DataStream<RowData> dataStream) {
    // shuffle and sort by partition keys
    final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
    if (partitionFields.length > 0) {
      RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
      // shuffle by partition keys
      dataStream = dataStream
          .keyBy(rowDataKeyGen::getPartitionPath);
    }

    return rowDataToHoodieRecord(conf, rowType, dataStream)
        .transform(
            "batch_index_bootstrap",
            TypeInformation.of(HoodieRecord.class),
            new BatchBootstrapOperator<>(conf))
        .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(defaultParallelism))
        .uid("uid_batch_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
  }

  public static DataStream<HoodieRecord> rowDataToHoodieRecord(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    return dataStream.map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class));
  }

  public static DataStream<Object> hoodieStreamWrite(Configuration conf, int defaultParallelism, DataStream<HoodieRecord> dataStream) {
    StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);
    return dataStream
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        .keyBy(HoodieRecord::getRecordKey)
        .transform(
            "bucket_assigner",
            TypeInformation.of(HoodieRecord.class),
            new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
        .uid("uid_bucket_assigner_" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getOptional(FlinkOptions.BUCKET_ASSIGN_TASKS).orElse(defaultParallelism))
        // shuffle by fileId(bucket id)
        .keyBy(record -> record.getCurrentLocation().getFileId())
        .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
        .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
  }

  public static DataStreamSink<CompactionCommitEvent> compact(Configuration conf, DataStream<Object> dataStream) {
    return dataStream.transform("compact_plan_generate",
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
  }

  public static DataStreamSink<Object> clean(Configuration conf, DataStream<Object> dataStream) {
    return dataStream.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits");
  }
}
