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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements AppendStreamTableSink<RowData>, PartitionableTableSink {

  private final Configuration conf;
  private final TableSchema schema;

  public HoodieTableSink(Configuration conf, TableSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
    // Read from kafka source
    RowType rowType = (RowType) this.schema.toRowDataType().notNull().getLogicalType();
    int numWriteTasks = this.conf.getInteger(FlinkOptions.WRITE_TASKS);
    StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);

    DataStream<Object> pipeline = dataStream
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
        .setParallelism(numWriteTasks);
    if (StreamerUtil.needsScheduleCompaction(conf)) {
      return pipeline.transform("compact_plan_generate",
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
          .setParallelism(1); // compaction commit should be singleton
    } else {
      return pipeline.addSink(new DummySinkFunction<>())
          .name("dummy").uid("uid_dummy");
    }
  }

  @Override
  public TableSink<RowData> configure(String[] strings, TypeInformation<?>[] infos) {
    return this;
  }

  @Override
  public TableSchema getTableSchema() {
    return this.schema;
  }

  @Override
  public DataType getConsumedDataType() {
    return this.schema.toRowDataType().bridgedTo(RowData.class);
  }

  @Override
  public void setStaticPartition(Map<String, String> partitions) {
    // no operation
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  // Dummy sink function that does nothing.
  private static class DummySinkFunction<T> implements SinkFunction<T> {}
}
