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

package org.apache.hudi.sink.v2.utils;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanEvent;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.sink.v2.CleanFunctionV2;
import org.apache.hudi.sink.v2.clustering.ClusteringCommitSinkV2;
import org.apache.hudi.sink.v2.compact.CompactionCommitSinkV2;
import org.apache.hudi.sink.v2.HoodieSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.hudi.sink.utils.Pipelines.opUID;

/**
 * Utilities to generate pipelines for hudi sink V2.
 */
public class PipelinesV2 {

  private static final String SINK_V2_NAME = "sink_v2";

  /**
   * Construct a write pipeline based on {@link HoodieSink}, which is implemented
   * based on Flink Sink V2 API.
   *
   * @param dataStream The input data stream
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param overwrite  Whether it is insert overwrite
   * @param isBounded  Whether the source is bounded
   *
   * @return A write pipeline for Sink V2
   */
  public static DataStreamSink<RowData> sink(
      DataStream<RowData> dataStream,
      Configuration conf,
      RowType rowType,
      boolean overwrite,
      boolean isBounded) {
    HoodieSink hoodieSink = new HoodieSink(conf, rowType, overwrite, isBounded);
    return dataStream.sinkTo(hoodieSink)
        .setParallelism(getParallelismForSinkV2(conf))
        .uid(opUID(SINK_V2_NAME, conf))
        .name(SINK_V2_NAME);
  }

  /**
   * Construct a write pipeline for Sink V2, which is almost same with the topology for Sink V1,
   * the ony difference is the final operator of V2 pipeline is a {@code ProcessFunction} instead
   * of {@code SinkFunction} like Sink V1.
   *
   * @param dataStream The input data stream
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param overwrite  Whether it is insert overwrite
   * @param isBounded  Whether the source is bounded
   *
   * @return A write pipeline for Sink V2
   */
  public static DataStream<RowData> composePipeline(
      DataStream<RowData> dataStream,
      Configuration conf,
      RowType rowType,
      boolean overwrite,
      boolean isBounded) {
    // bulk_insert mode
    if (OptionsResolver.isBulkInsertOperation(conf)) {
      if (!isBounded) {
        throw new HoodieException(
            "The bulk insert should be run in batch execution mode.");
      }
      return Pipelines.bulkInsert(conf, rowType, dataStream);
    }

    // Append mode
    if (OptionsResolver.isAppendMode(conf)) {
      // close compaction for append mode
      conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
      DataStream<RowData> pipeline = Pipelines.append(conf, rowType, dataStream);
      if (OptionsResolver.needsAsyncClustering(conf)) {
        return clusterV2(conf, rowType, pipeline);
      } else if (OptionsResolver.isLazyFailedWritesCleanPolicy(conf)) {
        // add clean function to rollback failed writes for lazy failed writes cleaning policy
        return cleanV2(conf, pipeline);
      } else {
        return pipeline;
      }
    }

    // process dataStream and write corresponding files
    DataStream<RowData> pipeline;
    final DataStream<HoodieFlinkInternalRow> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, dataStream, isBounded, overwrite);
    pipeline = Pipelines.hoodieStreamWrite(conf, rowType, hoodieRecordDataStream);
    // compaction
    if (OptionsResolver.needsAsyncCompaction(conf)) {
      // use synchronous compaction for bounded source.
      if (isBounded) {
        conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
      }
      return compactV2(conf, pipeline);
    } else {
      return cleanV2(conf, pipeline);
    }
  }

  /**
   * Get the parallelism for {@code SinkWriter} in Sink V2, the value is same with the parallelism of the last
   * operator in the write pipeline, so that the sink writer can be chained with write pipeline, which is
   * important for the partial task failover for some writing modes, e.g., append writing.
   *
   * @param conf The configuration
   *
   * @return The parallelism for the {@code SinkWriter} in Sink V2
   */
  private static int getParallelismForSinkV2(Configuration conf) {
    if (OptionsResolver.isBulkInsertOperation(conf)) {
      return conf.get(FlinkOptions.WRITE_TASKS);
    } else if (OptionsResolver.isAppendMode(conf)) {
      return OptionsResolver.needsAsyncClustering(conf) || OptionsResolver.isLazyFailedWritesCleanPolicy(conf)
          ? 1 : conf.get(FlinkOptions.WRITE_TASKS);
    } else {
      return 1;
    }
  }

  /**
   * The cleaning task pipeline.
   *
   * <p>It starts a cleaning task on new checkpoints, there is only one cleaning task at a time,
   * a new task can not be scheduled until the last task finished(fails or normally succeed).
   * The cleaning task never expects to throw but only log.
   *
   * <p>The difference with {@code Pipelines#clean} is {@code cleanV2} uses {@code CleanFunctionV2}
   * which is a {@code ProcessFunction} instead of {@code SinkFunction}, and the return value is a
   * {@code DataStream} instead of {@code DataStreamSink}.
   *
   * @param conf       The configuration
   * @param dataStream The input data stream
   *
   * @return The cleaning pipeline
   */
  public static DataStream<RowData> cleanV2(Configuration conf, DataStream<RowData> dataStream) {
    return dataStream.transform(
            "clean_commits",
            TypeInformation.of(RowData.class),
            new ProcessOperator<>(new CleanFunctionV2<>(conf)))
        .setParallelism(1)
        .setMaxParallelism(1);
  }

  /**
   * The clustering tasks pipeline.
   *
   * <p>The clustering plan operator monitors the new clustering plan on the timeline
   * then distributes the sub-plans to the clustering tasks. The clustering task then
   * handle over the metadata to commit task for clustering transaction commit.
   * The whole pipeline looks like the following:
   *
   * <pre>
   *                                     /=== | task1 | ===\
   *      | plan generation | ===> hash                      | commit |
   *                                     \=== | task2 | ===/
   *
   *      Note: both the clustering plan generation task and commission task are singleton.
   * </pre>
   *
   * <p>The difference with {@code Pipelines#cluster} is {@code clusterV2} uses {@code ClusteringCommitSinkV2}
   * which is a {@code ProcessFunction}, and the return value is a {@code DataStream} instead of {@code DataStreamSink}.
   *
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param dataStream The input data stream
   *
   * @return the clustering pipeline
   */
  public static DataStream<RowData> clusterV2(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    DataStream<ClusteringCommitEvent> clusteringStream = dataStream.transform("cluster_plan_generate",
            TypeInformation.of(ClusteringPlanEvent.class),
            new ClusteringPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1) // plan generate must be singleton
        .partitionCustom(new Pipelines.IndexPartitioner(), ClusteringPlanEvent::getIndex)
        .transform("clustering_task",
            TypeInformation.of(ClusteringCommitEvent.class),
            new ClusteringOperator(conf, rowType))
        .setParallelism(conf.get(FlinkOptions.CLUSTERING_TASKS));
    if (OptionsResolver.sortClusteringEnabled(conf)) {
      ExecNodeUtil.setManagedMemoryWeight(clusteringStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
    }
    return clusteringStream.transform(
            "clustering_commit",
            TypeInformation.of(RowData.class),
            new ProcessOperator<>(new ClusteringCommitSinkV2(conf)))
        .setParallelism(1)
        .setMaxParallelism(1); // clustering commit should be singleton
  }

  /**
   * The compaction tasks pipeline.
   *
   * <p>The compaction plan operator monitors the new compaction plan on the timeline
   * then distributes the sub-plans to the compaction tasks. The compaction task then
   * handle over the metadata to commit task for compaction transaction commit.
   * The whole pipeline looks like the following:
   *
   * <pre>
   *                                     /=== | task1 | ===\
   *      | plan generation | ===> hash                      | commit |
   *                                     \=== | task2 | ===/
   *
   *      Note: both the compaction plan generation task and commission task are singleton.
   * </pre>
   *
   * <p>The difference with {@code Pipelines#compact} is {@code compactV2} uses {@code CompactionCommitSinkV2}
   * which is a {@code ProcessFunction}, and the return value is a {@code DataStream} instead of {@code DataStreamSink}.
   *
   * @param conf       The configuration
   * @param dataStream The input data stream
   *
   * @return the compaction pipeline
   */
  public static DataStream<RowData> compactV2(Configuration conf, DataStream<RowData> dataStream) {
    // plan generate must be singleton
    return dataStream.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1)
        .partitionCustom(new Pipelines.IndexPartitioner(), CompactionPlanEvent::getIndex)
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new CompactOperator(conf))
        .setParallelism(conf.get(FlinkOptions.COMPACTION_TASKS))
        .transform(
            "compact_commit",
            TypeInformation.of(RowData.class),
            new ProcessOperator<>(new CompactionCommitSinkV2(conf)))
        .setParallelism(1)
        .setMaxParallelism(1);
  }
}
