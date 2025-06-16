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

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.client.model.HoodieFlinkInternalRowTypeInfo;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperator;
import org.apache.hudi.sink.append.AppendWriteOperator;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.bootstrap.batch.BatchBootstrapOperator;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bucket.BucketStreamWriteOperator;
import org.apache.hudi.sink.bucket.ConsistentBucketAssignFunction;
import org.apache.hudi.sink.bulk.BulkInsertWriteOperator;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanEvent;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketIndexPartitioner;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Utilities to generate all kinds of sub-pipelines.
 */
public class Pipelines {

  // The counter of operators, avoiding duplicate uids caused by the same operator
  private static final ConcurrentHashMap<String,Integer> OPERATOR_COUNTERS = new ConcurrentHashMap<>();

  /**
   * Bulk insert the input dataset at once.
   *
   * <p>By default, the input dataset would shuffle by the partition path first then
   * sort by the partition path before passing around to the write function.
   * The whole pipeline looks like the following:
   *
   * <pre>
   *      | input1 | ===\     /=== |sorter| === | task1 | (p1, p2)
   *                   shuffle
   *      | input2 | ===/     \=== |sorter| === | task2 | (p3, p4)
   *
   *      Note: Both input1 and input2's dataset come from partitions: p1, p2, p3, p4
   * </pre>
   *
   * <p>The write task switches to new file handle each time it receives a record
   * from the different partition path, the shuffle and sort would reduce small files.
   *
   * <p>The bulk insert should be run in batch execution mode.
   *
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param dataStream The input data stream
   * @return the bulk insert data stream sink
   */
  public static DataStreamSink<Object> bulkInsert(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    // we need same parallelism for all operators,
    // which is equal to write tasks number, to avoid shuffles
    final int PARALLELISM_VALUE = conf.getInteger(FlinkOptions.WRITE_TASKS);
    final boolean isBucketIndexType = OptionsResolver.isBucketIndexType(conf);

    if (isBucketIndexType) {
      // TODO support bulk insert for consistent bucket index
      if (OptionsResolver.isConsistentHashingBucketIndexType(conf)) {
        throw new HoodieException(
            "Consistent hashing bucket index does not work with bulk insert using FLINK engine. Use simple bucket index or Spark engine.");
      }
      String indexKeys = OptionsResolver.getIndexKeyField(conf);
      BucketIndexPartitioner<HoodieKey> partitioner = new BucketIndexPartitioner<>(conf, indexKeys);
      RowDataKeyGen keyGen = RowDataKeyGen.instance(conf, rowType);
      RowType rowTypeWithFileId = BucketBulkInsertWriterHelper.rowTypeWithFileId(rowType);
      InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowTypeWithFileId);
      boolean needFixedFileIdSuffix = OptionsResolver.isNonBlockingConcurrencyControl(conf);

      Map<String, String> bucketIdToFileId = new HashMap<>();
      dataStream = dataStream.partitionCustom(partitioner, keyGen::getHoodieKey)
          .map(record -> BucketBulkInsertWriterHelper.rowWithFileId(bucketIdToFileId, keyGen, record, indexKeys, conf, needFixedFileIdSuffix), typeInfo)
          .setParallelism(PARALLELISM_VALUE);
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
        SortOperatorGen sortOperatorGen = BucketBulkInsertWriterHelper.getFileIdSorterGen(rowTypeWithFileId);
        dataStream = dataStream.transform("file_sorter", typeInfo, sortOperatorGen.createSortOperator(conf))
            .setParallelism(PARALLELISM_VALUE);
        ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
            conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
      }
    } else if (!FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.PARTITION_PATH_FIELD)) {
      // if table is not partitioned then we don't need any shuffles,
      // and could add main write operator only
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT)) {
        // shuffle by partition keys
        // use #partitionCustom instead of #keyBy to avoid duplicate sort operations,
        // see BatchExecutionUtils#applyBatchExecutionSettings for details.
        Partitioner<String> partitioner = (key, channels) -> KeyGroupRangeAssignment.assignKeyToParallelOperator(key,
            KeyGroupRangeAssignment.computeDefaultMaxParallelism(PARALLELISM_VALUE), channels);
        RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
        dataStream = dataStream.partitionCustom(partitioner, rowDataKeyGen::getPartitionPath);
      }

      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
        final boolean isNeededSortInput = conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT_BY_RECORD_KEY);
        final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
        final String[] recordKeyFields = conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",");

        // if sort input by record key is needed then add record keys to partition keys
        String[] sortFields = isNeededSortInput
            ? Stream.concat(Arrays.stream(partitionFields), Arrays.stream(recordKeyFields)).toArray(String[]::new)
            : partitionFields;
        SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortFields);
        dataStream = dataStream
            .transform(isNeededSortInput ? "sorter:(partition_key, record_key)" : "sorter:(partition_key)",
                InternalTypeInfo.of(rowType), sortOperatorGen.createSortOperator(conf))
            .setParallelism(PARALLELISM_VALUE);
        ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
            conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
      }
    }

    // main write operator with following dummy sink in the end
    return dataStream
        .transform(opName(isBucketIndexType ? "bucket_bulk_insert" : "hoodie_bulk_insert_write", conf),
            TypeInformation.of(Object.class), BulkInsertWriteOperator.getFactory(conf, rowType))
        .uid(opUID("bucket_bulk_insert", conf))
        .setParallelism(PARALLELISM_VALUE)
        .addSink(DummySink.INSTANCE)
        .name("dummy");
  }

  /**
   * Insert the dataset with append mode(no upsert or deduplication).
   *
   * <p>The input dataset would be rebalanced among the write tasks:
   *
   * <pre>
   *      | input1 | ===\     /=== | task1 | (p1, p2, p3, p4)
   *                   shuffle
   *      | input2 | ===/     \=== | task2 | (p1, p2, p3, p4)
   *
   *      Note: Both input1 and input2's dataset come from partitions: p1, p2, p3, p4
   * </pre>
   *
   * <p>The write task switches to new file handle each time it receives a record
   * from the different partition path, so there may be many small files.
   *
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param dataStream The input data stream
   * @return the appending data stream sink
   */
  public static DataStream<Object> append(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      throw new HoodieNotSupportedException("Bucket index supports only upsert operation. Please, use upsert operation or switch to another index type.");
    }

    WriteOperatorFactory<RowData> operatorFactory = AppendWriteOperator.getFactory(conf, rowType);

    return dataStream
        .transform(opName("hoodie_append_write", conf), TypeInformation.of(Object.class), operatorFactory)
        .uid(opUID("hoodie_stream_write", conf))
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
  }

  /**
   * Constructs bootstrap pipeline as streaming.
   * The bootstrap operator loads the existing data index (primary key to file id mapping),
   * then sends the indexing data set to subsequent operator(usually the bucket assign operator).
   */
  public static DataStream<HoodieFlinkInternalRow> bootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream) {
    return bootstrap(conf, rowType, dataStream, false, false);
  }

  /**
   * Constructs bootstrap pipeline.
   * The bootstrap operator loads the existing data index (primary key to file id mapping),
   * then send the indexing data set to subsequent operator(usually the bucket assign operator).
   *
   * @param conf       The configuration
   * @param rowType    The row type
   * @param dataStream The data stream
   * @param bounded    Whether the source is bounded
   * @param overwrite  Whether it is insert overwrite
   */
  public static DataStream<HoodieFlinkInternalRow> bootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      boolean bounded,
      boolean overwrite) {
    final boolean globalIndex = conf.getBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED);
    if (overwrite || OptionsResolver.isBucketIndexType(conf)) {
      return rowDataToHoodieRecord(conf, rowType, dataStream);
    } else if (bounded && !globalIndex && OptionsResolver.isPartitionedTable(conf)) {
      return boundedBootstrap(conf, rowType, dataStream);
    } else {
      return streamBootstrap(conf, rowType, dataStream, bounded);
    }
  }

  private static DataStream<HoodieFlinkInternalRow> streamBootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      boolean bounded) {
    DataStream<HoodieFlinkInternalRow> dataStream1 = rowDataToHoodieRecord(conf, rowType, dataStream);

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED) || bounded) {
      dataStream1 = dataStream1
          .transform(
              "index_bootstrap",
              new HoodieFlinkInternalRowTypeInfo(rowType),
              new BootstrapOperator(conf))
          .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(dataStream1.getParallelism()))
          .uid(opUID("index_bootstrap", conf));
    }

    return dataStream1;
  }

  /**
   * Constructs bootstrap pipeline for batch execution mode.
   * The indexing data set is loaded before the actual data write
   * in order to support batch UPSERT.
   */
  private static DataStream<HoodieFlinkInternalRow> boundedBootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream) {
    final RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
    // shuffle by partition keys
    dataStream = dataStream
        .keyBy(rowDataKeyGen::getPartitionPath);

    return rowDataToHoodieRecord(conf, rowType, dataStream)
        .transform(
            "batch_index_bootstrap",
            new HoodieFlinkInternalRowTypeInfo(rowType),
            new BatchBootstrapOperator(conf))
        .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(dataStream.getParallelism()))
        .uid(opUID("batch_index_bootstrap", conf));
  }

  /**
   * Transforms the row data to hoodie records.
   */
  public static DataStream<HoodieFlinkInternalRow> rowDataToHoodieRecord(Configuration conf,
                                                                         RowType rowType,
                                                                         DataStream<RowData> dataStream) {
    return dataStream
        .map(RowDataToHoodieFunctions.create(rowType, conf), new HoodieFlinkInternalRowTypeInfo(rowType))
        .setParallelism(dataStream.getParallelism())
        .name("row_data_to_hoodie_record");
  }

  /**
   * The streaming write pipeline.
   *
   * <p>The input dataset shuffles by the primary key first then
   * shuffles by the file group ID before passing around to the write function.
   * The whole pipeline looks like the following:
   *
   * <pre>
   *      | input1 | ===\     /=== | bucket assigner | ===\     /=== | task1 |
   *                   shuffle(by PK)                    shuffle(by bucket ID)
   *      | input2 | ===/     \=== | bucket assigner | ===/     \=== | task2 |
   *
   *      Note: a file group must be handled by one write task to avoid write conflict.
   * </pre>
   *
   * <p>The bucket assigner assigns the inputs to suitable file groups, the write task caches
   * and flushes the data set to disk.
   *
   * @param conf       The configuration
   * @param dataStream The input data stream
   * @return the stream write data stream pipeline
   */
  public static DataStream<Object> hoodieStreamWrite(Configuration conf,
                                                     RowType rowType,
                                                     DataStream<HoodieFlinkInternalRow> dataStream) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      HoodieIndex.BucketIndexEngineType bucketIndexEngineType = OptionsResolver.getBucketEngineType(conf);
      switch (bucketIndexEngineType) {
        case SIMPLE:
          String indexKeyFields = OptionsResolver.getIndexKeyField(conf);
          // [HUDI-9036] BucketIndexPartitioner is also used in bulk insert mode,
          // keep use of HoodieKey here in partitionCustom for now
          BucketIndexPartitioner<HoodieKey> partitioner = new BucketIndexPartitioner<>(conf, indexKeyFields);
          return dataStream
              .partitionCustom(
                  partitioner,
                  record -> new HoodieKey(record.getRecordKey(), record.getPartitionPath()))
              .transform(
                  opName("bucket_write", conf),
                  TypeInformation.of(Object.class),
                  BucketStreamWriteOperator.getFactory(conf, rowType))
              .uid(opUID("bucket_write", conf))
              .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
        case CONSISTENT_HASHING:
          if (OptionsResolver.isInsertOverwrite(conf)) {
            // TODO support insert overwrite for consistent bucket index
            throw new HoodieException("Consistent hashing bucket index does not work with insert overwrite using FLINK engine. Use simple bucket index or Spark engine.");
          }
          return dataStream
              .transform(
                  opName("consistent_bucket_assigner", conf),
                  new HoodieFlinkInternalRowTypeInfo(rowType),
                  new ProcessOperator<>(new ConsistentBucketAssignFunction(conf)))
              .uid(opUID("consistent_bucket_assigner", conf))
              .setParallelism(conf.getInteger(FlinkOptions.BUCKET_ASSIGN_TASKS))
              .keyBy(HoodieFlinkInternalRow::getFileId)
              .transform(
                  opName("consistent_bucket_write", conf),
                  TypeInformation.of(Object.class),
                  BucketStreamWriteOperator.getFactory(conf, rowType))
              .uid(opUID("consistent_bucket_write", conf))
              .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
        default:
          throw new HoodieNotSupportedException("Unknown bucket index engine type: " + bucketIndexEngineType);
      }
    } else {
      return dataStream
          // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
          .keyBy(HoodieFlinkInternalRow::getRecordKey)
          .transform(
              "bucket_assigner",
              new HoodieFlinkInternalRowTypeInfo(rowType),
              new KeyedProcessOperator<>(new BucketAssignFunction(conf)))
          .uid(opUID("bucket_assigner", conf))
          .setParallelism(conf.getInteger(FlinkOptions.BUCKET_ASSIGN_TASKS))
          // shuffle by fileId(bucket id)
          .keyBy(HoodieFlinkInternalRow::getFileId)
          .transform(
              opName("stream_write", conf),
              TypeInformation.of(Object.class),
              StreamWriteOperator.getFactory(conf, rowType))
          .uid(opUID("stream_write", conf))
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
    }
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
   * @param conf       The configuration
   * @param dataStream The input data stream
   * @return the compaction pipeline
   */
  public static DataStreamSink<CompactionCommitEvent> compact(Configuration conf, DataStream<Object> dataStream) {
    DataStreamSink<CompactionCommitEvent> compactionCommitEventDataStream = dataStream.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1)
        .partitionCustom(new IndexPartitioner(), CompactionPlanEvent::getIndex)
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new CompactOperator(conf))
        .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
        .addSink(new CompactionCommitSink(conf))
        .name("compact_commit")
        .setParallelism(1); // compaction commit should be singleton
    compactionCommitEventDataStream.getTransformation().setMaxParallelism(1);
    return compactionCommitEventDataStream;
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
   * @param conf       The configuration
   * @param rowType    The input row type
   * @param dataStream The input data stream
   * @return the clustering pipeline
   */
  public static DataStreamSink<ClusteringCommitEvent> cluster(Configuration conf, RowType rowType, DataStream<Object> dataStream) {
    DataStream<ClusteringCommitEvent> clusteringStream = dataStream.transform("cluster_plan_generate",
            TypeInformation.of(ClusteringPlanEvent.class),
            new ClusteringPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1) // plan generate must be singleton
        .partitionCustom(new IndexPartitioner(), ClusteringPlanEvent::getIndex)
        .transform("clustering_task",
            TypeInformation.of(ClusteringCommitEvent.class),
            new ClusteringOperator(conf, rowType))
        .setParallelism(conf.getInteger(FlinkOptions.CLUSTERING_TASKS));
    if (OptionsResolver.sortClusteringEnabled(conf)) {
      ExecNodeUtil.setManagedMemoryWeight(clusteringStream.getTransformation(),
          conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
    }
    DataStreamSink<ClusteringCommitEvent> clusteringCommitEventDataStream = clusteringStream.addSink(new ClusteringCommitSink(conf))
        .name("clustering_commit")
        .setParallelism(1); // clustering commit should be singleton
    clusteringCommitEventDataStream.getTransformation().setMaxParallelism(1);
    return clusteringCommitEventDataStream;
  }

  public static DataStreamSink<Object> clean(Configuration conf, DataStream<Object> dataStream) {
    DataStreamSink<Object> cleanCommitDataStream = dataStream.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits");
    cleanCommitDataStream.getTransformation().setMaxParallelism(1);
    return cleanCommitDataStream;
  }

  public static DataStreamSink<Object> dummySink(DataStream<Object> dataStream) {
    return dataStream.addSink(Pipelines.DummySink.INSTANCE)
        // keeps the same parallelism to upstream operators to enable partial failover.
        .name("dummy");
  }

  public static String opName(String operatorN, Configuration conf) {
    return operatorN + ": " + getTablePath(conf);
  }

  public static String opUID(String operatorN, Configuration conf) {
    Integer operatorCount = OPERATOR_COUNTERS.merge(operatorN, 1, (oldValue, value) -> oldValue + value);
    return "uid_" + operatorN + (operatorCount == 1 ? "" : "_" + (operatorCount - 1)) + "_" + getTablePath(conf);
  }

  public static String getTablePath(Configuration conf) {
    String databaseName = conf.getString(FlinkOptions.DATABASE_NAME);
    return StringUtils.isNullOrEmpty(databaseName) ? conf.getString(FlinkOptions.TABLE_NAME)
        : databaseName + "." + conf.getString(FlinkOptions.TABLE_NAME);
  }

  /**
   * Dummy sink that does nothing.
   */
  public static class DummySink implements SinkFunction<Object> {
    private static final long serialVersionUID = 1L;
    public static DummySink INSTANCE = new DummySink();
  }

  public static class IndexPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int numPartitions) {
      return key % numPartitions;
    }
  }
}
