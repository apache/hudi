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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hudi.adapter.PartitionTimeExtractorAdapter;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperator;
import org.apache.hudi.sink.append.AppendWriteOperator;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.bootstrap.batch.BatchBootstrapOperator;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bucket.BucketStreamWriteOperator;
import org.apache.hudi.sink.bulk.BulkInsertWriteOperator;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanEvent;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketIndexPartitioner;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Utilities to generate all kinds of sub-pipelines.
 */
public class Pipelines {

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
    WriteOperatorFactory<RowData> operatorFactory = BulkInsertWriteOperator.getFactory(conf, rowType);
    if (OptionsResolver.isBucketIndexType(conf)) {
      String indexKeys = conf.getString(FlinkOptions.INDEX_KEY_FIELD);
      int numBuckets = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);

      BucketIndexPartitioner<HoodieKey> partitioner = new BucketIndexPartitioner<>(numBuckets, indexKeys);
      RowDataKeyGen keyGen = RowDataKeyGen.instance(conf, rowType);
      RowType rowTypeWithFileId = BucketBulkInsertWriterHelper.rowTypeWithFileId(rowType);
      InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowTypeWithFileId);

      Map<String, String> bucketIdToFileId = new HashMap<>();
      dataStream = dataStream.partitionCustom(partitioner, keyGen::getHoodieKey)
          .map(record -> BucketBulkInsertWriterHelper.rowWithFileId(bucketIdToFileId, keyGen, record, indexKeys, numBuckets), typeInfo)
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS)); // same parallelism as write task to avoid shuffle
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
        SortOperatorGen sortOperatorGen = BucketBulkInsertWriterHelper.getFileIdSorterGen(rowTypeWithFileId);
        dataStream = dataStream.transform("file_sorter", typeInfo, sortOperatorGen.createSortOperator())
            .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS)); // same parallelism as write task to avoid shuffle
        ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
            conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
      }
      return dataStream
          .transform(opName("bucket_bulk_insert", conf), TypeInformation.of(Object.class), operatorFactory)
          .uid(opUID("bucket_bulk_insert", conf))
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS))
          .addSink(DummySink.INSTANCE)
          .name("dummy");
    }

    final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
    if (partitionFields.length > 0) {
      RowDataKeyGen rowDataKeyGen = RowDataKeyGen.instance(conf, rowType);
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT)) {

        // shuffle by partition keys
        // use #partitionCustom instead of #keyBy to avoid duplicate sort operations,
        // see BatchExecutionUtils#applyBatchExecutionSettings for details.
        Partitioner<String> partitioner = (key, channels) -> KeyGroupRangeAssignment.assignKeyToParallelOperator(key,
            KeyGroupRangeAssignment.computeDefaultMaxParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS)), channels);
        dataStream = dataStream.partitionCustom(partitioner, rowDataKeyGen::getPartitionPath);
      }
      if (conf.getBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
        SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, partitionFields);
        // sort by partition keys
        dataStream = dataStream
            .transform("partition_key_sorter",
                InternalTypeInfo.of(rowType),
                sortOperatorGen.createSortOperator())
            .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
        ExecNodeUtil.setManagedMemoryWeight(dataStream.getTransformation(),
            conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
      }
    }
    return dataStream
        .transform(opName("hoodie_bulk_insert_write", conf),
            TypeInformation.of(Object.class),
            operatorFactory)
        // follow the parallelism of upstream operators to avoid shuffle
        .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS))
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
   * @param bounded    Whether the input stream is bounded
   * @return the appending data stream sink
   */
  public static DataStream<Object> append(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      boolean bounded) {
    if (!bounded) {
      // In principle, the config should be immutable, but the boundedness
      // is only visible when creating the sink pipeline.
      conf.setBoolean(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, false);
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
  public static DataStream<HoodieRecord> bootstrap(
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
  public static DataStream<HoodieRecord> bootstrap(
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

  private static DataStream<HoodieRecord> streamBootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      boolean bounded) {
    DataStream<HoodieRecord> dataStream1 = rowDataToHoodieRecord(conf, rowType, dataStream);

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED) || bounded) {
      dataStream1 = dataStream1
          .transform(
              "index_bootstrap",
              TypeInformation.of(HoodieRecord.class),
              new BootstrapOperator<>(conf))
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
  private static DataStream<HoodieRecord> boundedBootstrap(
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
            TypeInformation.of(HoodieRecord.class),
            new BatchBootstrapOperator<>(conf))
        .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(dataStream.getParallelism()))
        .uid(opUID("batch_index_bootstrap", conf));
  }

  /**
   * Transforms the row data to hoodie records.
   */
  public static DataStream<HoodieRecord> rowDataToHoodieRecord(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    return dataStream.map(RowDataToHoodieFunctions.create(rowType, conf), TypeInformation.of(HoodieRecord.class))
        .setParallelism(dataStream.getParallelism()).name("row_data_to_hoodie_record");
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
  public static DataStream<Object> hoodieStreamWrite(Configuration conf, DataStream<HoodieRecord> dataStream) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      WriteOperatorFactory<HoodieRecord> operatorFactory = BucketStreamWriteOperator.getFactory(conf);
      int bucketNum = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
      String indexKeyFields = conf.getString(FlinkOptions.INDEX_KEY_FIELD);
      BucketIndexPartitioner<HoodieKey> partitioner = new BucketIndexPartitioner<>(bucketNum, indexKeyFields);
      return dataStream.partitionCustom(partitioner, HoodieRecord::getKey)
          .transform(opName("bucket_write", conf), TypeInformation.of(Object.class), operatorFactory)
          .uid(opUID("bucket_write", conf))
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
    } else {
      WriteOperatorFactory<HoodieRecord> operatorFactory = StreamWriteOperator.getFactory(conf);
      return dataStream
          // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
          .keyBy(HoodieRecord::getRecordKey)
          .transform(
              "bucket_assigner",
              TypeInformation.of(HoodieRecord.class),
              new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
          .uid(opUID("bucket_assigner", conf))
          .setParallelism(conf.getInteger(FlinkOptions.BUCKET_ASSIGN_TASKS))
          // shuffle by fileId(bucket id)
          .keyBy(record -> record.getCurrentLocation().getFileId())
          .transform(opName("stream_write", conf), TypeInformation.of(Object.class), operatorFactory)
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
    return dataStream.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        // make the distribution strategy deterministic to avoid concurrent modifications
        // on the same bucket files
        .keyBy(plan -> plan.getOperation().getFileGroupId().getFileId())
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new ProcessOperator<>(new CompactFunction(conf)))
        .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
        .addSink(new CompactionCommitSink(conf))
        .name("compact_commit")
        .setParallelism(1); // compaction commit should be singleton
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
        .keyBy(plan ->
            // make the distribution strategy deterministic to avoid concurrent modifications
            // on the same bucket files
            plan.getClusteringGroupInfo().getOperations()
                .stream().map(ClusteringOperation::getFileId).collect(Collectors.joining()))
        .transform("clustering_task",
            TypeInformation.of(ClusteringCommitEvent.class),
            new ClusteringOperator(conf, rowType))
        .setParallelism(conf.getInteger(FlinkOptions.CLUSTERING_TASKS));
    if (OptionsResolver.sortClusteringEnabled(conf)) {
      ExecNodeUtil.setManagedMemoryWeight(clusteringStream.getTransformation(),
          conf.getInteger(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
    }
    return clusteringStream.addSink(new ClusteringCommitSink(conf))
        .name("clustering_commit")
        .setParallelism(1); // compaction commit should be singleton
  }

  public static DataStreamSink<Object> clean(Configuration conf, DataStream<Object> dataStream) {
    return dataStream.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits");
  }

  public static DataStreamSink<Object> dummySink(DataStream<Object> dataStream) {
    return dataStream.addSink(Pipelines.DummySink.INSTANCE)
        .setParallelism(1)
        .name("dummy");
  }

  public static String opName(String operatorN, Configuration conf) {
    return operatorN + ": " + conf.getString(FlinkOptions.TABLE_NAME);
  }

  public static String opUID(String operatorN, Configuration conf) {
    return "uid_" + operatorN + "_" + conf.getString(FlinkOptions.TABLE_NAME);
  }

  /**
   * Dummy sink that does nothing.
   */
  public static class DummySink implements SinkFunction<Object> {
    private static final long serialVersionUID = 1L;
    public static DummySink INSTANCE = new DummySink();
  }

  public static DataStreamSink<Object> successFileSink(DataStream<Object> dataStream, Configuration conf) {
    return dataStream.addSink(new PartitionSuccessFileWriteSink(conf))
            .setParallelism(1)
            .name("success_file_sink");
  }

  /**
   * Sink that write a success file to partition path when it write finished.
   **/
  public static class PartitionSuccessFileWriteSink extends RichSinkFunction<Object> implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSuccessFileWriteSink.class);
    // Success file name.
    private static final String SUCCESS_FILE_NAME = "_SUCCESS";
    // The name of active partitions state.
    private static final String ACTIVE_PARTITION_STATE_NAME = "active-partition-state";
    // The name of finished partition state.
    private static final String FINISHED_PARTITION_STATE_NAME = "finished-partition-state";
    // The global configuration of flink job.
    private Configuration conf;
    // The configured file system handle.
    private FileSystem fileSystem;
    // The extractor for partition time.
    private PartitionTimeExtractorAdapter partitionTimeExtractor;
    // The extractor for extract partition value from partition path.
    private PartitionValueExtractor partitionValueExtractor;
    // The table base path.
    private String tablePath;
    // The partition keys.
    private List<String> partitionKeys;
    // The partitions on writing currently.
    private final Set<String> activePartitions;
    // The partitions write finished.
    private final Set<String> finishedPartitions;
    // The operator state to store active partitions.
    private ListState<String> activePartitionsState;
    // The operator state to store finished partitions.
    private ListState<String> finishedPartitionsState;
    // The configured time delay to write success file.
    private Duration partitionSuccessFileDelay;

    PartitionSuccessFileWriteSink(Configuration conf) {
      this.conf = conf;
      activePartitions = new TreeSet<>();
      finishedPartitions = new TreeSet<>();
    }

    @Override
    public void open(Configuration config) throws Exception {
      tablePath = conf.get(FlinkOptions.PATH);
      fileSystem = FileSystem.get(URI.create(tablePath));
      String[] partitionFields = conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",");
      partitionKeys = Arrays.asList(partitionFields);
      partitionSuccessFileDelay = conf.get(FlinkOptions.PARTITION_WRITE_SUCCESS_FILE_DELAY);
      String partitionValueExtractorClzName = conf.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME);
      String partitionTimestampExtractPattern = conf.get(FlinkOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
      String partitionTimestampFormatPattern = conf.get(FlinkOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER);
      try {
        Class<?> partitionValueExtractorClz = Class.forName(partitionValueExtractorClzName);
        partitionValueExtractor = (PartitionValueExtractor) partitionValueExtractorClz.newInstance();
      } catch (ClassNotFoundException e) {
        LOG.error("class not found for: {}", partitionValueExtractorClzName, e);
        throw e;
      }
      partitionTimeExtractor = new PartitionTimeExtractorAdapter(partitionTimestampExtractPattern, partitionTimestampFormatPattern);
    }

    // Extract the partition time value from partition path, and convert them to timestamp.
    private Long convertTimestampByPartitionPath(String partitionPath) {
      List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partitionPath);
      LocalDateTime localDateTime = partitionTimeExtractor.extract(partitionKeys, partitionValues);
      return PartitionTimeExtractorAdapter.toMills(localDateTime);
    }

    @Override
    public void invoke(Object value, SinkFunction.Context context) throws Exception {
      String partition = (String) value;
      activePartitions.add(partition);
      long watermark = context.currentWatermark();
      Iterator<String> it = activePartitions.iterator();
      while (it.hasNext()) {
        String partitionPath = it.next();
        // Convert the partition path to timestamp if the table is partitioned by time field, like day, hour
        Long partitionTimestamp = convertTimestampByPartitionPath(partitionPath);
        // If the watermark is greater than the partition timestamp plus the delay time, it represents the
        // minimum timestamp in the streaming data is beyond the partition max timestamp, so add the partition
        // path to the finished partitions set and remove it from active partitions set.
        if (partitionTimestamp + partitionSuccessFileDelay.toMillis() < watermark) {
          finishedPartitions.add(partitionPath);
          it.remove();
        }
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
      // Save the partition path of active & finished partition path to state.
      Preconditions.checkNotNull(activePartitions);
      Preconditions.checkNotNull(finishedPartitions);
      activePartitionsState.update(new ArrayList<>(activePartitions));
      finishedPartitionsState.update(new ArrayList<>(finishedPartitions));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      ListStateDescriptor<String> activePartitionStateDesc =
              new ListStateDescriptor<>(ACTIVE_PARTITION_STATE_NAME, String.class);
      activePartitionsState = context.getOperatorStateStore().getListState(activePartitionStateDesc);
      ListStateDescriptor<String> finishedPartitionStateDesc =
              new ListStateDescriptor<>(FINISHED_PARTITION_STATE_NAME, String.class);
      finishedPartitionsState = context.getOperatorStateStore().getListState(finishedPartitionStateDesc);
      if (context.isRestored()) {
        for (String p : activePartitionsState.get()) {
          activePartitions.add(p);
        }
        for (String p : finishedPartitionsState.get()) {
          finishedPartitions.add(p);
        }
      }
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
      Iterator<String> it = finishedPartitions.iterator();
      //Iterate the finished partitions set, and write success file to the path of partition.
      while (it.hasNext()) {
        String partitionPath = it.next();
        partitionPath = tablePath + "/" + partitionPath;
        fileSystem.create(new Path(partitionPath, SUCCESS_FILE_NAME), FileSystem.WriteMode.OVERWRITE).close();
        LOG.info("Write success file to partition {}", partitionPath);
        it.remove();
      }
    }

    @Override
    public void close() {
      LOG.info("close partition success file write sink.");
    }
  }

}
