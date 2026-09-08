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

import org.apache.hudi.adapter.SinkFunctionAdapter;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperator;
import org.apache.hudi.sink.append.AppendWriteFunctions;
import org.apache.hudi.sink.append.AppendWriteOperator;
import org.apache.hudi.sink.bootstrap.AbstractBootstrapOperator;
import org.apache.hudi.sink.bootstrap.BootstrapOperatorFactory;
import org.apache.hudi.sink.bootstrap.batch.BatchBootstrapOperator;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bucket.BucketStreamWriteOperator;
import org.apache.hudi.sink.bucket.ConsistentBucketAssignFunction;
import org.apache.hudi.sink.bucket.LsmBucketBulkInsertWriterHelper;
import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.sink.bulk.BulkInsertWriteOperator;
import org.apache.hudi.sink.bulk.LsmBulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;
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
import org.apache.hudi.sink.partitioner.BucketIndexPartitionerFactory;
import org.apache.hudi.sink.partitioner.DynamicBucketAssignFunction;
import org.apache.hudi.sink.partitioner.DynamicBucketAssignOperator;
import org.apache.hudi.sink.partitioner.GlobalRecordIndexPartitioner;
import org.apache.hudi.sink.partitioner.MiniBatchBucketAssignOperator;
import org.apache.hudi.sink.partitioner.MinibatchBucketAssignFunction;
import org.apache.hudi.sink.partitioner.RecordIndexPartitioner;
import org.apache.hudi.sink.partitioner.index.IndexRowUtils;
import org.apache.hudi.sink.partitioner.index.IndexWriteOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunctions;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
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
  public static DataStream<RowData> bulkInsert(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    if (OptionsResolver.isRecordLevelIndex(conf)) {
      throw new HoodieException(
          "Record level index does not work with bulk insert using FLINK engine.");
    }
    // TODO support bulk insert for consistent bucket index
    if (OptionsResolver.isConsistentHashingBucketIndexType(conf)) {
      throw new HoodieException(
          "Consistent hashing bucket index does not work with bulk insert using FLINK engine. Use simple bucket index or Spark engine.");
    }

    // we need same parallelism for all operators,
    // which is equal to write tasks number, to avoid shuffles
    final int writeTasks = conf.get(FlinkOptions.WRITE_TASKS);
    final boolean isBucketIndexType = OptionsResolver.isBucketIndexType(conf);
    final boolean isLsmTreeStorageLayout = OptionsResolver.isLsmTreeStorageLayout(conf);

    DataStream<RowData> preparedDataStream = isBucketIndexType
        ? bucketShuffleAndSort(
            conf, rowType, dataStream, writeTasks, isLsmTreeStorageLayout)
        : shuffleAndSort(
            conf, rowType, dataStream, writeTasks, isLsmTreeStorageLayout);

    String operatorName =
        isBucketIndexType ? "bucket_bulk_insert" : "hoodie_bulk_insert_write";
    return preparedDataStream
        .transform(opName(operatorName, conf),
            TypeInformation.of(RowData.class), BulkInsertWriteOperator.getFactory(conf, rowType))
        .uid(opUID(operatorName, conf))
        .setParallelism(writeTasks);
  }

  /**
   * Shuffles and sorts the input stream for a bucket bulk insert writer.
   *
   * <p>Records are first routed to the write task that owns the target bucket. For the default
   * layout, the file ID is appended and the stream is optionally sorted by file ID. For the LSM
   * layout, the file ID and record key are appended in the same transform, then the stream is
   * sorted by file ID and record key.
   */
  private static DataStream<RowData> bucketShuffleAndSort(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      int writeTasks,
      boolean isLsmTreeStorageLayout) {
    List<String> indexKeyFieldList = OptionsResolver.getIndexKeyFields(conf);
    // Built once and captured by the per-record map closure (NumBucketsFunction is Serializable),
    // avoiding a per-record rebuild from conf inside BucketBulkInsertWriterHelper.
    NumBucketsFunction numBucketsFunction = new NumBucketsFunction(
        conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
        conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE),
        conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
    Partitioner<HoodieKey> partitioner =
        BucketIndexPartitionerFactory.create(conf, indexKeyFieldList);
    RowDataKeyGen keyGen = RowDataKeyGens.instance(conf, rowType);
    boolean needFixedFileIdSuffix =
        OptionsResolver.isNonBlockingConcurrencyControl(conf);

    Map<String, String> bucketIdToFileId = new HashMap<>();
    DataStream<RowData> routedDataStream =
        dataStream.partitionCustom(partitioner, keyGen::getHoodieKey);

    if (isLsmTreeStorageLayout) {
      RowType sortRowType =
          LsmBucketBulkInsertWriterHelper.rowTypeWithFileIdAndKey(
              rowType, needFixedFileIdSuffix);
      InternalTypeInfo<RowData> sortTypeInfo = InternalTypeInfo.of(sortRowType);
      DataStream<RowData> sortInput = routedDataStream
          .map(record -> LsmBucketBulkInsertWriterHelper.rowWithFileIdAndKey(
              bucketIdToFileId,
              keyGen,
              record,
              indexKeyFieldList,
              numBucketsFunction,
              needFixedFileIdSuffix), sortTypeInfo)
          .name("lsm_bulk_insert_sort_keys")
          .setParallelism(writeTasks);
      return addBulkInsertSorter(
          conf,
          sortInput,
          sortTypeInfo,
          LsmBucketBulkInsertWriterHelper.getFileIdAndKeySorterGen(
              sortRowType, needFixedFileIdSuffix),
          "lsm_sorter:(file_group, record_key)",
          writeTasks);
    }

    RowType rowTypeWithFileId =
        BucketBulkInsertWriterHelper.rowTypeWithFileId(rowType, needFixedFileIdSuffix);
    InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowTypeWithFileId);
    DataStream<RowData> rowsWithFileId = routedDataStream
        .map(record -> BucketBulkInsertWriterHelper.rowWithFileId(
            bucketIdToFileId,
            keyGen,
            record,
            indexKeyFieldList,
            numBucketsFunction,
            needFixedFileIdSuffix), typeInfo)
        .setParallelism(writeTasks);

    if (!conf.get(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
      return rowsWithFileId;
    }

    return addBulkInsertSorter(
        conf,
        rowsWithFileId,
        typeInfo,
        BucketBulkInsertWriterHelper.getFileIdSorterGen(
            rowTypeWithFileId, needFixedFileIdSuffix),
        "file_sorter",
        writeTasks);
  }

  /**
   * Shuffles and sorts the input stream for a non-bucket bulk insert writer.
   *
   * <p>Partitioned input is optionally shuffled by partition path. The LSM layout then appends
   * the partition path and record key and sorts by both fields; for a non-partitioned table the
   * partition path is empty, so the effective ordering is by record key. The default layout keeps
   * the existing behavior: non-partitioned input is passed through without shuffle or sort, while
   * partitioned input is sorted only when bulk-insert input sorting is enabled.
   */
  private static DataStream<RowData> shuffleAndSort(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      int writeTasks,
      boolean isLsmTreeStorageLayout) {
    final boolean isPartitioned =
        !FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.PARTITION_PATH_FIELD);
    final boolean shouldShuffle =
        isPartitioned && conf.get(FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_INPUT);
    final RowDataKeyGen rowDataKeyGen = RowDataKeyGens.instance(conf, rowType);

    DataStream<RowData> routedDataStream = dataStream;
    if (shouldShuffle) {
      // Use #partitionCustom instead of #keyBy to avoid duplicate sort operations,
      // see BatchExecutionUtils#applyBatchExecutionSettings for details.
      Partitioner<String> partitioner =
          (key, channels) -> KeyGroupRangeAssignment.assignKeyToParallelOperator(
              key,
              KeyGroupRangeAssignment.computeDefaultMaxParallelism(writeTasks),
              channels);
      routedDataStream =
          dataStream.partitionCustom(partitioner, rowDataKeyGen::getPartitionPath);
    }

    if (isLsmTreeStorageLayout) {
      // LSM sorted runs are ordered by partition path and the encoded record key strings.
      RowType sortRowType = LsmBulkInsertWriterHelper.rowTypeWithPartitionAndKey(rowType);
      InternalTypeInfo<RowData> sortTypeInfo = InternalTypeInfo.of(sortRowType);
      DataStream<RowData> sortInput = routedDataStream
          .map(record -> LsmBulkInsertWriterHelper.rowWithPartitionAndKey(
              rowDataKeyGen.getPartitionPath(record), record, rowDataKeyGen), sortTypeInfo)
          .name("lsm_bulk_insert_sort_keys")
          .setParallelism(writeTasks);
      return addBulkInsertSorter(
          conf,
          sortInput,
          sortTypeInfo,
          LsmBulkInsertWriterHelper.getPartitionAndKeySorterGen(sortRowType),
          "lsm_sorter:(partition_path, record_key)",
          writeTasks);
    }

    if (!isPartitioned || !conf.get(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT)) {
      return routedDataStream;
    }

    // Unlike the LSM path, the default-layout sorter orders the original record-key fields by
    // their Flink logical types. The resulting order can differ from encoded record-key String
    // ordering; for example, numeric keys are ordered as 2, 10 here but as "10", "2" for LSM.
    final boolean sortByRecordKey =
        conf.get(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT_BY_RECORD_KEY);
    final String[] partitionFields = FilePathUtils.extractPartitionKeys(conf);
    final String[] recordKeyFields = OptionsResolver.getRecordKeys(conf);
    String[] sortFields = sortByRecordKey
        ? Stream.concat(Arrays.stream(partitionFields), Arrays.stream(recordKeyFields))
            .toArray(String[]::new)
        : partitionFields;

    return addBulkInsertSorter(
        conf,
        routedDataStream,
        InternalTypeInfo.of(rowType),
        new SortOperatorGen(rowType, sortFields),
        sortByRecordKey
            ? "sorter:(partition_key, record_key)"
            : "sorter:(partition_key)",
        writeTasks);
  }

  private static DataStream<RowData> addBulkInsertSorter(
      Configuration conf,
      DataStream<RowData> dataStream,
      TypeInformation<RowData> typeInfo,
      SortOperatorGen sortOperatorGen,
      String operatorName,
      int writeTasks) {
    DataStream<RowData> sortedDataStream = dataStream
        .transform(operatorName, typeInfo, sortOperatorGen.createSortOperator(conf))
        .setParallelism(writeTasks);
    FlinkTransformationUtils.setManagedMemoryWeight(
        sortedDataStream.getTransformation(),
        conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
    return sortedDataStream;
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
  public static DataStream<RowData> append(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      throw new HoodieNotSupportedException("Bucket index supports only upsert operation. Please, use upsert operation or switch to another index type.");
    }

    Option<Partitioner> insertPartitioner = OptionsResolver.getInsertPartitioner(conf);
    if (insertPartitioner.isPresent()) {
      RowDataKeyGen rowDataKeyGen = RowDataKeyGens.instance(conf, rowType);
      dataStream = dataStream.partitionCustom(insertPartitioner.get(), rowDataKeyGen::getHoodieKey);
    }

    WriteOperatorFactory<RowData> operatorFactory = AppendWriteOperator.getFactory(conf, rowType);

    SingleOutputStreamOperator<RowData> appendWriteDataStream = dataStream
        .transform(opName("hoodie_append_write", conf), TypeInformation.of(RowData.class), operatorFactory)
        .uid(opUID("hoodie_stream_write", conf))
        .setParallelism(conf.get(FlinkOptions.WRITE_TASKS));
    if (!BufferType.NONE.name().equalsIgnoreCase(AppendWriteFunctions.resolveBufferType(conf))) {
      declareManagedMemoryIfNecessary(conf, appendWriteDataStream, () -> OptionsResolver.getWriteBufferSizeInBytes(conf));
    }
    return appendWriteDataStream;
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
    final boolean globalIndex = conf.get(FlinkOptions.INDEX_GLOBAL_ENABLED);
    if (overwrite || OptionsResolver.isBucketIndexType(conf)) {
      return rowDataToHoodieRecord(conf, rowType, dataStream);
    }
    if (bounded && !globalIndex && OptionsResolver.isPartitionedTable(conf)) {
      return boundedBootstrap(conf, rowType, dataStream);
    }
    return streamBootstrap(conf, rowType, dataStream, bounded);
  }

  private static DataStream<HoodieFlinkInternalRow> streamBootstrap(
      Configuration conf,
      RowType rowType,
      DataStream<RowData> dataStream,
      boolean bounded) {
    DataStream<HoodieFlinkInternalRow> dataStream1 = rowDataToHoodieRecord(conf, rowType, dataStream);

    boolean isGlobalRLI = OptionsResolver.isGlobalRecordLevelIndex(conf);
    if (conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED) || (bounded && !isGlobalRLI)) {
      AbstractBootstrapOperator bootstrapOperator = BootstrapOperatorFactory.createInstance(conf);
      dataStream1 = dataStream1
          .transform(
              "index_bootstrap",
              new HoodieFlinkInternalRowTypeInfo(rowType),
              bootstrapOperator)
          .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(dataStream1.getParallelism()))
          .uid(opUID("index_bootstrap", conf));
      ((OneInputTransformation<?, ?>) dataStream1.getTransformation()).setChainingStrategy(ChainingStrategy.ALWAYS);
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
    final RowDataKeyGen rowDataKeyGen = RowDataKeyGens.instance(conf, rowType);
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
  public static DataStream<RowData> hoodieStreamWrite(Configuration conf,
                                                      RowType rowType,
                                                      DataStream<HoodieFlinkInternalRow> dataStream) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      return bucketStreamWrite(conf, rowType, dataStream);
    }

    String writeOperatorUid = opUID("stream_write", conf);
    // uuid is used to generate operator id for the write operator, then the bucket assign operator can send
    // operator event to the coordinator of the write operator based on the operator id.
    // @see org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway.
    DataStream<HoodieFlinkInternalRow> bucketAssignStream =
        createBucketAssignStream(dataStream, conf, rowType, writeOperatorUid);
    boolean isStreamingIndexWriteEnabled = OptionsResolver.isStreamingIndexWriteEnabled(conf);
    SingleOutputStreamOperator<RowData> writeDataStream = bucketAssignStream
        // shuffle by fileId(bucket id)
        .keyBy(HoodieFlinkInternalRow::getFileId)
        .transform(
            opName("stream_write", conf),
            isStreamingIndexWriteEnabled
                ? InternalTypeInfo.of(IndexRowUtils.INDEX_ROW_TYPE)
                : TypeInformation.of(RowData.class),
            StreamWriteOperator.getFactory(conf, rowType))
        .uid(writeOperatorUid)
        .setParallelism(conf.get(FlinkOptions.WRITE_TASKS));
    declareManagedMemoryIfNecessary(
        conf, writeDataStream, () -> OptionsResolver.getWriteBufferSizeInBytes(conf));

    return isStreamingIndexWriteEnabled
        ? addIndexWrite(conf, writeDataStream, writeOperatorUid)
        : writeDataStream;
  }

  /**
   * The bucket index streaming write pipeline.
   *
   * <p>For the simple bucket index, the input dataset shuffles directly by bucket ID. For the
   * consistent hashing bucket index, the bucket assigner first assigns a file group, then the
   * dataset shuffles by file ID before passing around to the write function. The pipelines look
   * like the following:
   *
   * <pre>
   * Simple bucket index:
   *      | input1 | ===\     /=== | task1 |
   *                   shuffle(by bucket ID)
   *      | input2 | ===/     \=== | task2 |
   *
   * Consistent hashing bucket index:
   *      | input1 | === | bucket assigner1 | ===\     /=== | task1 |
   *                                            shuffle(by file ID)
   *      | input2 | === | bucket assigner2 | ===/     \=== | task2 |
   *
   *      Note: a file group must be handled by one write task to avoid write conflict.
   * </pre>
   *
   * @param conf       The configuration
   * @param rowType    The logical row type of the input records
   * @param dataStream The input data stream
   * @return the bucket write data stream
   */
  private static DataStream<RowData> bucketStreamWrite(
      Configuration conf,
      RowType rowType,
      DataStream<HoodieFlinkInternalRow> dataStream) {
    HoodieIndex.BucketIndexEngineType bucketIndexEngineType =
        OptionsResolver.getBucketEngineType(conf);
    DataStream<HoodieFlinkInternalRow> bucketAssignedStream;
    String writeOperatorName;
    switch (bucketIndexEngineType) {
      case SIMPLE:
        // [HUDI-9036] BucketIndexPartitioner is also used in bulk insert mode,
        // keep use of HoodieKey here in partitionCustom for now
        Partitioner<HoodieKey> partitioner = BucketIndexPartitionerFactory.create(conf);
        bucketAssignedStream = dataStream.partitionCustom(
            partitioner,
            record -> new HoodieKey(record.getRecordKey(), record.getPartitionPath()));
        writeOperatorName = "bucket_write";
        break;
      case CONSISTENT_HASHING:
        if (OptionsResolver.isInsertOverwrite(conf)) {
          // TODO support insert overwrite for consistent bucket index
          throw new HoodieException("Consistent hashing bucket index does not work with insert overwrite using FLINK engine. Use simple bucket index or Spark engine.");
        }
        bucketAssignedStream = dataStream
            .transform(
                opName("consistent_bucket_assigner", conf),
                new HoodieFlinkInternalRowTypeInfo(rowType),
                new ProcessOperator<>(new ConsistentBucketAssignFunction(conf)))
            .uid(opUID("consistent_bucket_assigner", conf))
            .setParallelism(conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS))
            .keyBy(HoodieFlinkInternalRow::getFileId);
        writeOperatorName = "consistent_bucket_write";
        break;
      default:
        throw new HoodieNotSupportedException(
            "Unknown bucket index engine type: " + bucketIndexEngineType);
    }

    SingleOutputStreamOperator<RowData> bucketWriteStream = bucketAssignedStream
        .transform(
            opName(writeOperatorName, conf),
            TypeInformation.of(RowData.class),
            BucketStreamWriteOperator.getFactory(conf, rowType))
        .uid(opUID(writeOperatorName, conf))
        .setParallelism(conf.get(FlinkOptions.WRITE_TASKS));
    declareManagedMemoryIfNecessary(
        conf, bucketWriteStream, () -> OptionsResolver.getWriteBufferSizeInBytes(conf));
    return bucketWriteStream;
  }

  /**
   * The streaming index write pipeline.
   *
   * <p>Index rows emitted by the data write operator are routed to the task responsible for their
   * record-index file group. The data write operator UID identifies its coordinator so the index
   * writer can participate in the same commit. The whole pipeline looks like the following:
   *
   * <pre>
   *      | data write1 | ===\     /=== | index task1 |
   *                        shuffle(by index file group)
   *      | data write2 | ===/     \=== | index task2 |
   *
   *      Note: a record-index file group must be handled by one index write task.
   * </pre>
   *
   * @param conf             The configuration
   * @param writeDataStream  The index rows emitted by the data write operator
   * @param writeOperatorUid The UID of the upstream data write operator
   * @return the index write data stream
   */
  private static DataStream<RowData> addIndexWrite(
      Configuration conf,
      DataStream<RowData> writeDataStream,
      String writeOperatorUid) {
    SingleOutputStreamOperator<RowData> indexWriteDataStream = writeDataStream
        .partitionCustom(
            OptionsResolver.isRecordLevelIndex(conf)
                ? new RecordIndexPartitioner(conf)
                : new GlobalRecordIndexPartitioner(conf),
            IndexRowUtils::getHoodieKey)
        .transform(
            opName("index_write", conf),
            TypeInformation.of(RowData.class),
            new IndexWriteOperator(conf, OperatorIDGenerator.fromUid(writeOperatorUid)))
        .uid(opUID("index_write", conf))
        .setParallelism(conf.get(FlinkOptions.INDEX_WRITE_TASKS));
    declareManagedMemoryIfNecessary(
        conf,
        indexWriteDataStream,
        () -> conf.get(FlinkOptions.INDEX_RLI_WRITE_BUFFER_SIZE) * 1024L * 1024L);
    return indexWriteDataStream;
  }

  /**
   * Creates a bucket assignment stream that routes records to appropriate file groups based on the index type.
   *
   * @param inputStream The input data stream of HoodieFlinkInternalRow records to be assigned to buckets
   * @param conf        The configuration containing index and assignment settings
   * @param rowType     The logical row type of the input data stream
   * @return A DataStream of HoodieFlinkInternalRow records with bucket assignments
   */
  private static DataStream<HoodieFlinkInternalRow> createBucketAssignStream(
      DataStream<HoodieFlinkInternalRow> inputStream, Configuration conf, RowType rowType, String writeOperatorUid) {
    String assignerOperatorName = "bucket_assigner";
    if (OptionsResolver.isGlobalRecordLevelIndex(conf) && !conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      return inputStream
          .partitionCustom(new GlobalRecordIndexPartitioner(conf), row -> new HoodieKey(row.getRecordKey(), row.getPartitionPath()))
          .transform(
              assignerOperatorName,
              new HoodieFlinkInternalRowTypeInfo(rowType),
              new MiniBatchBucketAssignOperator(new MinibatchBucketAssignFunction(conf), OperatorIDGenerator.fromUid(writeOperatorUid)))
          .uid(opUID(assignerOperatorName, conf))
          .setParallelism(conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
    }
    if (OptionsResolver.isRecordLevelIndex(conf)) {
      return inputStream
          .keyBy(HoodieFlinkInternalRow::getRecordKey)
          .transform(
              assignerOperatorName,
              new HoodieFlinkInternalRowTypeInfo(rowType),
              new DynamicBucketAssignOperator(new DynamicBucketAssignFunction(conf), OperatorIDGenerator.fromUid(writeOperatorUid)))
          .uid(opUID(assignerOperatorName, conf))
          .setParallelism(conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
    }
    return inputStream
        // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
        .keyBy(HoodieFlinkInternalRow::getRecordKey)
        .transform(
            assignerOperatorName,
            new HoodieFlinkInternalRowTypeInfo(rowType),
            new KeyedProcessOperator<>(new BucketAssignFunction(conf)))
        .uid(opUID(assignerOperatorName, conf))
        .setParallelism(conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
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
  public static DataStreamSink<CompactionCommitEvent> compact(Configuration conf, DataStream<RowData> dataStream) {
    DataStreamSink<CompactionCommitEvent> compactionCommitEventDataStream = dataStream.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1)
        .partitionCustom(new IndexPartitioner(), CompactionPlanEvent::getIndex)
        .transform("compact_task",
            TypeInformation.of(CompactionCommitEvent.class),
            new CompactOperator(conf))
        .setParallelism(conf.get(FlinkOptions.COMPACTION_TASKS))
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
  public static DataStreamSink<ClusteringCommitEvent> cluster(Configuration conf, RowType rowType, DataStream<RowData> dataStream) {
    DataStream<ClusteringCommitEvent> clusteringStream = dataStream.transform("cluster_plan_generate",
            TypeInformation.of(ClusteringPlanEvent.class),
            new ClusteringPlanOperator(conf))
        .setParallelism(1) // plan generate must be singleton
        .setMaxParallelism(1) // plan generate must be singleton
        .partitionCustom(new IndexPartitioner(), ClusteringPlanEvent::getIndex)
        .transform("clustering_task",
            TypeInformation.of(ClusteringCommitEvent.class),
            new ClusteringOperator(conf, rowType))
        .setParallelism(conf.get(FlinkOptions.CLUSTERING_TASKS));
    if (OptionsResolver.sortClusteringEnabled(conf)) {
      FlinkTransformationUtils.setManagedMemoryWeight(clusteringStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);
    }
    DataStreamSink<ClusteringCommitEvent> clusteringCommitEventDataStream = clusteringStream.addSink(new ClusteringCommitSink(conf))
        .name("clustering_commit")
        .setParallelism(1); // clustering commit should be singleton
    clusteringCommitEventDataStream.getTransformation().setMaxParallelism(1);
    return clusteringCommitEventDataStream;
  }

  public static DataStreamSink<RowData> clean(Configuration conf, DataStream<RowData> dataStream) {
    DataStreamSink<RowData> cleanCommitDataStream = dataStream.addSink(new CleanFunction<>(conf))
        .setParallelism(1)
        .name("clean_commits");
    cleanCommitDataStream.getTransformation().setMaxParallelism(1);
    return cleanCommitDataStream;
  }

  public static DataStreamSink<RowData> dummySink(DataStream<RowData> dataStream) {
    int upstreamParallelism = dataStream.getParallelism();
    return dataStream.addSink(Pipelines.DummySink.INSTANCE)
        // keeps the same parallelism to upstream operators to enable partial failover.
        .setParallelism(upstreamParallelism)
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
    String databaseName = conf.get(FlinkOptions.DATABASE_NAME);
    return StringUtils.isNullOrEmpty(databaseName) ? conf.get(FlinkOptions.TABLE_NAME)
        : databaseName + "." + conf.get(FlinkOptions.TABLE_NAME);
  }

  public static void declareManagedMemoryIfNecessary(Configuration conf, DataStream<?> dataStream, Supplier<Long> bufferSizeSupplier) {
    if (OptionsResolver.isManagedMemoryBufferEnabled(conf)) {
      FlinkTransformationUtils.setManagedMemoryWeight(dataStream.getTransformation(), bufferSizeSupplier.get());
    }
  }

  /**
   * Dummy sink that does nothing.
   */
  public static class DummySink implements SinkFunctionAdapter<RowData> {
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
