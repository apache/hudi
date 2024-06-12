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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.adapter.Utils;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metrics.FlinkClusteringMetrics;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Operator to execute the actual clustering task assigned by the clustering plan task.
 * In order to execute scalable, the input should shuffle by the clustering event {@link ClusteringPlanEvent}.
 */
public class ClusteringOperator extends TableStreamOperator<ClusteringCommitEvent> implements
    OneInputStreamOperator<ClusteringPlanEvent, ClusteringCommitEvent>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(ClusteringOperator.class);

  private final Configuration conf;
  private final RowType rowType;
  private int taskID;
  private transient HoodieWriteConfig writeConfig;
  private transient HoodieFlinkTable<?> table;
  private transient Schema schema;
  private transient Schema readerSchema;
  private transient int[] requiredPos;
  private transient AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;
  private transient HoodieFlinkWriteClient writeClient;
  private transient StreamRecordCollector<ClusteringCommitEvent> collector;
  private transient BinaryRowDataSerializer binarySerializer;

  /**
   * Whether to execute clustering asynchronously.
   */
  private final boolean asyncClustering;

  /**
   * Whether the clustering sort is enabled.
   */
  private final boolean sortClusteringEnabled;

  /**
   * Executor service to execute the clustering task.
   */
  private transient NonThrownExecutor executor;

  private transient FlinkClusteringMetrics clusteringMetrics;

  public ClusteringOperator(Configuration conf, RowType rowType) {
    // copy a conf let following modification not to impact the global conf
    this.conf = new Configuration(conf);
    this.rowType = BulkInsertWriterHelper.addMetadataFields(rowType, false);
    this.asyncClustering = OptionsResolver.needsAsyncClustering(conf);
    this.sortClusteringEnabled = OptionsResolver.sortClusteringEnabled(conf);

    // override max parquet file size in conf
    this.conf.setLong(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(),
        this.conf.getLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES));

    // target size should larger than small file limit
    this.conf.setLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT.key(),
        Math.min(this.conf.getLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES) / 1024 / 1024,
            this.conf.getLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT)));
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ClusteringCommitEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(this.conf);
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.table = writeClient.getHoodieTable();

    this.schema = AvroSchemaConverter.convertToSchema(rowType);
    this.readerSchema = this.schema;
    this.requiredPos = getRequiredPositions();

    this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(rowType);
    this.binarySerializer = new BinaryRowDataSerializer(rowType.getFieldCount());

    if (this.asyncClustering) {
      this.executor = NonThrownExecutor.builder(LOG).build();
    }

    this.collector = new StreamRecordCollector<>(output);

    registerMetrics();
  }

  @Override
  public void processElement(StreamRecord<ClusteringPlanEvent> element) throws Exception {
    final ClusteringPlanEvent event = element.getValue();
    final String instantTime = event.getClusteringInstantTime();
    final List<ClusteringOperation> clusteringOperations = event.getClusteringGroupInfo().getOperations();
    if (this.asyncClustering) {
      // executes the compaction task asynchronously to not block the checkpoint barrier propagate.
      executor.execute(
          () -> doClustering(instantTime, clusteringOperations),
          (errMsg, t) -> collector.collect(new ClusteringCommitEvent(instantTime, getFileIds(clusteringOperations), taskID)),
          "Execute clustering for instant %s from task %d", instantTime, taskID);
    } else {
      // executes the clustering task synchronously for batch mode.
      LOG.info("Execute clustering for instant {} from task {}", instantTime, taskID);
      doClustering(instantTime, clusteringOperations);
    }
  }

  @Override
  public void close() throws Exception {
    if (null != this.executor) {
      this.executor.close();
    }
    if (this.writeClient != null) {
      this.writeClient.close();
      this.writeClient = null;
    }
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    // no operation
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void doClustering(String instantTime, List<ClusteringOperation> clusteringOperations) throws Exception {
    clusteringMetrics.startClustering();
    BulkInsertWriterHelper writerHelper = new BulkInsertWriterHelper(this.conf, this.table, this.writeConfig,
        instantTime, this.taskID, getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getAttemptNumber(),
        this.rowType, true);

    Iterator<RowData> iterator;
    if (clusteringOperations.stream().anyMatch(operation -> CollectionUtils.nonEmpty(operation.getDeltaFilePaths()))) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      iterator = readRecordsForGroupWithLogs(clusteringOperations, instantTime);
    } else {
      // We want to optimize reading records for case there are no log files.
      iterator = readRecordsForGroupBaseFiles(clusteringOperations);
    }

    if (this.sortClusteringEnabled) {
      RowDataSerializer rowDataSerializer = new RowDataSerializer(rowType);
      BinaryExternalSorter sorter = initSorter();
      while (iterator.hasNext()) {
        RowData rowData = iterator.next();
        BinaryRowData binaryRowData = rowDataSerializer.toBinaryRow(rowData).copy();
        sorter.write(binaryRowData);
      }

      BinaryRowData row = binarySerializer.createInstance();
      while ((row = sorter.getIterator().next(row)) != null) {
        writerHelper.write(row);
      }
      sorter.close();
    } else {
      while (iterator.hasNext()) {
        writerHelper.write(iterator.next());
      }
    }

    List<WriteStatus> writeStatuses = writerHelper.getWriteStatuses(this.taskID);
    clusteringMetrics.endClustering();
    collector.collect(new ClusteringCommitEvent(instantTime, getFileIds(clusteringOperations), writeStatuses, this.taskID));
    writerHelper.close();
  }

  /**
   * Read records from baseFiles, apply updates and convert to Iterator.
   */
  @SuppressWarnings("unchecked")
  private Iterator<RowData> readRecordsForGroupWithLogs(List<ClusteringOperation> clusteringOps, String instantTime) {
    List<Iterator<RowData>> recordIterators = new ArrayList<>();

    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new FlinkTaskContextSupplier(null), writeConfig);
    LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);

    for (ClusteringOperation clusteringOp : clusteringOps) {
      try {
        Option<HoodieFileReader> baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
            ? Option.empty()
            : Option.of(HoodieIOFactory.getIOFactory(table.getStorage())
            .getReaderFactory(table.getConfig().getRecordMerger().getRecordType())
            .getFileReader(table.getConfig(), new StoragePath(clusteringOp.getDataFilePath())));
        HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
            .withStorage(table.getStorage())
            .withBasePath(table.getMetaClient().getBasePath())
            .withLogFilePaths(clusteringOp.getDeltaFilePaths())
            .withReaderSchema(readerSchema)
            .withLatestInstantTime(instantTime)
            .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
            .withReverseReader(writeConfig.getCompactionReverseLogReadEnabled())
            .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
            .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
            .withDiskMapType(writeConfig.getCommonConfig().getSpillableDiskMapType())
            .withBitCaskDiskMapCompressionEnabled(writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
            .withRecordMerger(writeConfig.getRecordMerger())
            .build();

        HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
        HoodieFileSliceReader<? extends IndexedRecord> hoodieFileSliceReader = new HoodieFileSliceReader(baseFileReader, scanner, readerSchema,
            tableConfig.getPreCombineField(),writeConfig.getRecordMerger(),
            tableConfig.getProps(),
            tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
                tableConfig.getPartitionFieldProp())));

        recordIterators.add(StreamSupport.stream(Spliterators.spliteratorUnknownSize(hoodieFileSliceReader, Spliterator.NONNULL), false).map(hoodieRecord -> {
          try {
            return this.transform(hoodieRecord.toIndexedRecord(readerSchema, new Properties()).get().getData());
          } catch (IOException e) {
            throw new HoodieIOException("Failed to read next record", e);
          }
        }).iterator());
      } catch (IOException e) {
        throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
            + " and " + clusteringOp.getDeltaFilePaths(), e);
      }
    }

    return new ConcatenatingIterator<>(recordIterators);
  }

  /**
   * Read records from baseFiles and get iterator.
   */
  private Iterator<RowData> readRecordsForGroupBaseFiles(List<ClusteringOperation> clusteringOps) {
    List<Iterator<RowData>> iteratorsForPartition = clusteringOps.stream().map(clusteringOp -> {
      Iterable<IndexedRecord> indexedRecords = () -> {
        try {
          HoodieFileReaderFactory fileReaderFactory = HoodieIOFactory.getIOFactory(table.getStorage())
              .getReaderFactory(table.getConfig().getRecordMerger().getRecordType());
          HoodieAvroFileReader fileReader = (HoodieAvroFileReader) fileReaderFactory.getFileReader(
              table.getConfig(), new StoragePath(clusteringOp.getDataFilePath()));

          return new CloseableMappingIterator<>(fileReader.getRecordIterator(readerSchema), HoodieRecord::getData);
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      };

      return StreamSupport.stream(indexedRecords.spliterator(), false).map(this::transform).iterator();
    }).collect(Collectors.toList());

    return new ConcatenatingIterator<>(iteratorsForPartition);
  }

  /**
   * Transform IndexedRecord into HoodieRecord.
   */
  private RowData transform(IndexedRecord indexedRecord) {
    GenericRecord record = (GenericRecord) indexedRecord;
    return (RowData) avroToRowDataConverter.convert(record);
  }

  private int[] getRequiredPositions() {
    final List<String> fieldNames = readerSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    return schema.getFields().stream()
        .map(field -> fieldNames.indexOf(field.name()))
        .mapToInt(i -> i)
        .toArray();
  }

  private BinaryExternalSorter initSorter() {
    ClassLoader cl = getContainingTask().getUserCodeClassLoader();
    NormalizedKeyComputer computer = createSortCodeGenerator().generateNormalizedKeyComputer("SortComputer").newInstance(cl);
    RecordComparator comparator = createSortCodeGenerator().generateRecordComparator("SortComparator").newInstance(cl);

    MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
    BinaryExternalSorter sorter = Utils.getBinaryExternalSorter(
            this.getContainingTask(),
            memManager,
            computeMemorySize(),
            this.getContainingTask().getEnvironment().getIOManager(),
            (AbstractRowDataSerializer) binarySerializer,
            binarySerializer,
            computer,
            comparator,
            this.conf);
    sorter.startThreads();

    // register the metrics.
    getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
    getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
    getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
    return sorter;
  }

  private SortCodeGenerator createSortCodeGenerator() {
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType,
        conf.getString(FlinkOptions.CLUSTERING_SORT_COLUMNS).split(","));
    return sortOperatorGen.createSortCodeGenerator();
  }

  private String getFileIds(List<ClusteringOperation> clusteringOperations) {
    return clusteringOperations.stream().map(ClusteringOperation::getFileId).collect(Collectors.joining(","));
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) {
    this.executor = executor;
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringCommitEvent>> output) {
    this.output = output;
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    clusteringMetrics = new FlinkClusteringMetrics(metrics);
    clusteringMetrics.registerMetrics();
  }
}
