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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
import org.apache.hadoop.fs.Path;
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

import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

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
  private transient BulkInsertWriterHelper writerHelper;
  private transient String instantTime;

  private transient BinaryExternalSorter sorter;
  private transient StreamRecordCollector<ClusteringCommitEvent> collector;
  private transient BinaryRowDataSerializer binarySerializer;

  public ClusteringOperator(Configuration conf, RowType rowType) {
    this.conf = conf;
    this.rowType = rowType;
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
    this.writeClient = StreamerUtil.createWriteClient(conf, getRuntimeContext());
    this.table = writeClient.getHoodieTable();

    this.schema = StreamerUtil.getTableAvroSchema(table.getMetaClient(), false);
    this.readerSchema = HoodieAvroUtils.addMetadataFields(this.schema);
    this.requiredPos = getRequiredPositions();

    this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(rowType);

    ClassLoader cl = getContainingTask().getUserCodeClassLoader();

    AbstractRowDataSerializer inputSerializer = new BinaryRowDataSerializer(rowType.getFieldCount());
    this.binarySerializer = new BinaryRowDataSerializer(inputSerializer.getArity());

    NormalizedKeyComputer computer = createSortCodeGenerator().generateNormalizedKeyComputer("SortComputer").newInstance(cl);
    RecordComparator comparator = createSortCodeGenerator().generateRecordComparator("SortComparator").newInstance(cl);

    MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
    this.sorter =
        new BinaryExternalSorter(
            this.getContainingTask(),
            memManager,
            computeMemorySize(),
            this.getContainingTask().getEnvironment().getIOManager(),
            inputSerializer,
            binarySerializer,
            computer,
            comparator,
            getContainingTask().getJobConfiguration());
    this.sorter.startThreads();

    collector = new StreamRecordCollector<>(output);

    // register the metrics.
    getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
    getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
    getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
  }

  @Override
  public void processElement(StreamRecord<ClusteringPlanEvent> element) throws Exception {
    ClusteringPlanEvent event = element.getValue();
    final String instantTime = event.getClusteringInstantTime();
    final ClusteringGroupInfo clusteringGroupInfo = event.getClusteringGroupInfo();

    initWriterHelper(instantTime);

    List<ClusteringOperation> clusteringOps = clusteringGroupInfo.getOperations();
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> op.getDeltaFilePaths().size() > 0);

    Iterator<RowData> iterator;
    if (hasLogFiles) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      iterator = readRecordsForGroupWithLogs(clusteringOps, instantTime);
    } else {
      // We want to optimize reading records for case there are no log files.
      iterator = readRecordsForGroupBaseFiles(clusteringOps);
    }

    RowDataSerializer rowDataSerializer = new RowDataSerializer(rowType);
    while (iterator.hasNext()) {
      RowData rowData = iterator.next();
      BinaryRowData binaryRowData = rowDataSerializer.toBinaryRow(rowData).copy();
      this.sorter.write(binaryRowData);
    }

    BinaryRowData row = binarySerializer.createInstance();
    while ((row = sorter.getIterator().next(row)) != null) {
      this.writerHelper.write(row);
    }
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.cleanHandlesGracefully();
      this.writeClient.close();
    }
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    List<WriteStatus> writeStatuses = this.writerHelper.getWriteStatuses(this.taskID);
    collector.collect(new ClusteringCommitEvent(instantTime, writeStatuses, this.taskID));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initWriterHelper(String clusteringInstantTime) {
    if (this.writerHelper == null) {
      this.writerHelper = new BulkInsertWriterHelper(this.conf, this.table, this.writeConfig,
          clusteringInstantTime, this.taskID, getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getAttemptNumber(),
          this.rowType);
      this.instantTime = clusteringInstantTime;
    }
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
        Option<HoodieAvroFileReader> baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
            ? Option.empty()
            : Option.of(HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath())));
        HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
            .withFileSystem(table.getMetaClient().getFs())
            .withBasePath(table.getMetaClient().getBasePath())
            .withLogFilePaths(clusteringOp.getDeltaFilePaths())
            .withReaderSchema(readerSchema)
            .withLatestInstantTime(instantTime)
            .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
            .withReadBlocksLazily(writeConfig.getCompactionLazyBlockReadEnabled())
            .withReverseReader(writeConfig.getCompactionReverseLogReadEnabled())
            .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
            .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
            .build();

        HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
        HoodieFileSliceReader<? extends IndexedRecord> hoodieFileSliceReader = HoodieFileSliceReader.getFileSliceReader(baseFileReader, scanner, readerSchema,
            tableConfig.getPayloadClass(),
            tableConfig.getPreCombineField(),
            tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
                tableConfig.getPartitionFieldProp())));

        recordIterators.add(StreamSupport.stream(Spliterators.spliteratorUnknownSize(hoodieFileSliceReader, Spliterator.NONNULL), false).map(hoodieRecord -> {
          try {
            return this.transform(hoodieRecord.toIndexedRecord(readerSchema, new Properties()).get());
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
          return HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath())).getRecordIterator(readerSchema);
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
    GenericRecord record = buildAvroRecordBySchema(indexedRecord, schema, requiredPos, new GenericRecordBuilder(schema));
    return (RowData) avroToRowDataConverter.convert(record);
  }

  private int[] getRequiredPositions() {
    final List<String> fieldNames = readerSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    return schema.getFields().stream()
        .map(field -> fieldNames.indexOf(field.name()))
        .mapToInt(i -> i)
        .toArray();
  }

  private SortCodeGenerator createSortCodeGenerator() {
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType,
        conf.getString(FlinkOptions.CLUSTERING_SORT_COLUMNS).split(","));
    return sortOperatorGen.createSortCodeGenerator();
  }

  @Override
  public void setKeyContextElement(StreamRecord<ClusteringPlanEvent> record) throws Exception {
    OneInputStreamOperator.super.setKeyContextElement(record);
  }
}
