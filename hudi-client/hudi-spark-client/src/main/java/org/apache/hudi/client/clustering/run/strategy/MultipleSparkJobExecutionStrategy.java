/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveOptimizationSortPartitioner;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.log.HoodieFileSliceReader.getFileSliceReader;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy to submit multiple spark jobs and union the results.
 */
public abstract class MultipleSparkJobExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(MultipleSparkJobExecutionStrategy.class);

  public MultipleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    // execute clustering for each group async and collect WriteStatus
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    // execute clustering for each group async and collect WriteStatus
    Stream<JavaRDD<WriteStatus>> writeStatusRDDStream = clusteringPlan.getInputGroups().stream()
        .map(inputGroup -> runClusteringForGroupAsync(inputGroup,
            clusteringPlan.getStrategy().getStrategyParams(),
            Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
            instantTime))
        .map(CompletableFuture::join);

    JavaRDD<WriteStatus>[] writeStatuses = convertStreamToArray(writeStatusRDDStream);
    JavaRDD<WriteStatus> writeStatusRDD = engineContext.union(writeStatuses);

    HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(writeStatusRDD);
    return writeMetadata;
  }


  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters.
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   *
   * @param inputRecords           RDD of {@link HoodieRecord}.
   * @param numOutputGroups        Number of output file groups.
   * @param instantTime            Clustering (replace commit) instant time.
   * @param strategyParams         Strategy parameters containing columns to sort the data by when clustering.
   * @param schema                 Schema of the data including metadata fields.
   * @param fileGroupIdList        File group id corresponding to each out group.
   * @param preserveHoodieMetadata Whether to preserve commit metadata while clustering.
   * @return RDD of {@link WriteStatus}.
   */
  public abstract JavaRDD<WriteStatus> performClusteringWithRecordsRDD(final JavaRDD<HoodieRecord<T>> inputRecords, final int numOutputGroups, final String instantTime,
                                                                       final Map<String, String> strategyParams, final Schema schema,
                                                                       final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata);

  /**
   * Create {@link BulkInsertPartitioner} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param schema         Schema of the data including metadata fields.
   * @return {@link RDDCustomColumnsSortPartitioner} if sort columns are provided, otherwise empty.
   */
  protected Option<BulkInsertPartitioner<T>> getPartitioner(Map<String, String> strategyParams, Schema schema) {
    if (getWriteConfig().isLayoutOptimizationEnabled()) {
      // sort input records by z-order/hilbert
      return Option.of(new RDDSpatialCurveOptimizationSortPartitioner((HoodieSparkEngineContext) getEngineContext(),
          getWriteConfig(), HoodieAvroUtils.addMetadataFields(schema)));
    } else if (strategyParams.containsKey(PLAN_STRATEGY_SORT_COLUMNS.key())) {
      return Option.of(new RDDCustomColumnsSortPartitioner(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()).split(","),
          HoodieAvroUtils.addMetadataFields(schema)));
    } else {
      return Option.empty();
    }
  }


  /**
   * Submit job to execute clustering for the group.
   */
  private CompletableFuture<JavaRDD<WriteStatus>> runClusteringForGroupAsync(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
                                                                             boolean preserveHoodieMetadata, String instantTime) {
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      JavaRDD<HoodieRecord<T>> inputRecords = readRecordsForGroup(jsc, clusteringGroup, instantTime);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsRDD(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata);
    });
  }

  /**
   * Get RDD of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private JavaRDD<HoodieRecord<T>> readRecordsForGroup(JavaSparkContext jsc, HoodieClusteringGroup clusteringGroup, String instantTime) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> op.getDeltaFilePaths().size() > 0);
    if (hasLogFiles) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      return readRecordsForGroupWithLogs(jsc, clusteringOps, instantTime);
    } else {
      // We want to optimize reading records for case there are no log files.
      return readRecordsForGroupBaseFiles(jsc, clusteringOps);
    }
  }

  /**
   * Read records from baseFiles, apply updates and convert to RDD.
   */
  private JavaRDD<HoodieRecord<T>> readRecordsForGroupWithLogs(JavaSparkContext jsc,
                                                               List<ClusteringOperation> clusteringOps,
                                                               String instantTime) {
    HoodieWriteConfig config = getWriteConfig();
    HoodieTable table = getHoodieTable();
    return jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<HoodieRecord<T>>> recordIterators = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), config);
        LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
          HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
              .withFileSystem(table.getMetaClient().getFs())
              .withBasePath(table.getMetaClient().getBasePath())
              .withLogFilePaths(clusteringOp.getDeltaFilePaths())
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(instantTime)
              .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
              .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
              .withReverseReader(config.getCompactionReverseLogReadEnabled())
              .withBufferSize(config.getMaxDFSStreamBufferSize())
              .withSpillableMapBasePath(config.getSpillableMapBasePath())
              .withPartition(clusteringOp.getPartitionPath())
              .build();

          Option<HoodieFileReader> baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
              ? Option.empty()
              : Option.of(HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath())));
          HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
          recordIterators.add(getFileSliceReader(baseFileReader, scanner, readerSchema,
              tableConfig.getPayloadClass(),
              tableConfig.getPreCombineField(),
              tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
                  tableConfig.getPartitionFieldProp()))));
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      });

      return new ConcatenatingIterator<>(recordIterators);
    });
  }

  /**
   * Read records from baseFiles and convert to RDD.
   */
  private JavaRDD<HoodieRecord<T>> readRecordsForGroupBaseFiles(JavaSparkContext jsc,
                                                                List<ClusteringOperation> clusteringOps) {
    return jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<IndexedRecord>> iteratorsForPartition = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
          HoodieFileReader<IndexedRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(getHoodieTable().getHadoopConf(), new Path(clusteringOp.getDataFilePath()));
          iteratorsForPartition.add(baseFileReader.getRecordIterator(readerSchema));
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      });

      return new ConcatenatingIterator<>(iteratorsForPartition);
    }).map(this::transform);
  }

  /**
   * Stream to array conversion with generic type is not straightforward.
   * Implement a utility method to abstract high level logic. This needs to be improved in future
   */
  private JavaRDD<WriteStatus>[] convertStreamToArray(Stream<JavaRDD<WriteStatus>> writeStatusRDDStream) {
    Object[] writeStatusObjects = writeStatusRDDStream.toArray(Object[]::new);
    JavaRDD<WriteStatus>[] writeStatusRDDArray = new JavaRDD[writeStatusObjects.length];
    for (int i = 0; i < writeStatusObjects.length; i++) {
      writeStatusRDDArray[i] = (JavaRDD<WriteStatus>) writeStatusObjects[i];
    }
    return writeStatusRDDArray;
  }

  /**
   * Transform IndexedRecord into HoodieRecord.
   */
  private HoodieRecord<T> transform(IndexedRecord indexedRecord) {
    GenericRecord record = (GenericRecord) indexedRecord;
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!getWriteConfig().populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(getWriteConfig().getProps())));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerators are supported when meta columns are disabled ", e);
      }
    }
    String key = KeyGenUtils.getRecordKeyFromGenericRecord(record, keyGeneratorOpt);
    String partition = KeyGenUtils.getPartitionPathFromGenericRecord(record, keyGeneratorOpt);
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }
}
