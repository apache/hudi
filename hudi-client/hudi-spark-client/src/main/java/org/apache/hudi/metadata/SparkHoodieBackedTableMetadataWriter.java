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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDMetadataWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;

@Slf4j
public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> {

  /**
   * Return a Spark based implementation of {@code HoodieTableMetadataWriter} which can be used to
   * write to the metadata table.
   * <p>
   * If the metadata table does not exist, an attempt is made to bootstrap it but there is no guaranteed that
   * table will end up bootstrapping at this time.
   *
   * @param conf
   * @param writeConfig
   * @param context
   * @param inflightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 attempting to bootstrap the table.
   * @return An instance of the {@code HoodieTableMetadataWriter}
   */
  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  SparkHoodieBackedTableMetadataWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp) {
    this(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, false);
  }

  SparkHoodieBackedTableMetadataWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp,
                                       boolean streamingWrites) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, streamingWrites);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry;
      if (metadataWriteConfig.isExecutorMetricsEnabled() && metadataWriteConfig.getMetricsReporterType() != MetricsReporterType.INMEMORY) {
        registry = Registry.getRegistry("HoodieMetadata", DistributedRegistry.class.getName());
        HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
        ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
      } else {
        registry = Registry.getRegistry("HoodieMetadata");
      }
      this.metrics = Option.of(new HoodieMetadataMetrics(metadataWriteConfig.getMetricsConfig(), dataMetaClient.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected JavaRDD<HoodieRecord> convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
    return HoodieJavaRDD.getJavaRDD(records);
  }

  @Override
  protected HoodieData<WriteStatus> convertEngineSpecificDataToHoodieData(JavaRDD<WriteStatus> records) {
    return HoodieJavaRDD.of(records);
  }

  @Override
  public JavaRDD<WriteStatus> streamWriteToMetadataTable(Pair<List<HoodieFileGroupId>, HoodieData<HoodieRecord>> fileGroupIdToTaggedRecords, String instantTime) {
    JavaRDD<HoodieRecord> mdtRecords = HoodieJavaRDD.getJavaRDD(fileGroupIdToTaggedRecords.getValue());
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting with instant %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
    JavaRDD<WriteStatus> partialMetadataWriteStatuses = getSparkWriteClient(Option.empty()).firstUpsertPreppedRecords(mdtRecords, instantTime, fileGroupIdToTaggedRecords.getKey());
    return partialMetadataWriteStatuses;
  }

  @Override
  public JavaRDD<WriteStatus> secondaryWriteToMetadataTablePartitions(JavaRDD<HoodieRecord> preppedRecords, String instantTime) {
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
    JavaRDD<WriteStatus> partialMetadataWriteStatuses = getSparkWriteClient(Option.empty()).secondaryUpsertPreppedRecords(preppedRecords, instantTime);
    return partialMetadataWriteStatuses;
  }

  @Override
  protected void bulkInsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient,
                                     String instantTime,
                                     JavaRDD<HoodieRecord> preppedRecordInputs,
                                     Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.bulkInsertPreppedRecords(preppedRecordInputs, instantTime, bulkInsertPartitioner);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs) {
    // When specified, reduce the parallelism of input record RDD to improve write performance.
    int parallelism = dataWriteConfig.getMetadataConfig().getRecordPreparationParallelism();
    if (parallelism > 0 && preppedRecordInputs.getNumPartitions() > parallelism) {
      preppedRecordInputs = preppedRecordInputs.coalesce(parallelism);
    }

    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient,
                                 String instantTime,
                                 JavaRDD<HoodieRecord> preppedRecordInputs,
                                 List<HoodieFileGroupId> fileGroupsIdsToUpdate) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = getSparkWriteClient(Option.of(writeClient)).firstUpsertPreppedRecords(preppedRecordInputs, instantTime, fileGroupsIdsToUpdate);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void bulkCommit(String instantTime, String partitionPath, HoodieData<HoodieRecord> records,
      MetadataTableFileGroupIndexParser indexParser) {
    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(indexParser);
    commitInternal(instantTime, Collections.singletonMap(partitionPath, records), true, Option.of(partitioner));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    List<String> partitionsToDrop = partitions.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList());
    log.info("Deleting Metadata Table partitions: {}", partitionsToDrop);

    SparkRDDWriteClient writeClient = (SparkRDDWriteClient) getWriteClient();
    String actionType = CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, HoodieTableType.MERGE_ON_READ);
    writeClient.startCommitForMetadataTable(metadataMetaClient, instantTime, actionType);
    HoodieWriteResult result = writeClient.deletePartitions(partitionsToDrop, instantTime);
    writeClient.commit(instantTime, result.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION, result.getPartitionToReplaceFileIds());
  }

  /**
   * Loads the file slices touched by the commit due to given instant time and returns the records for the expression index.
   * This generates partition stat record updates along with EI column stat update records. Partition stat record updates are generated
   * by reloading the affected partitions column range metadata from EI and then merging it with partition stat record from the updated data.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param indexPartition partition name of the expression index
   * @param instantTime    timestamp at of the current update commit
   */
  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexUpdates(HoodieCommitMetadata commitMetadata, String indexPartition, String instantTime) throws Exception {
    HoodieIndexDefinition indexDefinition = getIndexDefinition(indexPartition);
    boolean isExprIndexUsingColumnStats = indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS);
    Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt = Option.empty();
    if (isExprIndexUsingColumnStats) {
      // Fetch column range metadata for affected partitions in the commit
      HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> exprIndexPartitionStatUpdates =
          SparkMetadataWriterUtils.getExpressionIndexPartitionStatsForExistingFiles(
                  commitMetadata, indexPartition, engineContext, getTableMetadata(), dataMetaClient, dataWriteConfig.getMetadataConfig(),
                  Option.of(dataWriteConfig.getRecordMerger().getRecordType()), instantTime, dataWriteConfig)
              .flatMapValues(List::iterator);
      // The function below merges the column range metadata from the updated data with latest column range metadata of affected partition computed above
      partitionRecordsFunctionOpt = Option.of(rangeMetadata ->
          HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(exprIndexPartitionStatUpdates.union(rangeMetadata), true, Option.of(indexDefinition.getIndexName())));
    }

    // Step 1: Generate partition name, file path and size triplets from the newly created files in the commit metadata
    List<Pair<String, Pair<String, Long>>> partitionFilePathPairs = new ArrayList<>();
    commitMetadata.getPartitionToWriteStats().forEach((dataPartition, writeStats) -> writeStats.forEach(writeStat -> partitionFilePathPairs.add(
        Pair.of(writeStat.getPartitionPath(), Pair.of(new StoragePath(dataMetaClient.getBasePath(), writeStat.getPath()).toString(), writeStat.getFileSizeInBytes())))));
    int parallelism = Math.min(partitionFilePathPairs.size(), dataWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema = new TableSchemaResolver(dataMetaClient).getTableSchema();
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataMetaClient, tableSchema);
    // Step 2: Compute the expression index column stat and partition stat records for these newly created files
    // partitionRecordsFunctionOpt - Function used to generate partition stats. These stats are generated only for expression index created using column stats
    //
    // In the partitionRecordsFunctionOpt function we merge the expression index records from the new files created in the commit metadata
    // with the expression index records from the unmodified files to get the new partition stat records
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathPairs, indexDefinition, dataMetaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
            partitionRecordsFunctionOpt);
    return expressionIndexComputationMetadata.getPartitionStatRecordsOpt().isPresent()
        ? expressionIndexComputationMetadata.getExpressionIndexRecords().union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get())
        : expressionIndexComputationMetadata.getExpressionIndexRecords();
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet,
                                                               HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               HoodieSchema tableSchema, HoodieSchema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathAndSizeTriplet, indexDefinition,
        metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        Option.of(rangeMetadata ->
            HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(rangeMetadata, true, Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get());
    }
    return exprIndexRecords;
  }

  @Override
  protected HoodieData<HoodieRecord> getVectorIndexRecords(HoodieIndexDefinition indexDefinition) {
    try {
      String vectorColumn = indexDefinition.getSourceFields().get(0);
      HoodieSchema tableSchema = new TableSchemaResolver(dataMetaClient).getTableSchema();
      Pair<String, HoodieSchemaField> fieldSchema = HoodieSchemaUtils.getNestedField(tableSchema, vectorColumn)
          .orElseThrow(() -> new HoodieMetadataException("Vector column not found in table schema: " + vectorColumn));
      HoodieSchema vectorSchema = fieldSchema.getRight().schema().getNonNullType();
      ValidationUtils.checkState(vectorSchema.getType() == HoodieSchemaType.VECTOR,
          "Vector index can only be bootstrapped from VECTOR columns: " + vectorColumn);

      HoodieSchema.Vector resolvedVectorType = (HoodieSchema.Vector) vectorSchema;
      int configuredDimension = VectorIndexOptions.getDimension(indexDefinition.getIndexOptions());
      ValidationUtils.checkState(resolvedVectorType.getDimension() == configuredDimension,
          String.format("Vector dimension mismatch for %s: schema=%s, configured=%s",
              vectorColumn, resolvedVectorType.getDimension(), configuredDimension));

      // Read vectors directly into a lightweight RDD — no DataFrame/UDF overhead
      JavaRDD<SparkVectorIndexBootstrap.VectorRow> vectorRows = buildVectorRowsRDD(
          tableSchema, vectorColumn, indexDefinition.getIndexName());

      // generationId from the latest commit instant, not wall-clock
      String generationId = deriveGenerationId();
      long lastUpdatedTs = System.currentTimeMillis();
      JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
      return SparkVectorIndexBootstrap.bootstrap(
          jsc, vectorRows, indexDefinition, resolvedVectorType.getVectorElementType(),
          generationId, lastUpdatedTs);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to bootstrap vector index records", e);
    }
  }

  /**
   * Read all vectors from the latest base-table file slices into a lightweight RDD.
   * No DataFrame/UDF overhead — just raw (recordKey, partitionPath, fileId, vectorBytes) tuples.
   */
  private JavaRDD<SparkVectorIndexBootstrap.VectorRow> buildVectorRowsRDD(
      HoodieSchema tableSchema,
      String vectorColumn,
      String indexName) {
    List<Pair<String, FileSlice>> latestMergedFileSlices = getLatestMergedFileSlicesForVectorBootstrap();
    if (latestMergedFileSlices.isEmpty()) {
      log.warn("Vector index bootstrap found no latest file slices for {}", dataMetaClient.getBasePath());
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().emptyRDD();
    }

    log.info("Vector index bootstrap discovered {} latest file slices for {}", latestMergedFileSlices.size(), indexName);

    Option<String> instantTime = dataMetaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(instant -> instant.requestedTime());
    if (!instantTime.isPresent()) {
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().emptyRDD();
    }

    HoodieSchema requestedSchema = buildVectorBootstrapRequestedSchema(tableSchema, vectorColumn);
    ReaderContextFactory<InternalRow> readerContextFactory = engineContext.getReaderContextFactory(dataMetaClient);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(dataWriteConfig.getInternalSchema());
    int parallelism = latestMergedFileSlices.size();

    return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
        .parallelize(latestMergedFileSlices, parallelism)
        .flatMap(partitionAndFileSlice -> {
          String partitionPath = partitionAndFileSlice.getKey();
          FileSlice fileSlice = partitionAndFileSlice.getValue();
          HoodieReaderContext<InternalRow> readerContext = readerContextFactory.getContext();
          try (HoodieFileGroupReader<InternalRow> fileGroupReader = HoodieFileGroupReader.<InternalRow>newBuilder()
              .withReaderContext(readerContext)
              .withHoodieTableMetaClient(dataMetaClient)
              .withFileSlice(fileSlice)
              .withLatestCommitTime(instantTime.get())
              .withDataSchema(tableSchema)
              .withRequestedSchema(requestedSchema)
              .withInternalSchema(internalSchemaOption)
              .withShouldUseRecordPosition(false)
              .withProps(dataMetaClient.getTableConfig().getProps())
              .build();
               ClosableIterator<InternalRow> iterator = fileGroupReader.getClosableIterator()) {
            List<SparkVectorIndexBootstrap.VectorRow> rows = new ArrayList<>();
            while (iterator.hasNext()) {
              InternalRow record = iterator.next();
              Object vectorValue = readerContext.getRecordContext().getValue(record, requestedSchema, vectorColumn);
              if (vectorValue == null) {
                continue;
              }
              String recordKey = readerContext.getRecordContext().getRecordKey(record, requestedSchema);
              byte[] vectorBytes;
              if (vectorValue instanceof byte[]) {
                vectorBytes = (byte[]) vectorValue;
              } else if (vectorValue instanceof org.apache.spark.sql.catalyst.util.ArrayData) {
                // VECTOR columns may be returned as float arrays by the reader context
                // when the schema resolves the column as ArrayType(FloatType) instead of BinaryType.
                org.apache.spark.sql.catalyst.util.ArrayData arrayData =
                    (org.apache.spark.sql.catalyst.util.ArrayData) vectorValue;
                int numElements = arrayData.numElements();
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(numElements * Float.BYTES)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < numElements; i++) {
                  buf.putFloat(arrayData.getFloat(i));
                }
                vectorBytes = buf.array();
              } else {
                throw new HoodieMetadataException(
                    "Expected byte[] or ArrayData for VECTOR column, got: " + vectorValue.getClass().getName());
              }
              rows.add(new SparkVectorIndexBootstrap.VectorRow(
                  recordKey, partitionPath, fileSlice.getFileId(),
                  fileSlice.getBaseInstantTime(), vectorBytes));
            }
            return rows.iterator();
          }
        });
  }

  private HoodieSchema buildVectorBootstrapRequestedSchema(HoodieSchema tableSchema, String vectorColumn) {
    LinkedHashSet<String> projectedFields = new LinkedHashSet<>();
    if (dataMetaClient.getTableConfig().populateMetaFields()) {
      projectedFields.add(RECORD_KEY_METADATA_FIELD);
    } else {
      projectedFields.addAll(Arrays.asList(dataMetaClient.getTableConfig().getRecordKeyFields()
          .orElseThrow(() -> new HoodieMetadataException("Cannot bootstrap vector index without record key fields"))));
    }
    projectedFields.add(vectorColumn);
    return HoodieSchemaUtils.projectSchema(tableSchema, new ArrayList<>(projectedFields));
  }

  private List<Pair<String, FileSlice>> getLatestMergedFileSlicesForVectorBootstrap() {
    String latestInstant = dataMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
        .map(instant -> instant.requestedTime())
        .orElse("000");
    try (HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        engineContext, dataMetaClient, dataMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants())) {
      List<String> partitionPaths = metadata.getAllPartitionPaths();
      fsView.loadAllPartitions();
      List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
      partitionPaths.forEach(partition -> fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant)
          .forEach(fileSlice -> partitionFileSlicePairs.add(Pair.of(partition, fileSlice))));
      return partitionFileSlicePairs;
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to collect latest merged file slices for vector bootstrap", e);
    }
  }

  /**
   * Derive the generation ID from the latest commit instant on the data table timeline.
   * Falls back to wall-clock time if no commit instant is available.
   */
  private String deriveGenerationId() {
    return dataMetaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(instant -> {
          String ts = instant.requestedTime();
          // Use the numeric portion of the instant timestamp as generation ID
          try {
            return String.valueOf(Long.parseLong(ts.replaceAll("[^0-9]", "")));
          } catch (NumberFormatException e) {
            return String.valueOf(ts.hashCode() & 0x7FFFFFFF);
          }
        })
        .orElseGet(() -> String.valueOf(System.currentTimeMillis() / 1000L));
  }

  protected SparkRDDMetadataWriteClient getSparkWriteClient(Option<BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>>> writeClientOpt) {
    return ((SparkRDDMetadataWriteClient) writeClientOpt.orElse(getWriteClient()));
  }

  @Override
  public BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> initializeWriteClient() {
    return new SparkRDDMetadataWriteClient(engineContext, metadataWriteConfig, Option.empty());
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  protected void updateColumnsToIndexWithColStats(List<String> columnsToIndex) {
    new HoodieSparkIndexClient(dataWriteConfig, engineContext).createOrUpdateColumnStatsIndexDefinition(dataMetaClient, columnsToIndex);
  }
}
