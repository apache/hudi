/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata.index;

import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.SparkVectorIndexBootstrap;
import org.apache.hudi.metadata.SparkVectorIndexUpdater;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorMetadataRawKey;
import org.apache.hudi.metadata.index.vector.VectorIndexFileGroupUpdate;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * Spark implementation of {@link EngineIndexerSupport}.
 */
public class SparkIndexerSupport implements EngineIndexerSupport {
  private static final Logger LOG = LoggerFactory.getLogger(SparkIndexerSupport.class);

  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataWriteConfig;

  public SparkIndexerSupport(HoodieEngineContext engineContext,
                             HoodieWriteConfig dataWriteConfig) {
    this.engineContext = engineContext;
    this.dataWriteConfig = dataWriteConfig;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  public HoodieData<HoodieRecord> generateExpressionIndexRecords(
      List<FileInfoAndPartition> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism,
      HoodieSchema tableSchema,
      HoodieSchema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime,
      Option<PartitionStatsRecordsFunction> partitionRecordsFunctionOpt) {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieNotSupportedException("Hudi tables prior to version 8 do not support expression index.");
    }
    // SparkMetadataWriterUtils accepts the generic Function type, while the public engine
    // support API uses a named capability type to avoid exposing the full generic signature.
    Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>>
        partitionRecordsFunction = partitionRecordsFunctionOpt.map(function -> function);
    // Keep Spark-only computation metadata local to Spark support; common indexers only see
    // the final HoodieRecord stream for the metadata table.
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(
        filesToIndex, indexDefinition, metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        partitionRecordsFunction);
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (PARTITION_NAME_COLUMN_STATS.equals(indexDefinition.getIndexType()) && expressionIndexComputationMetadata.getPartitionStatRecordsOpt().isPresent()) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get());
    }
    return exprIndexRecords;
  }

  @Override
  public HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> loadExpressionIndexPartitionStats(
      HoodieTableMetaClient dataMetaClient,
      HoodieTableMetadata tableMetadata,
      HoodieCommitMetadata commitMetadata,
      HoodieIndexDefinition indexDefinition,
      String indexPartition,
      String instantTime) {
    return SparkMetadataWriterUtils.getExpressionIndexPartitionStatsForExistingFiles(
            commitMetadata, indexPartition, engineContext, tableMetadata, dataMetaClient, dataWriteConfig.getMetadataConfig(),
            Option.of(dataWriteConfig.getRecordMerger().getRecordType()), instantTime, dataWriteConfig)
        .flatMapValues(List::iterator);
  }

  @Override
  public HoodieData<HoodieRecord> generateVectorIndexRecords(
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient dataMetaClient,
      List<FileSliceAndPartition> fileSlices,
      HoodieSchema tableSchema,
      int generation) {
    try {
      String vectorColumn = indexDefinition.getSourceFields().get(0);
      HoodieSchemaField fieldSchema = HoodieSchemaUtils.getNestedField(tableSchema, vectorColumn)
          .orElseThrow(() -> new HoodieMetadataException("Vector column not found in table schema: " + vectorColumn))
          .getRight();
      HoodieSchema vectorSchema = fieldSchema.schema().getNonNullType();
      ValidationUtils.checkState(vectorSchema.getType() == HoodieSchemaType.VECTOR,
          "Vector index can only be bootstrapped from VECTOR columns: " + vectorColumn);

      HoodieSchema.Vector resolvedVectorType = (HoodieSchema.Vector) vectorSchema;
      int dimension = resolvedVectorType.getDimension();

      JavaRDD<SparkVectorIndexBootstrap.VectorRow> vectorRows = buildVectorRowsRDD(
          dataMetaClient, tableSchema, vectorColumn, indexDefinition.getIndexName(), fileSlices);

      JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
      long lastUpdatedTs = System.currentTimeMillis();
      return SparkVectorIndexBootstrap.bootstrap(
          jsc, vectorRows, indexDefinition, resolvedVectorType.getVectorElementType(),
          dimension, generation, lastUpdatedTs);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to bootstrap vector index records", e);
    }
  }

  @Override
  public HoodieData<HoodieRecord> generateVectorIndexUpdateRecords(
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient dataMetaClient,
      HoodieTableMetadata tableMetadata,
      List<VectorIndexFileGroupUpdate> fileGroupUpdates,
      HoodieSchema tableSchema,
      int generation,
      String instantTime) {
    if (fileGroupUpdates.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    String vectorColumn = indexDefinition.getSourceFields().get(0);
    HoodieSchema.Vector vectorSchema = (HoodieSchema.Vector) HoodieSchemaUtils
        .getNestedField(tableSchema, vectorColumn)
        .orElseThrow(() -> new HoodieMetadataException("Vector column not found: " + vectorColumn))
        .getRight().schema().getNonNullType();
    SparkVectorIndexUpdater.Artifacts artifacts = loadVectorArtifacts(
        tableMetadata, indexDefinition.getIndexName(), generation, vectorSchema);
    HoodieSchema requestedSchema = buildVectorBootstrapRequestedSchema(
        dataMetaClient, tableSchema, vectorColumn);
    ReaderContextFactory<InternalRow> contextFactory = engineContext.getReaderContextFactory(dataMetaClient);
    Option<InternalSchema> internalSchema = SerDeHelper.fromJson(dataWriteConfig.getInternalSchema());
    TypedProperties readerProps = dataWriteConfig.getProps();
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    JavaRDD<SparkVectorIndexUpdater.FileGroupRows> rows = jsc
        .parallelize(fileGroupUpdates, Math.max(1, fileGroupUpdates.size()))
        .map(update -> new SparkVectorIndexUpdater.FileGroupRows(
            update.getPreviousSlice()
                .map(slice -> readVectorRowsFromSlice(
                    dataMetaClient, contextFactory.getContext(), tableSchema, requestedSchema,
                    internalSchema, readerProps, vectorColumn, vectorSchema.getVectorElementType(),
                    update.getPartitionPath(), slice, instantTime, false))
                .orElse(Collections.emptyMap()),
            readVectorRowsFromSlice(
                dataMetaClient, contextFactory.getContext(), tableSchema, requestedSchema,
                internalSchema, readerProps, vectorColumn, vectorSchema.getVectorElementType(),
                update.getPartitionPath(), update.getCurrentSlice(),
                instantTime, true)));
    HoodieData<HoodieRecord> records = SparkVectorIndexUpdater.update(
        rows, artifacts, vectorSchema.getVectorElementType(), generation,
        instantTime, indexDefinition.getIndexName());
    return HoodieTableMetadataUtil.reduceByKeys(
        records, Math.max(1, fileGroupUpdates.size()), false);
  }

  private static Map<String, SparkVectorIndexBootstrap.VectorRow> readVectorRowsFromSlice(
      HoodieTableMetaClient dataMetaClient,
      HoodieReaderContext<InternalRow> readerContext,
      HoodieSchema tableSchema,
      HoodieSchema requestedSchema,
      Option<InternalSchema> internalSchema,
      TypedProperties readerProps,
      String vectorColumn,
      HoodieSchema.Vector.VectorElementType vectorType,
      String partitionPath,
      FileSlice fileSlice,
      String instantTime,
      boolean allowInflightInstants) {
    Map<String, SparkVectorIndexBootstrap.VectorRow> rows = new LinkedHashMap<>();
    try (HoodieFileGroupReader<InternalRow> reader = HoodieFileGroupReader.<InternalRow>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(dataMetaClient)
        .withLatestCommitTime(instantTime)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requestedSchema)
        .withInternalSchemaOpt(internalSchema)
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(partitionPath)
        .withShouldUseRecordPosition(true)
        .withAllowInflightInstants(allowInflightInstants)
        .withProps(readerProps)
        .build()) {
      Set<String> logRecordKeys = new HashSet<>();
      if (fileSlice.getLogFiles().findAny().isPresent()) {
        ClosableIterator<BufferedRecord<InternalRow>> logIterator = reader.getLogRecordsOnly();
        while (logIterator.hasNext()) {
          logRecordKeys.add(logIterator.next().getRecordKey());
        }
      }
      try (ClosableIterator<InternalRow> iterator = reader.getClosableIterator()) {
        while (iterator.hasNext()) {
          InternalRow record = iterator.next();
          Object vector = readerContext.getRecordContext().getValue(record, requestedSchema, vectorColumn);
          if (vector == null) {
            continue;
          }
          String recordKey = readerContext.getRecordContext().getRecordKey(record, requestedSchema);
          long rowPosition = logRecordKeys.contains(recordKey) ? -1L
              : readerContext.getRecordContext().extractRecordPosition(
                  record, requestedSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME, -1L);
          rows.put(recordKey, new SparkVectorIndexBootstrap.VectorRow(
              recordKey, partitionPath, fileSlice.getFileId(), fileSlice.getBaseInstantTime(),
              vectorBytes(vector, vectorType), rowPosition));
        }
      }
    } catch (Exception e) {
      throw new HoodieMetadataException(
          "Failed to read vector rows from file group " + fileSlice.getFileId(), e);
    }
    return rows;
  }

  private SparkVectorIndexUpdater.Artifacts loadVectorArtifacts(
      HoodieTableMetadata tableMetadata,
      String indexPartition,
      int generation,
      HoodieSchema.Vector vectorSchema) {
    List<VectorMetadataRawKey> keys = new ArrayList<>();
    keys.add(new VectorMetadataRawKey(VectorIndexMetadataKey.manifest(generation)));
    keys.add(new VectorMetadataRawKey(VectorIndexMetadataKey.quantizer(generation, 0)));
    Map<String, Object> metadata = new HashMap<>();
    tableMetadata.getRecordsByKeyPrefixes(HoodieListData.eager(keys), indexPartition, true)
        .collectAsList().forEach(record -> ((HoodieMetadataPayload) record.getData())
            .getVectorIndexMetadata().ifPresent(value -> metadata.put(record.getRecordKey(), value)));
    HoodieVectorIndexManifest manifest = requireArtifact(
        metadata, VectorIndexMetadataKey.manifest(generation), HoodieVectorIndexManifest.class);
    HoodieVectorIndexQuantizer quantizer = requireArtifact(
        metadata, VectorIndexMetadataKey.quantizer(generation, 0), HoodieVectorIndexQuantizer.class);

    List<VectorMetadataRawKey> centroidKeys = new ArrayList<>(manifest.getCentroidChunkCount());
    for (int chunk = 0; chunk < manifest.getCentroidChunkCount(); chunk++) {
      centroidKeys.add(new VectorMetadataRawKey(VectorIndexMetadataKey.centroids(generation, chunk)));
    }
    Map<String, Object> centroidMetadata = new HashMap<>();
    tableMetadata.getRecordsByKeyPrefixes(HoodieListData.eager(centroidKeys), indexPartition, true)
        .collectAsList().forEach(record -> ((HoodieMetadataPayload) record.getData())
            .getVectorIndexMetadata().ifPresent(
                value -> centroidMetadata.put(record.getRecordKey(), value)));
    List<HoodieVectorIndexCentroids> centroidChunks = new ArrayList<>(manifest.getCentroidChunkCount());
    for (int chunk = 0; chunk < manifest.getCentroidChunkCount(); chunk++) {
      centroidChunks.add(requireArtifact(
          centroidMetadata,
          VectorIndexMetadataKey.centroids(generation, chunk),
          HoodieVectorIndexCentroids.class));
    }

    if (manifest.getRoutingVersion() != 1
        || !Float.isFinite(manifest.getRoutingExpandRatio())
        || manifest.getRoutingExpandRatio() < 1.0f
        || manifest.getShardCount() <= 0) {
      throw new HoodieMetadataException("ACTIVE vector generation has unsupported routing or shard geometry");
    }
    float[][] centroids = decodeCentroids(
        centroidChunks, manifest.getNumClusters(), manifest.getDim(),
        vectorSchema.getVectorElementType());
    float[][] coarseCentroids = decodeFloatMatrix(
        manifest.getRoutingCoarseCentroids(), manifest.getDim(), "routing coarse centroids");
    int[] leafOffsets = decodeIntArray(manifest.getRoutingLeafOffsets(), "routing leaf offsets");
    Object routingModel;
    try {
      routingModel = TwoLevelKMeansBootstrap$.MODULE$.restoreModelForJava(
          coarseCentroids, centroids, leafOffsets);
    } catch (IllegalArgumentException exception) {
      throw new HoodieMetadataException("ACTIVE vector generation has invalid routing artifacts", exception);
    }
    String metricName = manifest.getMetric().toString();
    VectorDistanceMetric metric = "DOT".equalsIgnoreCase(metricName)
        ? VectorDistanceMetric.DOT_PRODUCT
        : VectorDistanceMetric.valueOf(metricName.toUpperCase());
    return new SparkVectorIndexUpdater.Artifacts(
        centroids,
        routingModel,
        manifest.getRoutingExpandRatio(),
        manifest.getShardCount(),
        metric,
        manifest.getDim(),
        manifest.getBitsTotal(),
        quantizer.getRandomSeed(),
        manifest.getAssumeNormalized(),
        manifest.getResidualEncoding());
  }

  private static <T> T requireArtifact(
      Map<String, Object> metadata, String key, Class<T> artifactClass) {
    Object value = metadata.get(key);
    if (!artifactClass.isInstance(value)) {
      throw new HoodieMetadataException("ACTIVE vector generation artifact is missing: " + key);
    }
    return artifactClass.cast(value);
  }

  private static float[][] decodeFloatMatrix(ByteBuffer source, int columns, String name) {
    ByteBuffer values = source.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int rowBytes = columns * Float.BYTES;
    if (columns <= 0 || values.remaining() == 0 || values.remaining() % rowBytes != 0) {
      throw new HoodieMetadataException("Invalid " + name + " payload size");
    }
    float[][] result = new float[values.remaining() / rowBytes][columns];
    for (int row = 0; row < result.length; row++) {
      for (int column = 0; column < columns; column++) {
        float value = values.getFloat();
        if (!Float.isFinite(value)) {
          throw new HoodieMetadataException(name + " contains a non-finite value");
        }
        result[row][column] = value;
      }
    }
    return result;
  }

  private static int[] decodeIntArray(ByteBuffer source, String name) {
    ByteBuffer values = source.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    if (values.remaining() == 0 || values.remaining() % Integer.BYTES != 0) {
      throw new HoodieMetadataException("Invalid " + name + " payload size");
    }
    int[] result = new int[values.remaining() / Integer.BYTES];
    for (int index = 0; index < result.length; index++) {
      result[index] = values.getInt();
    }
    return result;
  }

  private static float[][] decodeCentroids(
      List<HoodieVectorIndexCentroids> chunks,
      int numClusters,
      int dimension,
      HoodieSchema.Vector.VectorElementType elementType) {
    float[][] centroids = new float[numClusters][dimension];
    boolean[] populated = new boolean[numClusters];
    for (HoodieVectorIndexCentroids chunk : chunks) {
      ByteBuffer clusterIds = chunk.getClusterIds().duplicate().order(ByteOrder.LITTLE_ENDIAN);
      ByteBuffer values = chunk.getCentroidBytes().duplicate().order(ByteOrder.LITTLE_ENDIAN);
      while (clusterIds.remaining() >= Integer.BYTES) {
        int clusterId = clusterIds.getInt();
        if (clusterId < 0 || clusterId >= numClusters || populated[clusterId]) {
          throw new HoodieMetadataException("Invalid or duplicate vector centroid id " + clusterId);
        }
        for (int index = 0; index < dimension; index++) {
          if (elementType == HoodieSchema.Vector.VectorElementType.DOUBLE) {
            centroids[clusterId][index] = (float) values.getDouble();
          } else if (elementType == HoodieSchema.Vector.VectorElementType.INT8) {
            centroids[clusterId][index] = values.get();
          } else {
            centroids[clusterId][index] = values.getFloat();
          }
        }
        populated[clusterId] = true;
      }
      if (clusterIds.hasRemaining() || values.hasRemaining()) {
        throw new HoodieMetadataException("Malformed vector centroid chunk");
      }
    }
    for (int clusterId = 0; clusterId < numClusters; clusterId++) {
      if (!populated[clusterId]) {
        throw new HoodieMetadataException("ACTIVE vector generation is missing centroid " + clusterId);
      }
    }
    return centroids;
  }

  private static byte[] vectorBytes(
      Object vectorValue, HoodieSchema.Vector.VectorElementType vectorType) {
    if (vectorValue instanceof byte[]) {
      return (byte[]) vectorValue;
    }
    if (vectorValue instanceof org.apache.spark.sql.catalyst.util.ArrayData) {
      org.apache.spark.sql.catalyst.util.ArrayData array =
          (org.apache.spark.sql.catalyst.util.ArrayData) vectorValue;
      int elementBytes = vectorType.getElementSize();
      ByteBuffer buffer = ByteBuffer.allocate(array.numElements() * elementBytes)
          .order(ByteOrder.LITTLE_ENDIAN);
      for (int index = 0; index < array.numElements(); index++) {
        if (vectorType == HoodieSchema.Vector.VectorElementType.DOUBLE) {
          buffer.putDouble(array.getDouble(index));
        } else if (vectorType == HoodieSchema.Vector.VectorElementType.INT8) {
          buffer.put(array.getByte(index));
        } else {
          buffer.putFloat(array.getFloat(index));
        }
      }
      return buffer.array();
    }
    throw new HoodieMetadataException(
        "Expected byte[] or ArrayData for VECTOR column, got " + vectorValue.getClass().getName());
  }

  /**
   * Reads all vectors from the provided latest base-table file slices into a lightweight RDD of
   * (recordKey, partitionPath, fileId, baseInstantTime, vectorBytes, rowPosition) tuples,
   * without DataFrame/UDF overhead.
   */
  private JavaRDD<SparkVectorIndexBootstrap.VectorRow> buildVectorRowsRDD(
      HoodieTableMetaClient dataMetaClient,
      HoodieSchema tableSchema,
      String vectorColumn,
      String indexName,
      List<FileSliceAndPartition> fileSlices) {
    JavaSparkContext jsc = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext();
    if (fileSlices.isEmpty()) {
      LOG.warn("Vector index bootstrap found no latest file slices for {}", dataMetaClient.getBasePath());
      return jsc.emptyRDD();
    }
    LOG.info("Vector index bootstrap discovered {} latest file slices for {}", fileSlices.size(), indexName);

    Option<String> instantTime = dataMetaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(instant -> instant.requestedTime());
    if (!instantTime.isPresent()) {
      return jsc.emptyRDD();
    }

    HoodieSchema requestedSchema = buildVectorBootstrapRequestedSchema(dataMetaClient, tableSchema, vectorColumn);
    ReaderContextFactory<InternalRow> readerContextFactory = engineContext.getReaderContextFactory(dataMetaClient);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(dataWriteConfig.getInternalSchema());
    String latestCommitTime = instantTime.get();
    int parallelism = fileSlices.size();

    return jsc.parallelize(fileSlices, parallelism)
        .flatMap(fileSliceAndPartition -> {
          String partitionPath = fileSliceAndPartition.getPartitionPath();
          FileSlice fileSlice = fileSliceAndPartition.getFileSlice();
          HoodieReaderContext<InternalRow> readerContext = readerContextFactory.getContext();
          try (HoodieFileGroupReader<InternalRow> fileGroupReader = HoodieFileGroupReader.<InternalRow>builder()
              .withReaderContext(readerContext)
              .withHoodieTableMetaClient(dataMetaClient)
              .withLatestCommitTime(latestCommitTime)
              .withDataSchema(tableSchema)
              .withRequestedSchema(requestedSchema)
              .withInternalSchemaOpt(internalSchemaOption)
              .withBaseFileOption(fileSlice.getBaseFile())
              .withLogFiles(fileSlice.getLogFiles())
              .withPartitionPath(partitionPath)
              .withShouldUseRecordPosition(true)
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
                // VECTOR columns may be returned as float arrays by the reader context when the schema
                // resolves the column as ArrayType(FloatType) instead of BinaryType.
                org.apache.spark.sql.catalyst.util.ArrayData arrayData =
                    (org.apache.spark.sql.catalyst.util.ArrayData) vectorValue;
                int numElements = arrayData.numElements();
                ByteBuffer buf = ByteBuffer.allocate(numElements * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < numElements; i++) {
                  buf.putFloat(arrayData.getFloat(i));
                }
                vectorBytes = buf.array();
              } else {
                throw new HoodieMetadataException(
                    "Expected byte[] or ArrayData for VECTOR column, got: " + vectorValue.getClass().getName());
              }
              long rowPosition = readerContext.getRecordContext().extractRecordPosition(
                  record, requestedSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME, -1L);
              rows.add(new SparkVectorIndexBootstrap.VectorRow(
                  recordKey, partitionPath, fileSlice.getFileId(),
                  fileSlice.getBaseInstantTime(), vectorBytes, rowPosition));
            }
            return rows.iterator();
          }
        });
  }

  private HoodieSchema buildVectorBootstrapRequestedSchema(
      HoodieTableMetaClient dataMetaClient, HoodieSchema tableSchema, String vectorColumn) {
    LinkedHashSet<String> projectedFields = new LinkedHashSet<>();
    if (dataMetaClient.getTableConfig().populateMetaFields()) {
      projectedFields.add(RECORD_KEY_METADATA_FIELD);
    } else {
      projectedFields.addAll(Arrays.asList(dataMetaClient.getTableConfig().getRecordKeyFields()
          .orElseThrow(() -> new HoodieMetadataException("Cannot bootstrap vector index without record key fields"))));
    }
    projectedFields.add(vectorColumn);
    HoodieSchema projectedSchema = HoodieSchemaUtils.projectSchema(tableSchema, new ArrayList<>(projectedFields));
    List<HoodieSchemaField> fields = projectedSchema.getFields().stream()
        .map(field -> field.withName(field.name()))
        .collect(Collectors.toCollection(ArrayList::new));
    if (!projectedSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent()) {
      fields.add(HoodieSchemaField.of(
          ROW_INDEX_TEMPORARY_COLUMN_NAME, HoodieSchema.create(HoodieSchemaType.LONG),
          "Hudi metadata field: " + ROW_INDEX_TEMPORARY_COLUMN_NAME, -1L));
    }
    return HoodieSchema.createRecord(
        projectedSchema.getName(),
        projectedSchema.getDoc().orElse(null),
        projectedSchema.getNamespace().orElse(null),
        false,
        fields);
  }
}
