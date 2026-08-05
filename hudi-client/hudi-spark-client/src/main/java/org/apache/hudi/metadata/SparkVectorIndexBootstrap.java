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

package org.apache.hudi.metadata;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.index.vector.PostingBlockBuilder;
import org.apache.hudi.common.index.vector.QuantizedVector;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorIndexBootstrapUtils;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Two-phase vector index bootstrap for Spark.
 *
 * <p>Phase 1: Sample vectors → train centroids with Spark ML KMeans. Training
 * still uses a bounded sample (FAISS, ScaNN, and Milvus all use sample-based
 * centroid training for IVF), but the iterative KMeans work stays distributed
 * instead of collecting the sample to the driver.
 *
 * <p>Phase 2: Broadcast centroids → single-pass {@code mapPartitions} that
 * assigns each vector to its nearest centroid, computes RaBitQ binary codes,
 * and emits posting metadata records with per-candidate locators. Cluster
 * statistics (populations, shard counts) are gathered via a lightweight
 * {@code countByKey} before the main encoding pass.
 *
 * <p>Compared to the previous driver-local KMeans approach:
 * <ul>
 *   <li>No reflection — direct {@link HoodieMetadataPayload} method calls</li>
 *   <li>Proper {@code unpersist()} in try/finally</li>
 *   <li>generation ordinal allocated from existing manifest keys</li>
 * </ul>
 */
@Slf4j
public final class SparkVectorIndexBootstrap {

  /** Hard cap for percentile-driven training samples. */
  private static final int MAX_PERCENTILE_TRAINING_SAMPLE = 10_000_000;

  /** Absolute minimum sample floor for production index quality. */
  private static final int MIN_TRAINING_SAMPLE = 1_000_000;

  /** Minimum sample multiplier per cluster. FAISS recommends 256×K minimum. */
  private static final int MIN_SAMPLE_PER_CLUSTER = 256;

  /** Lower bound for percentile-driven sampling: 0.5% of total vectors. */
  private static final double MIN_SAMPLE_PERCENT = 0.005d;

  /** Upper bound for percentile-driven sampling: 1% of total vectors. */
  private static final double MAX_SAMPLE_PERCENT = 0.01d;

  private SparkVectorIndexBootstrap() {
  }

  /**
   * Run the two-phase vector index bootstrap and return all MDT records.
   *
   * @param jsc            Spark context
   * @param vectorRows     RDD of (recordKey, partitionPath, fileId, vectorBytes) — already
   *                       read from base-table file slices
   * @param indexDef       the index definition
   * @param vectorType     resolved vector element type from the table schema
   * @param generation     vector generation ordinal
   * @param lastUpdatedTs  timestamp for MDT bookkeeping
   * @return all metadata records to write into the vector index MDT partition
   */
  public static HoodieData<HoodieRecord> bootstrap(JavaSparkContext jsc,
                                             JavaRDD<VectorRow> vectorRows,
                                             HoodieIndexDefinition indexDef,
                                             HoodieSchema.Vector.VectorElementType vectorType,
                                             int dimension,
                                             int generation,
                                             long lastUpdatedTs) {
    Map<String, String> options = indexDef.getIndexOptions();
    String indexName = indexDef.getIndexName();
    String vectorColumn = indexDef.getSourceFields().isEmpty() ? "" : indexDef.getSourceFields().get(0);
    VectorIndexOptions.ResolvedOptions resolvedOptions = VectorIndexOptions.resolve(options);
    int numClusters = resolvedOptions.numClusters;
    VectorDistanceMetric metric = resolvedOptions.metric;

    String quantizerType = resolvedOptions.quantizer.name();
    long quantizerSeed = resolvedOptions.rabitqSeed;
    int rabitqBits = resolvedOptions.rabitqBits;
    boolean assumeNormalized = resolvedOptions.assumeNormalized;
    boolean storeInMdt = true;
    int targetRowsPerShard = 100_000;
    int maxShardsPerCluster = 64;
    if ("IVF_RABITQ".equals(quantizerType) && rabitqBits > 1 && metric != VectorDistanceMetric.L2) {
      throw new IllegalArgumentException("Multibit RaBitQ currently supports L2 metric only. Requested: " + metric);
    }
    int quantizedCodeBytes = "IVF_RABITQ".equals(quantizerType)
        ? new RaBitQEncoder(dimension, rabitqBits, quantizerSeed, assumeNormalized).totalCodeBytes()
        : 0;
    int codeRowBytes = ((dimension + 63) / 64) * Long.BYTES;
    int targetBlockBytes = 64 * 1024;
    int vectorsPerBlock = PostingBlockBuilder.deriveVectorsPerBlock(targetBlockBytes, dimension, rabitqBits, 36,
        metric == VectorDistanceMetric.COSINE && !assumeNormalized);

    // ---- Phase 1: Sample → Train centroids with Spark ML KMeans ----

    // Invalid vector values are not indexable and must not fail the source-table operation.
    vectorRows = vectorRows.filter(row -> isValidVectorPayload(row.vectorBytes, dimension, vectorType));
    vectorRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
    try {
      long totalVectors = vectorRows.count();
      if (totalVectors == 0) {
        log.warn("Vector index bootstrap found zero vectors for {}", indexName);
        return HoodieJavaRDD.of(jsc.emptyRDD());
      }

      numClusters = (int) Math.min(numClusters, totalVectors);
      int percentileTarget = (int) Math.ceil(totalVectors * MAX_SAMPLE_PERCENT);
      int percentileFloor = (int) Math.ceil(totalVectors * MIN_SAMPLE_PERCENT);
      int percentileSample = Math.max(percentileFloor, Math.min(MAX_PERCENTILE_TRAINING_SAMPLE, percentileTarget));
      int targetSample = (int) Math.min(
          totalVectors,
          Math.max(
              MIN_TRAINING_SAMPLE,
              Math.max(MIN_SAMPLE_PER_CLUSTER * numClusters, percentileSample)));
      double sampleFraction = Math.min(1.0, (double) targetSample / totalVectors);
      log.info("Vector bootstrap: {} vectors, {} clusters, sampling {} vectors ({:.2f}%) for centroid training "
              + "[policy=max(1M, 256*K, min(10M, 0.5%-1% of N))]",
          totalVectors, numClusters, targetSample, sampleFraction * 100);

      BootstrapTrainingArtifacts trainingArtifacts = trainCentroidsWithSparkMl(
          jsc, vectorRows, dimension, numClusters, sampleFraction, totalVectors, vectorType, metric,
          resolvedOptions.maxIterations, quantizerSeed, indexName);
      float[][] centroids = trainingArtifacts.centroids;
      Object routingModel = trainingArtifacts.routingModel;
      if (routingModel == null) {
        throw new IllegalStateException(
            "Vector bootstrap did not produce the required two-level routing model for metric " + metric);
      }
      float[][] coarseCentroids = TwoLevelKMeansBootstrap$.MODULE$.coarseCentroidsForJava(routingModel);
      int[] leafOffsets = TwoLevelKMeansBootstrap$.MODULE$.leafOffsetsForJava(routingModel);
      double[][] centroidsDouble = toDoubleCentroids(centroids);
      int actualK = centroids.length;

      log.info("Centroid training complete: {} clusters for {}", actualK, indexName);

      // ---- Phase 2a: Assign clusters + gather stats (lightweight pass) ----

      Broadcast<float[][]> bCentroids = jsc.broadcast(centroids);
      Broadcast<VectorDistanceMetric> bMetric = jsc.broadcast(metric);
      Broadcast<Integer> bDimension = jsc.broadcast(dimension);
      Broadcast<HoodieSchema.Vector.VectorElementType> bVectorType = jsc.broadcast(vectorType);
      Broadcast<Object> bRoutingModel = jsc.broadcast(routingModel);
      final float assignmentExpandRatio = 1.1f;

      // Assign cluster IDs and collect (clusterId -> count) and (clusterId -> set of fileGroupIds)
      JavaPairRDD<Integer, VectorRow> assignedRows = vectorRows.mapToPair(row -> {
        float[] vector = toFloatArrayFromBytes(row.vectorBytes, bDimension.value(), bVectorType.value());
        int clusterId = TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
            bRoutingModel.value(), vector, assignmentExpandRatio);
        return new Tuple2<>(clusterId, row);
      });

      assignedRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
      try {
        // Unpersist the raw vectorRows — we now have assignedRows cached
        vectorRows.unpersist();

        Map<Integer, Long> clusterVectorCounts = assignedRows.countByKey();
        long largestClusterPopulation = clusterVectorCounts.values().stream()
            .mapToLong(Long::longValue)
            .max()
            .orElse(0L);
        int generationShardCount = computeShardCount(
            largestClusterPopulation, targetRowsPerShard, maxShardsPerCluster);

        log.info("Cluster stats collected: {} clusters, total {} vectors", clusterVectorCounts.size(), totalVectors);

        // ---- Phase 2b: Single-pass encode + emit canonical posting records ----

        Broadcast<Integer> bShardCount = jsc.broadcast(generationShardCount);
        boolean residualEncoding = true;

        JavaRDD<HoodieRecord> dataRecords;
        if (storeInMdt) {
          dataRecords = buildPostingRecords(
              assignedRows, bDimension, bVectorType, bCentroids,
              bShardCount, quantizerSeed, rabitqBits, assumeNormalized, residualEncoding,
              metric, vectorsPerBlock, generation, lastUpdatedTs, indexName);
        } else {
          dataRecords = jsc.emptyRDD();
        }

        // ---- Driver-side: emit singleton + cluster metadata records ----

        List<HoodieRecord> driverRecords = new ArrayList<>();
        // Centroids
        ByteBuffer clusterIds = ByteBuffer.allocate(actualK * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int clusterId = 0; clusterId < actualK; clusterId++) {
          clusterIds.putInt(clusterId);
        }
        clusterIds.flip();
        driverRecords.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(
            generation,
            0,
            clusterIds,
            VectorIndexBootstrapUtils.serializeCentroids(centroidsDouble, vectorType),
            ByteBuffer.allocate(actualK * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN),
            indexName));

        // Quantizer
        driverRecords.add(HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
            generation, 0, quantizerType, quantizerSeed, null, indexName));

        if (storeInMdt) {
          // Manifest
          driverRecords.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
              generation,
              String.valueOf(generation),
              "ACTIVE",
              dimension,
              dimension,
              codeRowBytes,
              rabitqBits,
              Math.max(0, rabitqBits - 1),
              actualK,
              VectorRoutingArtifacts.ROUTING_VERSION,
              VectorRoutingArtifacts.serializeFloatMatrix(coarseCentroids),
              VectorRoutingArtifacts.serializeIntArray(leafOffsets),
              assignmentExpandRatio,
              generationShardCount,
              Math.max(1, actualK),
              manifestMetricName(metric),
              assumeNormalized,
              residualEncoding,
              vectorColumn,
              targetBlockBytes,
              vectorsPerBlock,
              1,
              1,
              0.0d,
              0.0d,
              0.0d,
              0.0d,
              1,
              "",
              0,
              0,
              "",
              lastUpdatedTs,
              indexName));
          driverRecords.add(HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
              generation, indexName));

          // Cluster manifests
          for (Map.Entry<Integer, Long> entry : clusterVectorCounts.entrySet()) {
            int clusterId = entry.getKey();
            driverRecords.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
                generation,
                clusterId,
                // Exact rerank uses per-candidate posting locators, not cluster-wide file-group sets.
                // Keep this reserved metadata empty to avoid stale routing state and driver-side collection.
                Collections.emptySet(),
                entry.getValue(),
                lastUpdatedTs,
                indexName));
          }
        }

        JavaRDD<HoodieRecord> driverRdd = jsc.parallelize(driverRecords, 1);

        // Flat union — no linear chain
        return HoodieJavaRDD.of(jsc.union(driverRdd, dataRecords));
      } finally {
        assignedRows.unpersist();
      }
    } finally {
      // Safety net — unpersist vectorRows if it wasn't already unpersisted
      vectorRows.unpersist(false);
    }
  }

  // ---- Record builders (no reflection!) ----

  private static BootstrapTrainingArtifacts trainCentroidsWithSparkMl(JavaSparkContext jsc,
                                                                      JavaRDD<VectorRow> vectorRows,
                                                                      int dimension,
                                                                      int numClusters,
                                                                      double sampleFraction,
                                                                      long totalVectors,
                                                                      HoodieSchema.Vector.VectorElementType vectorType,
                                                                      VectorDistanceMetric metric,
                                                                      int maxIter,
                                                                      long seed,
                                                                      String indexName) {
    JavaRDD<VectorRow> trainingRows = vectorRows;
    boolean ownsTrainingRows = false;
    if (sampleFraction < 1.0d) {
      trainingRows = vectorRows.sample(false, sampleFraction, seed);
      trainingRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
      ownsTrainingRows = true;
    }

    try {
      long trainingCount = trainingRows.count();
      if (trainingCount < numClusters && sampleFraction < 1.0d) {
        log.warn("Vector bootstrap sample for {} produced {} rows for K={}; falling back to full RDD for centroid training",
            indexName, trainingCount, numClusters);
        trainingRows.unpersist(false);
        ownsTrainingRows = false;
        trainingRows = vectorRows;
        trainingCount = totalVectors;
      }

      int actualK = (int) Math.min(numClusters, trainingCount);
      if (metric == VectorDistanceMetric.DOT_PRODUCT) {
        log.warn("Spark ML KMeans does not support dot_product distance; falling back to bounded driver-local training for {}", indexName);
        List<double[]> sampleVectors = trainingRows
            .map(row -> toDoubleArray(row.vectorBytes, dimension, vectorType))
            .collect();
        return new BootstrapTrainingArtifacts(
            toFloatCentroids(VectorIndexBootstrapUtils.trainCentroids(
                sampleVectors, dimension, actualK, maxIter, metric)),
            null);
      }

      log.info("Training two-level KMeans centroids for {} on {} sampled vectors with K={}, maxIter={}",
          indexName, trainingCount, actualK, maxIter);
      SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
      JavaRDD<float[]> sampledVectors = trainingRows.map(
          row -> toFloatArrayFromBytes(row.vectorBytes, dimension, vectorType));
      Object model = TwoLevelKMeansBootstrap$.MODULE$.trainModelForJava(
          spark,
          sampledVectors.rdd(),
          actualK,
          Math.max(1, maxIter),
          seed);
      return new BootstrapTrainingArtifacts(
          TwoLevelKMeansBootstrap$.MODULE$.leafCentroidsForJava(model),
          model);
    } finally {
      if (ownsTrainingRows) {
        trainingRows.unpersist(false);
      }
    }
  }

  private static JavaRDD<HoodieRecord> buildPostingRecords(
      JavaPairRDD<Integer, VectorRow> assignedRows,
      Broadcast<Integer> bDimension,
      Broadcast<HoodieSchema.Vector.VectorElementType> bVectorType,
      Broadcast<float[][]> bCentroids,
      Broadcast<Integer> bShardCount,
      long quantizerSeed,
      int rabitqBits,
      boolean assumeNormalized,
      boolean residualEncoding,
      VectorDistanceMetric metric,
      int vectorsPerBlock,
      int generation,
      long lastUpdatedTs,
      String indexName) {
    boolean includeVectorNorm = metric == VectorDistanceMetric.COSINE && !assumeNormalized;
    JavaPairRDD<ClusterShardSortKey, EncodedPostingRow> encodedRows = assignedRows.mapToPair(entry -> {
      RaBitQEncoder encoder = new RaBitQEncoder(bDimension.value(), rabitqBits, quantizerSeed, assumeNormalized);
      int clusterId = entry._1;
      VectorRow row = entry._2;
      int shardId = computeShardId(row.recordKey, bShardCount.value());
      if (row.rowPosition < 0) {
        throw new IllegalStateException("Vector index bootstrap requires file-absolute rowPosition for record "
            + row.recordKey + "; enable parquet row-index extraction before packed MDT block emission");
      }

      float[] vector = toFloatArrayFromBytes(row.vectorBytes, bDimension.value(), bVectorType.value());
      QuantizedVector quantized;
      if (rabitqBits > 1 || residualEncoding) {
        float[] center = residualEncoding ? bCentroids.value()[clusterId] : null;
        quantized = encoder.encodeResidual(vector, center);
      } else {
        quantized = encoder.encode(vector);
      }

      int codeRowBytes = ((bDimension.value() + 63) / 64) * Long.BYTES;
      EncodedPostingRow encoded = new EncodedPostingRow(
          row.recordKey,
          row.fileId,
          row.partitionPath,
          row.baseInstantTime,
          row.rowPosition,
          VectorIndexBootstrapUtils.padToRow(quantized.getCode(), codeRowBytes),
          VectorIndexBootstrapUtils.splitExPlanes(quantized.getExtendedCode(), Math.max(0, rabitqBits - 1), bDimension.value(), codeRowBytes),
          quantized.getAdditiveFactor1() == null ? 0.0f : quantized.getAdditiveFactor1(),
          quantized.getRescaleFactor1() == null ? 0.0f : quantized.getRescaleFactor1(),
          quantized.getError1() == null ? 0.0f : quantized.getError1(),
          quantized.getAdditiveFactor() == null ? 0.0f : quantized.getAdditiveFactor(),
          quantized.getRescaleFactor() == null ? 0.0f : quantized.getRescaleFactor(),
          quantized.getScalar(),
          includeVectorNorm ? quantized.getVectorNorm() : null);
      return new Tuple2<>(new ClusterShardSortKey(clusterId, shardId, row.fileId, row.rowPosition, row.recordKey), encoded);
    });

    int shufflePartitions = Math.max(1, encodedRows.getNumPartitions());
    return encodedRows
        .repartitionAndSortWithinPartitions(new ClusterShardPartitioner(shufflePartitions))
        .mapPartitions(iterator -> {
          List<HoodieRecord> records = new ArrayList<>();
          ClusterShardSortKey currentKey = null;
          PostingBlockBuilder builder = null;
          int blockId = 0;
          int rowsInBlock = 0;
          while (iterator.hasNext()) {
            Tuple2<ClusterShardSortKey, EncodedPostingRow> entry = iterator.next();
            ClusterShardSortKey key = entry._1;
            EncodedPostingRow row = entry._2;
            if (currentKey == null || !currentKey.sameClusterShard(key)) {
              if (builder != null && rowsInBlock > 0) {
                records.add(createPostingBlockRecord(generation, currentKey, blockId, builder, indexName));
              }
              currentKey = key;
              builder = newPostingBlockBuilder(bDimension.value(), rabitqBits, includeVectorNorm);
              blockId = 0;
              rowsInBlock = 0;
            }

            builder.addRow(
                row.recordKey,
                row.signPlane,
                row.exPlanes,
                row.fAdd1,
                row.fRescale1,
                row.err1,
                row.fAddEx,
                row.fRescaleEx,
                row.residualNorm,
                row.vectorNorm,
                row.fileGroupId,
                row.baseInstantTime,
                row.partitionPath,
                row.rowPosition);
            rowsInBlock++;
            if (rowsInBlock == vectorsPerBlock) {
              records.add(createPostingBlockRecord(generation, currentKey, blockId++, builder, indexName));
              builder = newPostingBlockBuilder(bDimension.value(), rabitqBits, includeVectorNorm);
              rowsInBlock = 0;
            }
          }
          if (builder != null && rowsInBlock > 0) {
            records.add(createPostingBlockRecord(generation, currentKey, blockId, builder, indexName));
          }
          return records.iterator();
        });
  }

  // ---- Vector conversion ----

  /**
   * Convert raw vector bytes to float array. This handles FLOAT, DOUBLE, and INT8 element types
   * where the backing storage is a fixed-size byte array (the primary production path).
   */
  static float[] toFloatArrayFromBytes(byte[] vectorBytes, int dimension,
                                       HoodieSchema.Vector.VectorElementType elementType) {
    int expectedBytes = Math.multiplyExact(dimension, elementType.getElementSize());
    if (vectorBytes == null || vectorBytes.length != expectedBytes) {
      throw new IllegalArgumentException(
          "Expected vector payload of " + expectedBytes + " bytes, got "
              + (vectorBytes == null ? "null" : vectorBytes.length));
    }
    ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    float[] result = new float[dimension];
    switch (elementType) {
      case FLOAT:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getFloat();
        }
        validateFinite(result);
        return result;
      case DOUBLE:
        for (int i = 0; i < dimension; i++) {
          result[i] = (float) buffer.getDouble();
        }
        validateFinite(result);
        return result;
      case INT8:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.get();
        }
        return result;
      default:
        throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
    }
  }

  private static boolean isValidVectorPayload(
      byte[] vectorBytes,
      int dimension,
      HoodieSchema.Vector.VectorElementType elementType) {
    try {
      toFloatArrayFromBytes(vectorBytes, dimension, elementType);
      return true;
    } catch (IllegalArgumentException exception) {
      return false;
    }
  }

  private static void validateFinite(float[] vector) {
    for (int dimension = 0; dimension < vector.length; dimension++) {
      if (!Float.isFinite(vector[dimension])) {
        throw new IllegalArgumentException(
            "Vector contains a non-finite value at dimension " + dimension);
      }
    }
  }

  /**
   * Convert raw vector bytes to double array for centroid training.
   */
  static double[] toDoubleArray(byte[] vectorBytes, int dimension,
                                HoodieSchema.Vector.VectorElementType elementType) {
    ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    double[] result = new double[dimension];
    switch (elementType) {
      case FLOAT:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getFloat();
        }
        return result;
      case DOUBLE:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getDouble();
        }
        return result;
      case INT8:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.get();
        }
        return result;
      default:
        throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
    }
  }

  // ---- Centroid math ----

  static int findNearestCentroid(float[] vector, float[][] centroids, VectorDistanceMetric metric) {
    int best = 0;
    float bestDist = metric.compute(vector, centroids[0]);
    for (int i = 1; i < centroids.length; i++) {
      float dist = metric.compute(vector, centroids[i]);
      if (dist < bestDist) {
        bestDist = dist;
        best = i;
      }
    }
    return best;
  }

  private static float[] subtract(float[] left, float[] right) {
    if (left.length != right.length) {
      throw new IllegalArgumentException("Vector length mismatch: " + left.length + " != " + right.length);
    }
    float[] residual = new float[left.length];
    for (int i = 0; i < left.length; i++) {
      residual[i] = left[i] - right[i];
    }
    return residual;
  }

  // ---- Utilities ----

  private static String manifestMetricName(VectorDistanceMetric metric) {
    return metric == VectorDistanceMetric.DOT_PRODUCT ? "DOT" : metric.name();
  }

  private static PostingBlockBuilder newPostingBlockBuilder(int dimension, int rabitqBits, boolean includeVectorNorm) {
    return new PostingBlockBuilder(
        ((dimension + 63) / 64) * Long.BYTES,
        Math.max(0, rabitqBits - 1),
        includeVectorNorm);
  }

  private static HoodieRecord createPostingBlockRecord(int generation,
                                                       ClusterShardSortKey key,
                                                       int blockId,
                                                       PostingBlockBuilder builder,
                                                       String indexName) {
    return HoodieMetadataPayload.createVectorIndexPostingBlockRecord(
        generation,
        key.clusterId,
        key.shardId,
        blockId,
        builder.build(),
        indexName);
  }

  private static int computeShardCount(long clusterPopulation, int targetRowsPerShard, int maxShardsPerCluster) {
    if (clusterPopulation <= 0) {
      return 1;
    }
    long computed = (clusterPopulation + targetRowsPerShard - 1L) / targetRowsPerShard;
    return (int) Math.min(Math.max(1L, computed), maxShardsPerCluster);
  }

  static int computeShardId(String recordKey, int shardCount) {
    // Murmur3-style bit mixing for better distribution than String.hashCode()
    int h = recordKey.hashCode();
    h ^= (h >>> 16);
    h *= 0x85ebca6b;
    h ^= (h >>> 13);
    return Math.floorMod(h, Math.max(1, shardCount));
  }

  private static float[][] toFloatCentroids(double[][] centroids) {
    float[][] result = new float[centroids.length][];
    for (int i = 0; i < centroids.length; i++) {
      result[i] = new float[centroids[i].length];
      for (int j = 0; j < centroids[i].length; j++) {
        result[i][j] = (float) centroids[i][j];
      }
    }
    return result;
  }

  private static double[][] toDoubleCentroids(float[][] centroids) {
    double[][] result = new double[centroids.length][];
    for (int i = 0; i < centroids.length; i++) {
      result[i] = new double[centroids[i].length];
      for (int j = 0; j < centroids[i].length; j++) {
        result[i][j] = centroids[i][j];
      }
    }
    return result;
  }

  // ---- Data carrier ----

  /**
   * Lightweight serializable carrier for vector data read from base-table file slices.
   * Avoids the overhead of Spark Rows, DataFrames, and InternalRow wrappers.
   */
  public static final class VectorRow implements Serializable {
    private static final long serialVersionUID = 2L;

    final String recordKey;
    final String partitionPath;
    final String fileId;
    final String baseInstantTime;
    final byte[] vectorBytes;
    final long rowPosition;

    public VectorRow(String recordKey, String partitionPath, String fileId, String baseInstantTime, byte[] vectorBytes) {
      this(recordKey, partitionPath, fileId, baseInstantTime, vectorBytes, -1L);
    }

    public VectorRow(String recordKey, String partitionPath, String fileId, String baseInstantTime, byte[] vectorBytes, long rowPosition) {
      this.recordKey = recordKey;
      this.partitionPath = partitionPath;
      this.fileId = fileId;
      this.baseInstantTime = baseInstantTime;
      this.vectorBytes = vectorBytes;
      this.rowPosition = rowPosition;
    }
  }

  private static final class ClusterShardSortKey implements Comparable<ClusterShardSortKey>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int clusterId;
    private final int shardId;
    private final String fileGroupId;
    private final long rowPosition;
    private final String recordKey;

    private ClusterShardSortKey(int clusterId, int shardId, String fileGroupId, long rowPosition, String recordKey) {
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.fileGroupId = fileGroupId == null ? "" : fileGroupId;
      this.rowPosition = rowPosition;
      this.recordKey = recordKey == null ? "" : recordKey;
    }

    private boolean sameClusterShard(ClusterShardSortKey other) {
      return other != null && clusterId == other.clusterId && shardId == other.shardId;
    }

    @Override
    public int compareTo(ClusterShardSortKey other) {
      int comparison = Integer.compare(clusterId, other.clusterId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = Integer.compare(shardId, other.shardId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = fileGroupId.compareTo(other.fileGroupId);
      if (comparison != 0) {
        return comparison;
      }
      comparison = Long.compare(rowPosition, other.rowPosition);
      if (comparison != 0) {
        return comparison;
      }
      return recordKey.compareTo(other.recordKey);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ClusterShardSortKey)) {
        return false;
      }
      ClusterShardSortKey that = (ClusterShardSortKey) other;
      return clusterId == that.clusterId
          && shardId == that.shardId
          && rowPosition == that.rowPosition
          && fileGroupId.equals(that.fileGroupId)
          && recordKey.equals(that.recordKey);
    }

    @Override
    public int hashCode() {
      int result = clusterId;
      result = 31 * result + shardId;
      result = 31 * result + fileGroupId.hashCode();
      result = 31 * result + Long.hashCode(rowPosition);
      result = 31 * result + recordKey.hashCode();
      return result;
    }
  }

  private static final class ClusterShardPartitioner extends Partitioner {
    private static final long serialVersionUID = 1L;

    private final int numPartitions;

    private ClusterShardPartitioner(int numPartitions) {
      this.numPartitions = Math.max(1, numPartitions);
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      ClusterShardSortKey sortKey = (ClusterShardSortKey) key;
      int hash = 31 * sortKey.clusterId + sortKey.shardId;
      hash ^= (hash >>> 16);
      return Math.floorMod(hash, numPartitions);
    }
  }

  private static final class EncodedPostingRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String recordKey;
    private final String fileGroupId;
    private final String partitionPath;
    private final String baseInstantTime;
    private final long rowPosition;
    private final byte[] signPlane;
    private final byte[] exPlanes;
    private final float fAdd1;
    private final float fRescale1;
    private final float err1;
    private final float fAddEx;
    private final float fRescaleEx;
    private final float residualNorm;
    private final Float vectorNorm;

    private EncodedPostingRow(String recordKey,
                              String fileGroupId,
                              String partitionPath,
                              String baseInstantTime,
                              long rowPosition,
                              byte[] signPlane,
                              byte[] exPlanes,
                              float fAdd1,
                              float fRescale1,
                              float err1,
                              float fAddEx,
                              float fRescaleEx,
                              float residualNorm,
                              Float vectorNorm) {
      this.recordKey = recordKey;
      this.fileGroupId = fileGroupId;
      this.partitionPath = partitionPath;
      this.baseInstantTime = baseInstantTime;
      this.rowPosition = rowPosition;
      this.signPlane = signPlane;
      this.exPlanes = exPlanes;
      this.fAdd1 = fAdd1;
      this.fRescale1 = fRescale1;
      this.err1 = err1;
      this.fAddEx = fAddEx;
      this.fRescaleEx = fRescaleEx;
      this.residualNorm = residualNorm;
      this.vectorNorm = vectorNorm;
    }
  }

  private static final class BootstrapTrainingArtifacts implements Serializable {
    private static final long serialVersionUID = 1L;

    private final float[][] centroids;
    private final Object routingModel;

    private BootstrapTrainingArtifacts(float[][] centroids, Object routingModel) {
      this.centroids = centroids;
      this.routingModel = routingModel;
    }
  }
}
