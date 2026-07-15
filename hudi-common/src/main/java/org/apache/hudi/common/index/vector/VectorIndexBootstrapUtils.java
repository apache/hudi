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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Shared Phase 1 bootstrap helper for vector-index metadata generation.
 *
 * <p>This is intentionally engine-agnostic so Spark can reuse it while tests can
 * validate the clustering and MDT-record generation logic without spinning up a
 * full table/index bootstrap flow.
 */
public final class VectorIndexBootstrapUtils {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexBootstrapUtils.class);
  private static final float DEGENERATE_RESIDUAL_EPS = 1.0e-12f;
  private static final int DEFAULT_BOOTSTRAP_GENERATION = 1;

  private VectorIndexBootstrapUtils() {
  }

  /**
   * Train KMeans centroids from a sample of vectors.
   *
   * <p>This is the engine-agnostic centroid training entry point. At scale, callers
   * should sample ~256*K to 1024*K vectors from the full dataset and pass only the
   * sample here. Training on more provides negligible recall improvement for IVF.
   *
   * @param sampleVectors sampled vectors as double arrays (each of length {@code dimension})
   * @param options       vector index options (dimension, num_clusters, max_iter, metric)
   * @return trained centroids as double[K][dimension]
   */
  public static double[][] trainCentroids(List<double[]> sampleVectors, Map<String, String> options) {
    checkArgument(sampleVectors != null && !sampleVectors.isEmpty(),
        "At least one sample vector is required for centroid training");
    int dimension = VectorIndexOptions.getDimension(options);
    int requestedClusters = Math.max(1, VectorIndexOptions.getNumClusters(options));
    int numClusters = Math.min(requestedClusters, sampleVectors.size());
    int maxIter = Math.max(1, VectorIndexOptions.getMaxIter(options));
    VectorDistanceMetric metric = VectorIndexOptions.getMetric(options);

    for (int i = 0; i < sampleVectors.size(); i++) {
      checkArgument(sampleVectors.get(i).length == dimension,
          String.format("Sample vector %d has dimension %d, expected %d", i, sampleVectors.get(i).length, dimension));
    }

    double[][] centroids = initializeCentroidsFromVectors(sampleVectors, numClusters, dimension);
    int[] assignments = new int[sampleVectors.size()];
    for (int i = 0; i < assignments.length; i++) {
      assignments[i] = -1;
    }

    for (int iteration = 0; iteration < maxIter; iteration++) {
      boolean changed = assignVectorsToClosestCentroids(sampleVectors, centroids, assignments, metric);
      centroids = recomputeCentroidsFromVectors(sampleVectors, assignments, centroids, dimension);
      if (!changed) {
        break;
      }
    }
    return centroids;
  }

  public static BootstrapResult buildBootstrapResult(List<VectorDocument> documents,
                                                     String mdtPartition,
                                                     Map<String, String> options,
                                                     HoodieSchema.Vector.VectorElementType elementType) {
    return buildBootstrapResult(documents, mdtPartition, DEFAULT_BOOTSTRAP_GENERATION, options, elementType);
  }

  public static BootstrapResult buildBootstrapResult(List<VectorDocument> documents,
                                                     String mdtPartition,
                                                     int generation,
                                                     Map<String, String> options,
                                                     HoodieSchema.Vector.VectorElementType elementType) {
    checkArgument(documents != null && !documents.isEmpty(), "Vector index bootstrap requires at least one vector");

    int dimension = VectorIndexOptions.getDimension(options);
    VectorDistanceMetric metric = VectorIndexOptions.getMetric(options);
    int rabitqBits = VectorIndexOptions.getRaBitQBits(options);
    long seed = VectorIndexOptions.getRaBitQSeed(options);
    boolean assumeNormalized = VectorIndexOptions.isRaBitQAssumeNormalized(options);
    boolean residualEncoding = VectorIndexOptions.isRaBitQResidualEncoding(options);
    int shardCount = resolveShardCount(options);

    validateDocuments(documents, dimension);

    List<double[]> rawVectors = new ArrayList<>(documents.size());
    for (VectorDocument doc : documents) {
      rawVectors.add(toDoubleArray(doc.getVector()));
    }
    double[][] centroids = trainCentroids(rawVectors, options);
    float[][] floatCentroids = toFloatCentroids(centroids);

    int[] assignments = new int[documents.size()];
    Map<String, Integer> assignmentMap = new LinkedHashMap<>();
    PostingEntry[] entries = new PostingEntry[documents.size()];
    RaBitQEncoder encoder = new RaBitQEncoder(
        dimension,
        rabitqBits,
        seed,
        assumeNormalized);
    int degenerateRows = 0;
    for (int i = 0; i < documents.size(); i++) {
      VectorDocument document = documents.get(i);
      int clusterId = findClosestCentroid(document.getVector(), floatCentroids, metric);
      assignments[i] = clusterId;
      assignmentMap.put(document.getRecordKey(), clusterId);
      VectorQuantizer.QuantizedVector quantized = encoder.encodeResidual(document.getVector(), floatCentroids[clusterId]);
      if (quantized.scalar <= DEGENERATE_RESIDUAL_EPS) {
        degenerateRows++;
      }
      entries[i] = new PostingEntry(
          document,
          clusterId,
          shardOf(document.getRecordKey(), shardCount),
          quantized);
    }

    Arrays.sort(entries, Comparator
        .comparingInt((PostingEntry entry) -> entry.clusterId)
        .thenComparingInt(entry -> entry.shardId)
        .thenComparing(entry -> entry.document.getFileGroupId())
        .thenComparingLong(entry -> entry.document.getRowPosition())
        .thenComparing(entry -> entry.document.getRecordKey()));

    int targetBlockBytes = VectorIndexOptions.getRaBitQPostingTargetBlockBytes(options);
    int vectorsPerBlock = PostingBlockBuilder.deriveVectorsPerBlock(
        targetBlockBytes, dimension, rabitqBits, avgKeyLen(documents), false);
    int codeRowBytes = ((dimension + 63) / 64) * Long.BYTES;
    int exPlanes = Math.max(0, rabitqBits - 1);
    List<HoodieRecord> records = new ArrayList<>();
    Map<Integer, Long> liveCounts = new HashMap<>();
    PostingBlockBuilder builder = new PostingBlockBuilder(codeRowBytes, exPlanes);
    int curCluster = -1;
    int curShard = -1;
    int blockId = 0;

    for (PostingEntry entry : entries) {
      if (entry.clusterId != curCluster || entry.shardId != curShard) {
        blockId = flushIfNonEmpty(builder, records, mdtPartition, generation, curCluster, curShard, blockId, true);
        curCluster = entry.clusterId;
        curShard = entry.shardId;
        blockId = 0;
      } else if (builder.rowCount() >= vectorsPerBlock) {
        blockId = flushIfNonEmpty(builder, records, mdtPartition, generation, curCluster, curShard, blockId, false);
      }

      VectorQuantizer.QuantizedVector quantized = entry.quantized;
      builder.addRow(
          entry.document.getRecordKey(),
          padToRow(quantized.code, codeRowBytes),
          splitExPlanes(quantized.extendedCode, exPlanes, dimension, codeRowBytes),
          quantized.additiveFactor1 == null ? 0.0f : quantized.additiveFactor1,
          quantized.rescaleFactor1 == null ? 0.0f : quantized.rescaleFactor1,
          quantized.error1 == null ? 0.0f : quantized.error1,
          quantized.additiveFactor == null ? 0.0f : quantized.additiveFactor,
          quantized.rescaleFactor == null ? 0.0f : quantized.rescaleFactor,
          quantized.scalar,
          entry.document.getFileGroupId(),
          entry.document.getBaseInstantTime(),
          entry.document.getPartitionPath(),
          entry.document.getRowPosition());
      liveCounts.merge(entry.clusterId, 1L, Long::sum);
    }
    flushIfNonEmpty(builder, records, mdtPartition, generation, curCluster, curShard, blockId, true);
    int blockRecords = records.size();

    records.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
        generation,
        String.valueOf(generation),
        "ACTIVE",
        dimension,
        dimension,
        codeRowBytes,
        rabitqBits,
        exPlanes,
        floatCentroids.length,
        shardCount,
        manifestMetricName(metric),
        assumeNormalized,
        residualEncoding,
        "",
        targetBlockBytes,
        vectorsPerBlock,
        0,
        0,
        0L,
        0L,
        mdtPartition));
    records.add(HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        generation,
        0,
        VectorIndexOptions.getQuantizer(options),
        seed,
        null,
        mdtPartition));
    records.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(
        generation,
        0L,
        0,
        clusterIdsBuffer(floatCentroids.length),
        serializeCentroids(centroids, elementType),
        ByteBuffer.allocate(floatCentroids.length * Float.BYTES).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER),
        mdtPartition));
    liveCounts.forEach((clusterId, count) ->
        records.add(HoodieMetadataPayload.createVectorIndexClusterStatsRecord(
            generation, clusterId, count, 0L, 0L, mdtPartition)));
    LOG.info("[vector_bootstrap] vectors={} clusters={} vectorsPerBlock={} blocks={} degenerateRows={}",
        documents.size(), liveCounts.size(), vectorsPerBlock, blockRecords, degenerateRows);

    return new BootstrapResult(floatCentroids, assignmentMap, records);
  }

  public static List<HoodieRecord> buildBootstrapRecords(List<VectorDocument> documents,
                                                         String partitionPath,
                                                         Map<String, String> options,
                                                         HoodieSchema.Vector.VectorElementType elementType) {
    return buildBootstrapResult(documents, partitionPath, options, elementType).getRecords();
  }

  private static void validateDocuments(List<VectorDocument> documents, int dimension) {
    documents.forEach(document -> {
      checkArgument(document.getRecordKey() != null && !document.getRecordKey().isEmpty(),
          "Vector index bootstrap requires a non-empty record key");
      checkArgument(document.getVector() != null, "Vector index bootstrap requires non-null vectors");
      checkArgument(document.getVector().length == dimension,
          String.format("Vector dimension mismatch for record %s: expected %s but got %s",
              document.getRecordKey(), dimension, document.getVector().length));
      checkArgument(document.getFileGroupId() != null && !document.getFileGroupId().isEmpty(),
          "Vector index bootstrap requires fileGroupId for record " + document.getRecordKey());
      checkArgument(document.getPartitionPath() != null,
          "Vector index bootstrap requires partitionPath for record " + document.getRecordKey());
      checkArgument(document.getBaseInstantTime() != null && !document.getBaseInstantTime().isEmpty(),
          "Vector index bootstrap requires baseInstantTime for record " + document.getRecordKey());
      checkArgument(document.getRowPosition() >= 0,
          "Vector index bootstrap requires rowPosition for record " + document.getRecordKey());
    });
  }

  private static double[][] initializeCentroidsFromVectors(List<double[]> vectors, int numClusters, int dimension) {
    double[][] centroids = new double[numClusters][dimension];
    for (int clusterId = 0; clusterId < numClusters; clusterId++) {
      int sourceIndex = (int) (((long) clusterId * vectors.size()) / numClusters);
      System.arraycopy(vectors.get(sourceIndex), 0, centroids[clusterId], 0, dimension);
    }
    return centroids;
  }

  private static boolean assignVectorsToClosestCentroids(List<double[]> vectors,
                                                         double[][] centroids,
                                                         int[] assignments,
                                                         VectorDistanceMetric metric) {
    boolean changed = false;
    for (int i = 0; i < vectors.size(); i++) {
      int bestCluster = findClosestCentroid(vectors.get(i), centroids, metric);
      if (assignments[i] != bestCluster) {
        assignments[i] = bestCluster;
        changed = true;
      }
    }
    return changed;
  }

  private static double[][] recomputeCentroidsFromVectors(List<double[]> vectors,
                                                          int[] assignments,
                                                          double[][] previousCentroids,
                                                          int dimension) {
    double[][] sums = new double[previousCentroids.length][dimension];
    int[] counts = new int[previousCentroids.length];

    for (int i = 0; i < vectors.size(); i++) {
      int clusterId = assignments[i];
      counts[clusterId]++;
      double[] vector = vectors.get(i);
      for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
        sums[clusterId][dimensionIndex] += vector[dimensionIndex];
      }
    }

    double[][] centroids = new double[previousCentroids.length][dimension];
    for (int clusterId = 0; clusterId < previousCentroids.length; clusterId++) {
      if (counts[clusterId] == 0) {
        System.arraycopy(previousCentroids[clusterId], 0, centroids[clusterId], 0, dimension);
      } else {
        for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
          centroids[clusterId][dimensionIndex] = sums[clusterId][dimensionIndex] / counts[clusterId];
        }
      }
    }

    return centroids;
  }

  public static ByteBuffer serializeCentroids(double[][] centroids, HoodieSchema.Vector.VectorElementType elementType) {
    int dimension = centroids[0].length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(centroids.length * dimension * elementType.getElementSize())
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (double[] centroid : centroids) {
      for (double value : centroid) {
        switch (elementType) {
          case FLOAT:
            byteBuffer.putFloat((float) value);
            break;
          case DOUBLE:
            byteBuffer.putDouble(value);
            break;
          case INT8:
            byteBuffer.put((byte) Math.round(value));
            break;
          default:
            throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
        }
      }
    }
    byteBuffer.flip();
    return byteBuffer;
  }

  public static int findClosestCentroid(double[] vector, double[][] centroids, VectorDistanceMetric metric) {
    checkArgument(centroids != null && centroids.length > 0, "At least one centroid is required");
    int bestCluster = 0;
    double bestDistance = compute(metric, vector, centroids[0]);
    for (int clusterId = 1; clusterId < centroids.length; clusterId++) {
      double distance = compute(metric, vector, centroids[clusterId]);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestCluster = clusterId;
      }
    }
    return bestCluster;
  }

  public static int findClosestCentroid(float[] vector, float[][] centroids, VectorDistanceMetric metric) {
    checkArgument(centroids != null && centroids.length > 0, "At least one centroid is required");
    int bestCluster = 0;
    float bestDistance = metric.compute(vector, centroids[0]);
    for (int clusterId = 1; clusterId < centroids.length; clusterId++) {
      float distance = metric.compute(vector, centroids[clusterId]);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestCluster = clusterId;
      }
    }
    return bestCluster;
  }

  private static double compute(VectorDistanceMetric metric, double[] a, double[] b) {
    checkArgument(a.length == b.length, "Vector dimension mismatch: " + a.length + " vs " + b.length);
    switch (metric) {
      case COSINE:
        double dot = 0d;
        double normA = 0d;
        double normB = 0d;
        for (int i = 0; i < a.length; i++) {
          dot += a[i] * b[i];
          normA += a[i] * a[i];
          normB += b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0d ? 1d : 1d - dot / denom;
      case L2:
        double sum = 0d;
        for (int i = 0; i < a.length; i++) {
          double delta = a[i] - b[i];
          sum += delta * delta;
        }
        return Math.sqrt(sum);
      case DOT_PRODUCT:
        double product = 0d;
        for (int i = 0; i < a.length; i++) {
          product += a[i] * b[i];
        }
        return -product;
      default:
        throw new IllegalArgumentException("Unsupported vector metric: " + metric);
    }
  }

  private static float[][] toFloatCentroids(double[][] centroids) {
    float[][] converted = new float[centroids.length][];
    for (int i = 0; i < centroids.length; i++) {
      converted[i] = new float[centroids[i].length];
      for (int j = 0; j < centroids[i].length; j++) {
        converted[i][j] = (float) centroids[i][j];
      }
    }
    return converted;
  }

  public static byte[] padToRow(byte[] packedCode, int codeRowBytes) {
    byte[] row = new byte[codeRowBytes];
    if (packedCode != null) {
      System.arraycopy(packedCode, 0, row, 0, Math.min(packedCode.length, row.length));
    }
    return row;
  }

  public static byte[] splitExPlanes(byte[] levelPacked, int exBits, int dimension, int codeRowBytes) {
    byte[] planes = new byte[exBits * codeRowBytes];
    if (exBits == 0 || levelPacked == null) {
      return planes;
    }
    for (int dim = 0; dim < dimension; dim++) {
      int base = dim * exBits;
      for (int bit = 0; bit < exBits; bit++) {
        int absoluteBit = base + bit;
        if ((levelPacked[absoluteBit >> 3] & (1 << (absoluteBit & 7))) != 0) {
          int plane = exBits - 1 - bit;
          planes[plane * codeRowBytes + (dim >> 3)] |= (byte) (1 << (dim & 7));
        }
      }
    }
    return planes;
  }

  private static int flushIfNonEmpty(PostingBlockBuilder builder,
                                     List<HoodieRecord> out,
                                     String mdtPartition,
                                     int generation,
                                     int clusterId,
                                     int shardId,
                                     int blockId,
                                     boolean groupBoundary) {
    if (builder.rowCount() == 0) {
      return groupBoundary ? 0 : blockId;
    }
    out.add(HoodieMetadataPayload.createVectorIndexPostingBlockRecord(
        generation, clusterId, shardId, blockId, builder.build(), mdtPartition));
    builder.reset();
    return blockId + 1;
  }

  private static int resolveShardCount(Map<String, String> options) {
    int maxShards = Math.max(1, VectorIndexOptions.getRaBitQPostingMaxShardsPerCluster(options));
    return Integer.highestOneBit(maxShards);
  }

  private static int shardOf(String recordKey, int shardCount) {
    int hash = recordKey.hashCode();
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    return hash & (shardCount - 1);
  }

  private static int avgKeyLen(List<VectorDocument> documents) {
    long total = 0L;
    for (VectorDocument document : documents) {
      total += document.getRecordKey().getBytes(StandardCharsets.UTF_8).length;
    }
    return (int) Math.max(1L, total / documents.size());
  }

  private static double[] toDoubleArray(float[] vector) {
    double[] converted = new double[vector.length];
    for (int i = 0; i < vector.length; i++) {
      converted[i] = vector[i];
    }
    return converted;
  }

  private static ByteBuffer clusterIdsBuffer(int count) {
    ByteBuffer clusterIds = ByteBuffer.allocate(count * Integer.BYTES).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (int clusterId = 0; clusterId < count; clusterId++) {
      clusterIds.putInt(clusterId);
    }
    clusterIds.flip();
    return clusterIds;
  }

  private static String manifestMetricName(VectorDistanceMetric metric) {
    return metric == VectorDistanceMetric.DOT_PRODUCT ? "DOT" : metric.name();
  }

  public static final class VectorDocument {
    private final String recordKey;
    private final float[] vector;
    private final String fileGroupId;
    private final String partitionPath;
    private final String baseInstantTime;
    private final long rowPosition;

    public VectorDocument(String recordKey,
                          float[] vector,
                          String fileGroupId,
                          String partitionPath,
                          String baseInstantTime,
                          long rowPosition) {
      this.recordKey = recordKey;
      this.vector = vector;
      this.fileGroupId = fileGroupId;
      this.partitionPath = partitionPath;
      this.baseInstantTime = baseInstantTime;
      this.rowPosition = rowPosition;
    }

    public String getRecordKey() {
      return recordKey;
    }

    public float[] getVector() {
      return vector;
    }

    public String getFileGroupId() {
      return fileGroupId;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public String getBaseInstantTime() {
      return baseInstantTime;
    }

    public long getRowPosition() {
      return rowPosition;
    }
  }

  private static final class PostingEntry {
    private final VectorDocument document;
    private final int clusterId;
    private final int shardId;
    private final VectorQuantizer.QuantizedVector quantized;

    private PostingEntry(VectorDocument document,
                         int clusterId,
                         int shardId,
                         VectorQuantizer.QuantizedVector quantized) {
      this.document = document;
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.quantized = quantized;
    }
  }

  public static final class BootstrapResult {
    private final float[][] centroids;
    private final Map<String, Integer> assignments;
    private final List<HoodieRecord> records;

    BootstrapResult(float[][] centroids, Map<String, Integer> assignments, List<HoodieRecord> records) {
      this.centroids = centroids;
      this.assignments = Collections.unmodifiableMap(new LinkedHashMap<>(assignments));
      this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public float[][] getCentroids() {
      return centroids;
    }

    public Map<String, Integer> getAssignments() {
      return assignments;
    }

    public List<HoodieRecord> getRecords() {
      return records;
    }
  }
}
