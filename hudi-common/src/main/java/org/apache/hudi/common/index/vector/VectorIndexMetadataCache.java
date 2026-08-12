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

import org.apache.hudi.avro.model.HoodieVectorIndexCentroids;
import org.apache.hudi.avro.model.HoodieVectorIndexClusterStats;
import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.avro.model.HoodieVectorIndexQuantizer;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Driver-side cache for vector index metadata that is stable between rebuilds.
 *
 * <p>Holds centroids, the active generation manifest, quantizer configuration,
 * and all cluster manifests in driver memory. This data changes only on index
 * rebuild or LIRE maintenance, so it can be loaded once and reused across queries
 * until the Hudi timeline advances.
 *
 * <p>Typical memory footprint: ~12 MB for K=4096, D=768 (dominated by centroids).
 *
 * <p>This class is engine-agnostic (lives in hudi-common) so Spark, Trino, and
 * Flink can all share the same caching behavior.
 *
 * <p>Thread safety: instances are immutable after construction. The owning index
 * support class is responsible for atomic replacement on cache invalidation.
 */
public final class VectorIndexMetadataCache implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMetadataCache.class);

  private final float[][] centroids;
  private final int generationId;
  private final long quantizerSeed;
  private final int rabitqBits;
  private final int dimension;
  private final boolean assumeNormalized;
  private final boolean residualEncoding;
  private final int defaultShardCount;
  private final Map<Integer, ClusterManifest> clusterManifests;
  private final String validForInstant;

  /**
   * Compact representation of one cluster's manifest metadata.
   */
  public static final class ClusterManifest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int clusterId;
    private final int shardCount;
    private final Set<String> fileGroupIds;
    private final long vectorCount;

    public ClusterManifest(int clusterId, int shardCount, Set<String> fileGroupIds, long vectorCount) {
      this.clusterId = clusterId;
      this.shardCount = shardCount;
      this.fileGroupIds = fileGroupIds == null ? Collections.emptySet() : Collections.unmodifiableSet(fileGroupIds);
      this.vectorCount = vectorCount;
    }

    public int getClusterId() {
      return clusterId;
    }

    public int getShardCount() {
      return Math.max(1, shardCount);
    }

    public Set<String> getFileGroupIds() {
      return fileGroupIds;
    }

    public long getVectorCount() {
      return vectorCount;
    }
  }

  private VectorIndexMetadataCache(float[][] centroids,
                                   int generationId,
                                   long quantizerSeed,
                                   int rabitqBits,
                                   int dimension,
                                   boolean assumeNormalized,
                                   boolean residualEncoding,
                                   int defaultShardCount,
                                   Map<Integer, ClusterManifest> clusterManifests,
                                   String validForInstant) {
    this.centroids = centroids;
    this.generationId = generationId;
    this.quantizerSeed = quantizerSeed;
    this.rabitqBits = rabitqBits;
    this.dimension = dimension;
    this.assumeNormalized = assumeNormalized;
    this.residualEncoding = residualEncoding;
    this.defaultShardCount = Math.max(1, defaultShardCount);
    this.clusterManifests = Collections.unmodifiableMap(clusterManifests);
    this.validForInstant = validForInstant;
  }

  // ---- Factory -----------------------------------------------------------

  /**
   * Loads all cacheable vector index metadata in ONE MDT round trip.
   *
   * <p>Fetches: __centroids__, __manifest__, __quantizer__, and all C|gen|* cluster
   * manifest rows for the active generation.
   *
   * @param metadataTable  the metadata table reader
   * @param indexPartition the vector index partition name (e.g. "vector_index_embedding_idx")
   * @param vectorSchema   the VECTOR schema for centroid deserialization
   * @param currentInstant the current timeline instant for staleness tracking
   * @return a populated cache, or null if critical metadata is missing
   */
  public static VectorIndexMetadataCache load(HoodieTableMetadata metadataTable,
                                              String indexPartition,
                                              HoodieSchema.Vector vectorSchema,
                                              String currentInstant) {
    return load(metadataTable, indexPartition, vectorSchema, currentInstant, true);
  }

  /**
   * Loads cacheable vector index metadata.
   *
   * <p>When {@code shouldLoadClusterManifests} is false, this intentionally avoids
   * reading all {@code C|} rows into the driver. Use this mode for posting-based
   * searches where selected cluster shard counts and file groups are derived from
   * the probed postings instead.
   */
  public static VectorIndexMetadataCache load(HoodieTableMetadata metadataTable,
                                              String indexPartition,
                                              HoodieSchema.Vector vectorSchema,
                                              String currentInstant,
                                              boolean shouldLoadClusterManifests) {
    // Hot-path routing metadata. Scan binary key families and then select the
    // highest ACTIVE generation before reading generation-scoped records.
    List<HoodieRecord<HoodieMetadataPayload>> records;
    List<RawKey> lookupKeys = new ArrayList<>(Collections.singletonList(
        familyPrefix(VectorIndexMetadataKey.FAMILY_MANIFEST)));
    if (shouldLoadClusterManifests) {
      lookupKeys.add(familyPrefix(VectorIndexMetadataKey.FAMILY_CENTROIDS));
      lookupKeys.add(familyPrefix(VectorIndexMetadataKey.FAMILY_QUANTIZER));
      lookupKeys.add(familyPrefix(VectorIndexMetadataKey.FAMILY_CLUSTER_STATS));
    }
    records = new ArrayList<>(metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(lookupKeys), indexPartition, true)
        .collectAsList());

    Map<Integer, HoodieVectorIndexManifest> activeManifests = new HashMap<>();
    Set<Integer> centroidGenerations = new HashSet<>();
    Set<Integer> quantizerGenerations = new HashSet<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Object info = extractVectorInfo(record);
      if (isManifestInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_MANIFEST);
        if (recordGeneration != null && "ACTIVE".equalsIgnoreCase(getManifestState(info))) {
          activeManifests.put(recordGeneration, asManifest(info));
        }
      } else if (isCentroidsInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_CENTROIDS);
        if (recordGeneration != null) {
          centroidGenerations.add(recordGeneration);
        }
      } else if (isQuantizerInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_QUANTIZER);
        if (recordGeneration != null) {
          quantizerGenerations.add(recordGeneration);
        }
      }
    }

    HoodieVectorIndexManifest activeManifest = null;
    int generationId = -1;
    int centroidGenerationId = -1;
    int quantizerGenerationId = -1;
    for (Map.Entry<Integer, HoodieVectorIndexManifest> entry : activeManifests.entrySet()) {
      int candidateGeneration = entry.getKey();
      if ((!shouldLoadClusterManifests || centroidGenerations.contains(candidateGeneration))
          && candidateGeneration > generationId) {
        generationId = candidateGeneration;
        centroidGenerationId = candidateGeneration;
        activeManifest = entry.getValue();
      }
    }

    if (activeManifest == null) {
      LOG.warn("Vector metadata cache load found no complete ACTIVE generation: partition={}, records={}, activeGenerations={}, centroidGenerations={}",
          indexPartition, records.size(), activeManifests.keySet(), centroidGenerations);
      return null;
    }

    if (!shouldLoadClusterManifests) {
      records.addAll(metadataTable
          .getRecordsByKeyPrefixes(HoodieListData.eager(Arrays.asList(
              rawKey(VectorIndexMetadataKey.centroids(generationId, 0)),
              rawKey(VectorIndexMetadataKey.quantizer(generationId, 0)))), indexPartition, true)
          .collectAsList());
      for (HoodieRecord<HoodieMetadataPayload> record : records) {
        Object info = extractVectorInfo(record);
        if (isCentroidsInfo(info)) {
          Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_CENTROIDS);
          if (recordGeneration != null) {
            centroidGenerations.add(recordGeneration);
          }
        } else if (isQuantizerInfo(info)) {
          Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_QUANTIZER);
          if (recordGeneration != null) {
            quantizerGenerations.add(recordGeneration);
          }
        }
      }
    }

    if (quantizerGenerations.contains(generationId)) {
      quantizerGenerationId = generationId;
    } else {
      LOG.warn("Vector metadata cache load found no quantizer for ACTIVE generation: partition={}, generation={}, quantizerGenerations={}",
          indexPartition, generationId, quantizerGenerations);
      return null;
    }

    float[][] centroids = null;
    Long quantizerSeed = null;
    int rabitqBits = activeManifest.getBitsTotal();
    int dimension = activeManifest.getDim();
    boolean assumeNormalized = activeManifest.getAssumeNormalized();
    boolean residualEncoding = activeManifest.getResidualEncoding();
    Map<Integer, ClusterManifest> clusterManifests = new HashMap<>();

    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Object info = extractVectorInfo(record);
      if (info == null) {
        continue;
      }

      if (isCentroidsInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_CENTROIDS);
        if (recordGeneration == null || recordGeneration != centroidGenerationId) {
          continue;
        }
        ByteBuffer centroidBytes = getCentroidBytes(info);
        if (centroidBytes != null && vectorSchema != null) {
          centroids = deserializeCentroids(centroidBytes, activeManifest);
        }
      } else if (isQuantizerInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_QUANTIZER);
        if (recordGeneration == null || recordGeneration != quantizerGenerationId) {
          continue;
        }
        quantizerSeed = getQuantizerSeed(info);
      } else if (shouldLoadClusterManifests && isClusterStatsInfo(info)) {
        Integer recordGeneration = generationFromKey(record.getRecordKey(), VectorIndexMetadataKey.FAMILY_CLUSTER_STATS);
        if (recordGeneration == null || recordGeneration != generationId) {
          continue;
        }
        Integer clusterId = clusterIdFromKey(record.getRecordKey());
        if (clusterId != null) {
          clusterManifests.put(clusterId, new ClusterManifest(
              clusterId,
              activeManifest.getShardCount(),
              Collections.emptySet(),
              getClusterLiveCount(info)));
        }
      }
    }

    if (centroids == null || centroids.length == 0) {
      LOG.warn("Vector metadata cache load found no centroids: partition={}, manifestGeneration={}, centroidGeneration={}, records={}",
          indexPartition, generationId, centroidGenerationId, records.size());
      return null;
    }
    if (quantizerSeed == null) {
      LOG.warn("Vector metadata cache load found no quantizer seed: partition={}, generation={}",
          indexPartition, generationId);
      return null;
    }

    LOG.info("Loaded vector metadata cache: partition={}, manifestGeneration={}, centroidGeneration={}, quantizerGeneration={}, "
            + "clusters={}, dim={}, rabitqBits={}, shouldLoadClusterManifests={}",
        indexPartition, generationId, centroidGenerationId, quantizerGenerationId,
        centroids.length, dimension, rabitqBits, shouldLoadClusterManifests);

    return new VectorIndexMetadataCache(
        centroids,
        generationId,
        quantizerSeed,
        rabitqBits,
        dimension,
        assumeNormalized,
        residualEncoding,
        activeManifest.getShardCount(),
        clusterManifests,
        currentInstant);
  }

  // ---- Query helpers (pure CPU, no IO) -----------------------------------

  /**
   * Finds the top-P closest clusters to the query vector.
   * Linear scan over centroids; HNSW routing replaces this later.
   */
  public int[] findTopClusters(float[] queryVector, int numProbes, VectorDistanceMetric metric) {
    if (centroids == null || centroids.length == 0) {
      return new int[0];
    }
    int effectiveProbes = Math.min(Math.max(1, numProbes), centroids.length);
    VectorIndexPruner pruner = new VectorIndexPruner(
        centroids, Collections.emptyMap(), metric);
    return pruner.findTopClusters(queryVector, effectiveProbes);
  }

  /**
   * Returns shard counts for the given cluster IDs from the cached cluster manifests.
   */
  public Map<Integer, Integer> getShardCounts(int[] clusterIds) {
    Map<Integer, Integer> result = new HashMap<>(clusterIds.length);
    for (int clusterId : clusterIds) {
      ClusterManifest manifest = clusterManifests.get(clusterId);
      if (manifest != null) {
        result.put(clusterId, manifest.getShardCount());
      } else {
        result.put(clusterId, defaultShardCount);
      }
    }
    return result;
  }

  /**
   * Returns the union of file group IDs for the given cluster IDs,
   * optionally filtered by partition paths.
   */
  public Set<String> getFileGroupsForClusters(int[] clusterIds, Set<String> partitionFilter) {
    Set<String> fileGroups = new HashSet<>();
    for (int clusterId : clusterIds) {
      ClusterManifest manifest = clusterManifests.get(clusterId);
      if (manifest != null && !manifest.getFileGroupIds().isEmpty()) {
        // ClusterManifest file groups are not partition-filtered — they are the full set.
        // Partition filtering happens at the Spark level when matching against pruned file slices.
        fileGroups.addAll(manifest.getFileGroupIds());
      }
    }
    return fileGroups;
  }

  /**
   * Returns true if the cache is stale relative to the given timeline instant.
   */
  public boolean isStaleFor(String currentInstant) {
    if (validForInstant == null || currentInstant == null) {
      return true;
    }
    return !validForInstant.equals(currentInstant);
  }

  // ---- Accessors ---------------------------------------------------------

  public float[][] getCentroids() {
    return centroids;
  }

  public int getGenerationId() {
    return generationId;
  }

  public long getQuantizerSeed() {
    return quantizerSeed;
  }

  public int getRaBitQBits() {
    return rabitqBits;
  }

  public int getDimension() {
    return dimension;
  }

  public boolean isAssumeNormalized() {
    return assumeNormalized;
  }

  public boolean isResidualEncoding() {
    return residualEncoding;
  }

  public Map<Integer, ClusterManifest> getClusterManifests() {
    return clusterManifests;
  }

  public String getValidForInstant() {
    return validForInstant;
  }

  public String getLoadInstant() {
    return validForInstant;
  }

  public int numClusters() {
    return centroids == null ? 0 : centroids.length;
  }

  // ---- Private helpers ---------------------------------------------------

  private static Object extractVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    HoodieMetadataPayload payload = record.getData();
    if (payload == null) {
      return null;
    }
    return payload.getVectorIndexMetadata().isPresent()
        ? payload.getVectorIndexMetadata().get()
        : null;
  }

  private static boolean isManifestInfo(Object info) {
    return info instanceof HoodieVectorIndexManifest || hasAvroName(info, "HoodieVectorIndexManifest");
  }

  private static boolean isCentroidsInfo(Object info) {
    return info instanceof HoodieVectorIndexCentroids || hasAvroName(info, "HoodieVectorIndexCentroids");
  }

  private static boolean isQuantizerInfo(Object info) {
    return info instanceof HoodieVectorIndexQuantizer || hasAvroName(info, "HoodieVectorIndexQuantizer");
  }

  private static boolean isClusterStatsInfo(Object info) {
    return info instanceof HoodieVectorIndexClusterStats || hasAvroName(info, "HoodieVectorIndexClusterStats");
  }

  private static boolean hasAvroName(Object info, String name) {
    return info instanceof GenericRecord && name.equals(((GenericRecord) info).getSchema().getName());
  }

  private static HoodieVectorIndexManifest asManifest(Object info) {
    if (info instanceof HoodieVectorIndexManifest) {
      return (HoodieVectorIndexManifest) info;
    }
    GenericRecord record = (GenericRecord) info;
    return new HoodieVectorIndexManifest(
        intField(record, "indexVersion"),
        stringField(record, "generationId"),
        stringField(record, "state"),
        intField(record, "dim"),
        intField(record, "dimPadded"),
        intField(record, "codeRowBytes"),
        intField(record, "bitsTotal"),
        intField(record, "numExPlanes"),
        intField(record, "numClusters"),
        intField(record, "routingVersion"),
        byteBufferField(record, "routingCoarseCentroids"),
        byteBufferField(record, "routingLeafOffsets"),
        floatField(record, "routingExpandRatio"),
        intField(record, "shardCount"),
        intField(record, "fileGroupCount"),
        stringField(record, "metric"),
        booleanField(record, "assumeNormalized"),
        booleanField(record, "residualEncoding"),
        stringField(record, "vectorColumn"),
        intField(record, "targetBlockBytes"),
        intField(record, "vectorsPerBlock"),
        intField(record, "blockFormatVersion"),
        intField(record, "factorVersion"),
        doubleField(record, "kappa"),
        doubleField(record, "gMin"),
        doubleField(record, "eps1Max"),
        doubleField(record, "epsNRel"),
        intField(record, "centroidChunkCount"),
        nullableStringField(record, "centroidChecksum"),
        intField(record, "splitLimit"),
        intField(record, "mergeFloor"),
        nullableStringField(record, "lastContiguousSourceInstant"),
        longField(record, "createdTs"));
  }

  private static String getManifestState(Object info) {
    return info instanceof HoodieVectorIndexManifest
        ? ((HoodieVectorIndexManifest) info).getState()
        : stringField((GenericRecord) info, "state");
  }

  private static ByteBuffer getCentroidBytes(Object info) {
    return info instanceof HoodieVectorIndexCentroids
        ? ((HoodieVectorIndexCentroids) info).getCentroidBytes()
        : (ByteBuffer) ((GenericRecord) info).get("centroidBytes");
  }

  private static long getQuantizerSeed(Object info) {
    return info instanceof HoodieVectorIndexQuantizer
        ? ((HoodieVectorIndexQuantizer) info).getRandomSeed()
        : longField((GenericRecord) info, "randomSeed");
  }

  private static long getClusterLiveCount(Object info) {
    return info instanceof HoodieVectorIndexClusterStats
        ? ((HoodieVectorIndexClusterStats) info).getLiveCount()
        : longField((GenericRecord) info, "liveCount");
  }

  private static String stringField(GenericRecord record, String field) {
    Object value = requiredField(record, field);
    return value.toString();
  }

  private static String nullableStringField(GenericRecord record, String field) {
    Object value = record.get(field);
    return value == null ? null : value.toString();
  }

  private static ByteBuffer byteBufferField(GenericRecord record, String field) {
    return (ByteBuffer) requiredField(record, field);
  }

  private static float floatField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).floatValue();
  }

  private static double doubleField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).doubleValue();
  }

  private static int intField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).intValue();
  }

  private static long longField(GenericRecord record, String field) {
    return ((Number) requiredField(record, field)).longValue();
  }

  private static boolean booleanField(GenericRecord record, String field) {
    return (Boolean) requiredField(record, field);
  }

  private static Object requiredField(GenericRecord record, String field) {
    Object value = record.get(field);
    if (value == null) {
      throw new IllegalArgumentException(
          "Required vector metadata field '" + field + "' is missing from " + record.getSchema().getName());
    }
    return value;
  }

  private static Integer generationFromKey(String recordKey, int family) {
    byte[] bytes = keyBytes(recordKey);
    if (bytes == null || bytes.length < 5 || Byte.toUnsignedInt(bytes[0]) != family) {
      return null;
    }
    return readInt(bytes, 1);
  }

  private static Integer clusterIdFromKey(String recordKey) {
    byte[] bytes = keyBytes(recordKey);
    if (bytes == null || bytes.length < 9
        || Byte.toUnsignedInt(bytes[0]) != VectorIndexMetadataKey.FAMILY_CLUSTER_STATS) {
      return null;
    }
    return readInt(bytes, 5);
  }

  private static int readInt(byte[] bytes, int offset) {
    return (Byte.toUnsignedInt(bytes[offset]) << 24)
        | (Byte.toUnsignedInt(bytes[offset + 1]) << 16)
        | (Byte.toUnsignedInt(bytes[offset + 2]) << 8)
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }

  private static byte[] keyBytes(String recordKey) {
    return recordKey == null ? null : recordKey.getBytes(StandardCharsets.ISO_8859_1);
  }

  public static float[][] deserializeCentroids(ByteBuffer bytes, HoodieSchema.Vector vectorSchema) {
    return deserializeCentroids(bytes, vectorSchema.getDimension(), vectorSchema.getDimension(), -1);
  }

  /**
   * Deserializes centroid records. The persisted centroid payload is always float32,
   * independent of the table vector element type.
   */
  private static float[][] deserializeCentroids(ByteBuffer bytes, HoodieVectorIndexManifest manifest) {
    return deserializeCentroids(bytes, manifest.getDim(), manifest.getDimPadded(), manifest.getNumClusters());
  }

  private static float[][] deserializeCentroids(ByteBuffer bytes, int dim, int dimPadded, int expectedCentroidCount) {
    ByteBuffer dup = bytes.duplicate().order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    int bytesPerCentroid = dimPadded * Float.BYTES;
    if (bytesPerCentroid == 0) {
      return new float[0][];
    }
    if (expectedCentroidCount >= 0) {
      int expectedBytes = expectedCentroidCount * bytesPerCentroid;
      if (dup.remaining() != expectedBytes) {
        throw new IllegalArgumentException(
            "Centroid payload size mismatch: expected " + expectedBytes + " bytes for "
                + expectedCentroidCount + " centroids with padded dimension " + dimPadded
                + ", got " + dup.remaining());
      }
    } else if (dup.remaining() % bytesPerCentroid != 0) {
      throw new IllegalArgumentException(
          "Centroid payload has trailing bytes: remaining=" + dup.remaining()
              + ", bytesPerCentroid=" + bytesPerCentroid);
    }
    int centroidCount = expectedCentroidCount >= 0 ? expectedCentroidCount : dup.remaining() / bytesPerCentroid;
    float[][] result = new float[centroidCount][dim];
    for (int i = 0; i < centroidCount; i++) {
      for (int j = 0; j < dim; j++) {
        result[i][j] = dup.getFloat();
      }
      for (int j = dim; j < dimPadded; j++) {
        dup.getFloat();
      }
    }
    return result;
  }

  private static RawKey familyPrefix(int family) {
    String prefix = new String(new byte[] {(byte) family}, StandardCharsets.ISO_8859_1);
    return rawKey(prefix);
  }

  private static RawKey rawKey(String key) {
    return new RawKey() {
      @Override
      public String encode() {
        return key;
      }
    };
  }

}
