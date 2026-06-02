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

import org.apache.hudi.avro.model.HoodieVectorIndexInfo;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.RawKey;

import java.io.Serializable;
import java.nio.ByteBuffer;
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

  private final float[][] centroids;
  private final int generationId;
  private final long quantizerSeed;
  private final int dimension;
  private final boolean assumeNormalized;
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
                                   int dimension,
                                   boolean assumeNormalized,
                                   Map<Integer, ClusterManifest> clusterManifests,
                                   String validForInstant) {
    this.centroids = centroids;
    this.generationId = generationId;
    this.quantizerSeed = quantizerSeed;
    this.dimension = dimension;
    this.assumeNormalized = assumeNormalized;
    this.clusterManifests = Collections.unmodifiableMap(clusterManifests);
    this.validForInstant = validForInstant;
  }

  // ---- Factory -----------------------------------------------------------

  /**
   * Loads all cacheable vector index metadata in ONE MDT round trip.
   *
   * <p>Fetches: __centroids__, __manifest__, __quantizer__, and all C|gen|* cluster
   * manifest rows for the active generation. Falls back to legacy __fg__/ rows if
   * cluster manifests are not available.
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
    // ONE MDT fetch: centroids + manifest + quantizer + C| prefix for all clusters
    List<RawKey> lookupKeys = Arrays.asList(
        simpleKey(HoodieTableMetadataUtil.VECTOR_INDEX_CENTROIDS_KEY),
        simpleKey(HoodieTableMetadataUtil.VECTOR_INDEX_MANIFEST_KEY),
        simpleKey(HoodieTableMetadataUtil.VECTOR_INDEX_QUANTIZER_KEY),
        simpleKey(HoodieTableMetadataUtil.VECTOR_INDEX_CLUSTER_KEY_PREFIX));

    List<HoodieRecord<HoodieMetadataPayload>> records = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(lookupKeys), indexPartition, true)
        .collectAsList();

    // Extract centroids
    float[][] centroids = null;
    int generationId = -1;
    long quantizerSeed = 42L;
    int dimension = 0;
    boolean assumeNormalized = false;
    Map<Integer, ClusterManifest> clusterManifests = new HashMap<>();

    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      HoodieVectorIndexInfo info = extractVectorInfo(record);
      if (info == null) {
        continue;
      }

      String entryType = info.getEntryType();
      if (entryType == null) {
        continue;
      }

      switch (entryType) {
        case HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CENTROIDS:
          if (info.getCentroidBytes() != null && vectorSchema != null) {
            centroids = deserializeCentroids(info.getCentroidBytes(), vectorSchema);
          }
          break;

        case HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_MANIFEST:
          Integer parsedGen = parseGenerationId(info.getGenerationId());
          if (parsedGen != null) {
            generationId = parsedGen;
          }
          // Manifest may also carry quantizer config
          if (info.getRandomSeed() != 0) {
            quantizerSeed = info.getRandomSeed();
          }
          assumeNormalized = info.getAssumeNormalized();
          if (info.getQuantizedCodeBytes() > 0) {
            dimension = info.getQuantizedCodeBytes() * 8;
          }
          break;

        case HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_QUANTIZER:
          quantizerSeed = info.getRandomSeed();
          assumeNormalized = info.getAssumeNormalized();
          if (info.getQuantizedCodeBytes() > 0) {
            dimension = info.getQuantizedCodeBytes() * 8;
          }
          break;

        case HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER:
          Set<String> fgIds = info.getFileGroupIds() == null
              ? Collections.emptySet()
              : new HashSet<>(info.getFileGroupIds());
          clusterManifests.put(
              info.getClusterId(),
              new ClusterManifest(info.getClusterId(), info.getShardCount(), fgIds, info.getVectorCount()));
          break;

        default:
          // Generation manifests (M|) may also be returned; extract generationId as fallback
          if (generationId < 0) {
            Integer fallbackGen = parseGenerationId(info.getGenerationId());
            if (fallbackGen != null) {
              generationId = fallbackGen;
            }
          }
          break;
      }
    }

    if (centroids == null || centroids.length == 0) {
      return null;
    }

    // Centroids are the authoritative source for dimension; quantizedCodeBytes
    // is only a fallback (it represents the RaBitQ code length, not the dimension).
    if (centroids.length > 0) {
      dimension = centroids[0].length;
    } else if (dimension <= 0) {
      dimension = 0;
    }

    // If no cluster manifests from C| records, fall back to legacy __fg__/ rows
    if (clusterManifests.isEmpty()) {
      clusterManifests = loadLegacyFgMappings(metadataTable, indexPartition);
    }

    return new VectorIndexMetadataCache(
        centroids,
        generationId,
        quantizerSeed,
        dimension,
        assumeNormalized,
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
        result.put(clusterId, 1);
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

  public int getDimension() {
    return dimension;
  }

  public boolean isAssumeNormalized() {
    return assumeNormalized;
  }

  public Map<Integer, ClusterManifest> getClusterManifests() {
    return clusterManifests;
  }

  public String getValidForInstant() {
    return validForInstant;
  }

  public int numClusters() {
    return centroids == null ? 0 : centroids.length;
  }

  // ---- Private helpers ---------------------------------------------------

  private static HoodieVectorIndexInfo extractVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    HoodieMetadataPayload payload = record.getData();
    if (payload == null) {
      return null;
    }
    return payload.getVectorIndexMetadata().isPresent()
        ? payload.getVectorIndexMetadata().get()
        : null;
  }

  private static Integer parseGenerationId(String raw) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    try {
      return Integer.parseUnsignedInt(raw);
    } catch (NumberFormatException e) {
      try {
        return Integer.parseInt(raw);
      } catch (NumberFormatException e2) {
        return null;
      }
    }
  }

  /**
   * Deserializes centroids from a ByteBuffer using the vector schema's element type.
   * Same logic as VectorIndexSupport.deserializeCentroids but accessible from hudi-common.
   */
  public static float[][] deserializeCentroids(ByteBuffer bytes, HoodieSchema.Vector vectorSchema) {
    HoodieSchema.Vector.VectorElementType elementType = vectorSchema.getVectorElementType();
    int dim = vectorSchema.getDimension();
    ByteBuffer dup = bytes.duplicate().order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    int bytesPerCentroid = dim * elementType.getElementSize();
    if (bytesPerCentroid == 0) {
      return new float[0][];
    }
    int centroidCount = dup.remaining() / bytesPerCentroid;
    float[][] result = new float[centroidCount][dim];
    for (int i = 0; i < centroidCount; i++) {
      for (int j = 0; j < dim; j++) {
        switch (elementType) {
          case FLOAT:
            result[i][j] = dup.getFloat();
            break;
          case DOUBLE:
            result[i][j] = (float) dup.getDouble();
            break;
          case INT8:
            result[i][j] = dup.get();
            break;
          default:
            throw new IllegalArgumentException("Unsupported element type: " + elementType);
        }
      }
    }
    return result;
  }

  /**
   * Falls back to legacy __fg__/ prefix scan when cluster manifests are unavailable.
   */
  private static Map<Integer, ClusterManifest> loadLegacyFgMappings(HoodieTableMetadata metadataTable,
                                                                     String indexPartition) {
    List<RawKey> fgKeys = Collections.singletonList(
        simpleKey(HoodieTableMetadataUtil.VECTOR_INDEX_FG_MAPPING_KEY_PREFIX));

    List<HoodieRecord<HoodieMetadataPayload>> fgRecords = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(fgKeys), indexPartition, true)
        .collectAsList();

    Map<Integer, Set<String>> clusterFileGroups = new HashMap<>();
    Map<Integer, Long> clusterVectorCounts = new HashMap<>();

    for (HoodieRecord<HoodieMetadataPayload> record : fgRecords) {
      HoodieVectorIndexInfo info = extractVectorInfo(record);
      if (info == null) {
        continue;
      }
      int clusterId = info.getClusterId();
      if (info.getFileGroupIds() != null) {
        clusterFileGroups
            .computeIfAbsent(clusterId, k -> new HashSet<>())
            .addAll(info.getFileGroupIds());
      }
      clusterVectorCounts.merge(clusterId, info.getVectorCount(), Long::sum);
    }

    Map<Integer, ClusterManifest> result = new HashMap<>();
    for (Map.Entry<Integer, Set<String>> entry : clusterFileGroups.entrySet()) {
      int clusterId = entry.getKey();
      result.put(clusterId, new ClusterManifest(
          clusterId,
          1,  // legacy rows don't have shard counts
          entry.getValue(),
          clusterVectorCounts.getOrDefault(clusterId, 0L)));
    }
    return result;
  }

  private static RawKey simpleKey(String key) {
    return new RawKey() {
      @Override
      public String encode() {
        return key;
      }
    };
  }
}
