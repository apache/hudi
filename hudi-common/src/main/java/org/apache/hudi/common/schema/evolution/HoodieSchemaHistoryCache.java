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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.TimelinePathProvider;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Global, JVM-wide schema-history cache. Holds a per-table TreeMap of
 * version-id → {@link HoodieSchema} so the read path can look up a schema by
 * version without re-parsing the {@code .hoodie/.schema/} blob on every call.
 *
 * <p>This is the read-path equivalent of {@link HoodieSchemaHistoryStorageManager}:
 * the storage manager owns disk I/O, the cache owns the in-memory map. A segment
 * lock keyed by tablePath hash limits contention without serializing all
 * lookups, and a Caffeine cache with {@code maximumSize=1000} and weakValues
 * keeps memory bounded across many tables.
 *
 * <p>The Caffeine cache is JVM-singleton — every spark task in an executor
 * shares one map per table. Schemas are evicted under memory pressure rather
 * than time.
 */
@Slf4j
public final class HoodieSchemaHistoryCache {

  // Segment lock keyed by tablePath hash. 16 stripes is enough to keep most
  // multi-table executors from contending on a single monitor while still
  // bounding the lock overhead.
  private static final Object[] LOCK_LIST = new Object[16];

  static {
    for (int i = 0; i < LOCK_LIST.length; i++) {
      LOCK_LIST[i] = new Object();
    }
  }

  // Cache shape mirrors the legacy InternalSchemaCache it replaces — same
  // 1000-entry cap, same weakValues semantics — so behavior is unchanged
  // beyond the schema type.
  private static final Cache<String, TreeMap<Long, HoodieSchema>> HISTORICAL_SCHEMA_CACHE =
      Caffeine.newBuilder().maximumSize(1000).weakValues().build();

  private HoodieSchemaHistoryCache() {
  }

  /**
   * Resolves a schema by version id, hitting the cache when possible and
   * falling back to the schema-history files on disk. Returns {@code null}
   * when no schema with that id exists for the table.
   *
   * <p>Two cache-miss conditions trigger a fresh read from disk: the table
   * has no entry yet, and the requested version is newer than any version
   * the cache currently knows about (the cache was warmed before the table
   * advanced).
   */
  public static HoodieSchema searchSchemaAndCache(long versionId, HoodieTableMetaClient metaClient) {
    String tablePath = metaClient.getBasePath().toString();
    synchronized (LOCK_LIST[tablePath.hashCode() & (LOCK_LIST.length - 1)]) {
      TreeMap<Long, HoodieSchema> historicalSchemas = HISTORICAL_SCHEMA_CACHE.getIfPresent(tablePath);
      if (historicalSchemas == null || HoodieSchemaSerDe.searchSchema(versionId, historicalSchemas) == null) {
        historicalSchemas = readHistoricalSchemas(metaClient);
        HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
      } else if (historicalSchemas.keySet().stream().max(Long::compareTo)
          .map(maxVersionId -> versionId > maxVersionId).orElse(false)) {
        // Cache is stale — the requested version is newer than anything we've
        // seen. Re-read so the cache picks up newly-written schema versions.
        historicalSchemas = readHistoricalSchemas(metaClient);
        HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
      }
      return HoodieSchemaSerDe.searchSchema(versionId, historicalSchemas);
    }
  }

  private static TreeMap<Long, HoodieSchema> readHistoricalSchemas(HoodieTableMetaClient metaClient) {
    HoodieSchemaHistoryStorageManager schemasManager = new HoodieSchemaHistoryStorageManager(metaClient);
    String historySchemaStr = schemasManager.getHistorySchemaStr();
    if (StringUtils.isNullOrEmpty(historySchemaStr)) {
      return new TreeMap<>();
    }
    return HoodieSchemaSerDe.parseHistorySchemas(historySchemaStr);
  }

  /**
   * Looks up the {@code latest_schema} (evolution schema) and {@code SCHEMA_KEY}
   * (avro schema) strings carried in the latest commit before the given
   * clustering/compaction instant. Pass-through — already string-based, no
   * schema-type conversion. Used by the compaction/clustering scheduler so the
   * subsequent operation reads with the right schema even when the timeline
   * has advanced after the schedule decision.
   */
  public static Pair<Option<String>, Option<String>> getInternalSchemaAndAvroSchemaForClusteringAndCompaction(
      HoodieTableMetaClient metaClient, String compactionAndClusteringInstant) {
    HoodieTimeline timelineBefore = metaClient.getCommitsAndCompactionTimeline()
        .findInstantsBefore(compactionAndClusteringInstant)
        .filterCompletedInstants();
    Option<HoodieInstant> lastInstantBefore = timelineBefore.lastInstant();
    if (!lastInstantBefore.isPresent()) {
      return Pair.of(Option.empty(), Option.empty());
    }
    HoodieCommitMetadata metadata;
    try {
      metadata = timelineBefore.readCommitMetadata(lastInstantBefore.get());
    } catch (Exception e) {
      throw new HoodieException(String.format("cannot read metadata from commit: %s", lastInstantBefore.get()), e);
    }
    String latestSchemaStr = metadata.getMetadata(HoodieSchemaSerDe.LATEST_SCHEMA);
    if (latestSchemaStr == null) {
      return Pair.of(Option.empty(), Option.empty());
    }
    String existingSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
    return Pair.of(Option.of(latestSchemaStr), Option.of(existingSchemaStr));
  }

  /**
   * Schema lookup intended for the Spark task path: avoids constructing a full
   * {@link HoodieTableMetaClient} since metaClient init is non-trivial at task
   * scope. Tries three sources in order:
   *
   * <ol>
   *   <li>Parse the {@code latest_schema} blob directly from the commit file
   *       whose instant matches {@code versionId}. Cheapest path — one file
   *       open, no scan.</li>
   *   <li>Read the schema-history blob from {@code .hoodie/.schema/} and
   *       look up by version id. Triggered when the requested version's
   *       commit has been archived.</li>
   *   <li>Fall back to the table's avro schema (parsed via
   *       {@link HoodieSchemaUtils#createHoodieWriteSchema}) when the table
   *       wasn't using schema-on-read at write time but a later commit
   *       enabled it.</li>
   * </ol>
   */
  public static HoodieSchema getSchemaByVersionId(long versionId,
                                                  String tablePath,
                                                  HoodieStorage storage,
                                                  String validCommits,
                                                  TimelineLayout timelineLayout,
                                                  HoodieTableConfig tableConfig) {
    InstantFileNameParser fileNameParser = timelineLayout.getInstantFileNameParser();
    CommitMetadataSerDe commitMetadataSerDe = timelineLayout.getCommitMetadataSerDe();
    InstantGenerator instantGenerator = timelineLayout.getInstantGenerator();
    TimelinePathProvider timelinePathProvider = timelineLayout.getTimelinePathProvider();
    StoragePath timelinePath = timelinePathProvider.getTimelinePath(tableConfig, new StoragePath(tablePath));

    String avroSchema = "";
    Set<String> commitSet = Arrays.stream(validCommits.split(",")).collect(Collectors.toSet());
    List<String> validateCommitList = commitSet.stream()
        .map(fileNameParser::extractTimestamp).collect(Collectors.toList());

    // Step 1: parse latest_schema directly from the candidate commit file.
    StoragePath candidateCommitFile = commitSet.stream()
        .filter(fileName -> fileNameParser.extractTimestamp(fileName).equals(versionId + ""))
        .findFirst()
        .map(f -> new StoragePath(timelinePath, f))
        .orElse(null);
    if (candidateCommitFile != null) {
      try {
        HoodieCommitMetadata metadata;
        try (InputStream is = storage.open(candidateCommitFile)) {
          metadata = commitMetadataSerDe.deserialize(
              instantGenerator.createNewInstant(
                  new StoragePathInfo(candidateCommitFile, -1, false, (short) 0, 0L, 0L)),
              is, () -> false, HoodieCommitMetadata.class);
        }
        String latestSchemaStr = metadata.getMetadata(HoodieSchemaSerDe.LATEST_SCHEMA);
        avroSchema = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        if (latestSchemaStr != null) {
          return HoodieSchemaSerDe.fromJson(latestSchemaStr).orElse(null);
        }
      } catch (Exception e) {
        // Swallow and fall through to step 2 — the commit file may have been
        // archived or written by a Hudi version that didn't carry latest_schema.
        log.warn("Cannot find internal schema from commit file {}. Falling back to parsing historical internal schema",
            candidateCommitFile);
      }
    }

    // Step 2: read the schema-history blob.
    HoodieSchemaHistoryStorageManager historyStorageManager =
        new HoodieSchemaHistoryStorageManager(storage, new StoragePath(tablePath));
    String latestHistorySchema =
        historyStorageManager.getHistorySchemaStrByGivenValidCommits(validateCommitList);
    if (latestHistorySchema.isEmpty()) {
      return HoodieSchema.empty();
    }
    HoodieSchema fileSchema = HoodieSchemaSerDe.searchSchema(versionId, HoodieSchemaSerDe.parseHistorySchemas(latestHistorySchema));
    if (fileSchema != null && !fileSchema.isEmptySchema()) {
      return fileSchema;
    }

    // Step 3: fall back to the table's avro schema. Triggered when the table
    // started without schema-on-read enabled — there's no version history to
    // parse, but the avro schema in commit metadata gives us the structure.
    if (StringUtils.isNullOrEmpty(avroSchema)) {
      return HoodieSchema.empty();
    }
    return HoodieSchemaUtils.createHoodieWriteSchema(avroSchema, false);
  }

  public static HoodieSchema getSchemaByVersionId(long versionId,
                                                  String tablePath,
                                                  HoodieStorage storage,
                                                  String validCommits,
                                                  TimelineLayout timelineLayout) {
    return getSchemaByVersionId(versionId, tablePath, storage, validCommits, timelineLayout,
        HoodieTableConfig.loadFromHoodieProps(storage, tablePath));
  }

  /**
   * Schema lookup using a metaClient. Convenient when the caller already has
   * one — derives the full validCommits string from the timeline and forwards
   * to the path-based overload.
   */
  public static HoodieSchema getSchemaByVersionId(long versionId, HoodieTableMetaClient metaClient) {
    InstantFileNameGenerator factory = metaClient.getInstantFileNameGenerator();
    String validCommitLists = metaClient.getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .map(factory::getFileName)
        .collect(Collectors.joining(","));
    return getSchemaByVersionId(versionId, metaClient.getBasePath().toString(), metaClient.getStorage(),
        validCommitLists, metaClient.getTimelineLayout(), metaClient.getTableConfig());
  }
}
