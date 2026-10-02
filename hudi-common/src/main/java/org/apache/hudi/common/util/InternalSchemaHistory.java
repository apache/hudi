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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The schema history of a table as seen by one query, used to find the {@link InternalSchema} a data file
 * was written with from the file's commit time.
 *
 * <p>It is loaded once where the query is planned, restricted to the query's valid commits, and shipped to
 * the readers, so resolving a file's schema reads no table metadata. It resolves a version id exactly like
 * {@link InternalSchemaCache#getInternalSchemaByVersionId(long, String, org.apache.hudi.storage.HoodieStorage,
 * String, org.apache.hudi.common.table.timeline.TimelineLayout, org.apache.hudi.common.table.HoodieTableConfig)}:
 * a valid commit resolves to the {@code latest_schema} in its own commit metadata, which is not always the history
 * version in effect at its commit time (a schema change requested before the commit may complete after it).
 *
 * <p>It is kept as configuration entries, so it can travel inside a Hadoop configuration and a reader looks up
 * only the entry it needs.
 */
@Slf4j
public final class InternalSchemaHistory implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String CONFIG_PREFIX = "hoodie.internal.schema.history.";
  // comma-separated version ids of the schema history
  private static final String VERSIONS_KEY = CONFIG_PREFIX + "versions";
  private static final String VERSION_KEY_PREFIX = CONFIG_PREFIX + "version.";
  // comma-separated "<commit time>=<version id>" for commits whose schema is a history version,
  // and "<commit time>=#<n>" for commits whose schema is kept under SCHEMA_KEY_PREFIX + n
  private static final String COMMITS_KEY = CONFIG_PREFIX + "commits";
  // schemas of commits that are not history versions, each distinct schema kept once
  private static final String SCHEMA_KEY_PREFIX = CONFIG_PREFIX + "schema.";
  private static final char SCHEMA_REF_MARKER = '#';

  private static final int MAX_COMMIT_READ_PARALLELISM = 16;

  // Completed commit files never change, so what each one says about its schema is kept across queries.
  private static final Cache<String, CommitSchema> COMMIT_SCHEMA_CACHE = Caffeine.newBuilder()
      .maximumWeight(16L * 1024 * 1024)
      .weigher((String commitFile, CommitSchema commitSchema) -> commitFile.length() + Math.max(1, commitSchema.weight))
      .build();

  private final Map<String, String> configs;

  private InternalSchemaHistory(Map<String, String> configs) {
    this.configs = configs;
  }

  /**
   * Loads the schema history of the table restricted to the given commits.
   *
   * @param metaClient   meta client of the table
   * @param validCommits comma-separated instant file names of the commits the query may read; when empty, the
   *                     completed commits of the meta client's timeline
   */
  public static InternalSchemaHistory load(HoodieTableMetaClient metaClient, String validCommits) {
    InstantFileNameParser fileNameParser = metaClient.getInstantFileNameParser();
    Set<String> commitFileNames = StringUtils.isNullOrEmpty(validCommits)
        ? Collections.emptySet()
        : new LinkedHashSet<>(Arrays.asList(validCommits.split(",")));
    List<String> commitTimes = commitFileNames.stream().map(fileNameParser::extractTimestamp).collect(Collectors.toList());

    String historySchemaStr = new FileBasedInternalSchemaStorageManager(metaClient).getHistorySchemaStrByGivenValidCommits(commitTimes);
    TreeMap<Long, InternalSchema> history = historySchemaStr.isEmpty() ? new TreeMap<>() : SerDeHelper.parseSchemas(historySchemaStr);

    Map<String, String> configs = new HashMap<>();
    configs.put(VERSIONS_KEY, history.keySet().stream().map(String::valueOf).collect(Collectors.joining(",")));
    history.forEach((versionId, schema) -> configs.put(VERSION_KEY_PREFIX + versionId, SerDeHelper.toJson(schema)));

    List<String> commitEntries = new ArrayList<>();
    Map<String, Integer> schemaRefs = new HashMap<>();
    List<String> resolvableCommitFiles = commitFileNames.stream()
        .filter(commitFileName -> parseVersionId(fileNameParser.extractTimestamp(commitFileName)).isPresent())
        .collect(Collectors.toList());
    Map<String, CommitSchema> commitSchemas = readCommitSchemas(metaClient, resolvableCommitFiles);
    for (String commitFileName : resolvableCommitFiles) {
      CommitSchema commitSchema = commitSchemas.get(commitFileName);
      if (commitSchema == null) {
        continue;
      }
      String commitTime = fileNameParser.extractTimestamp(commitFileName);
      if (commitSchema.hasLatestSchema) {
        InternalSchema latestSchema = commitSchema.latestSchema;
        if (latestSchema != null && latestSchema.equals(history.get(latestSchema.schemaId()))) {
          commitEntries.add(commitTime + "=" + latestSchema.schemaId());
        } else {
          commitEntries.add(commitTime + "=" + schemaRef(latestSchema == null ? "" : SerDeHelper.toJson(latestSchema), schemaRefs, configs));
        }
      } else if (commitSchema.avroSchema != null && !history.isEmpty() && history.floorKey(parseVersionId(commitTime).get()) == null) {
        commitEntries.add(commitTime + "=" + schemaRef(SerDeHelper.toJson(commitSchema.avroSchema), schemaRefs, configs));
      }
    }
    configs.put(COMMITS_KEY, String.join(",", commitEntries));
    return new InternalSchemaHistory(configs);
  }

  /**
   * Returns the reference of a commit schema that is not a history version, storing each distinct schema once.
   */
  private static String schemaRef(String schemaJson, Map<String, Integer> schemaRefs, Map<String, String> configs) {
    Integer ref = schemaRefs.get(schemaJson);
    if (ref == null) {
      ref = schemaRefs.size();
      schemaRefs.put(schemaJson, ref);
      configs.put(SCHEMA_KEY_PREFIX + ref, schemaJson);
    }
    return SCHEMA_REF_MARKER + String.valueOf(ref);
  }

  /**
   * Returns the schema a file written by the given commit was written with, or an empty schema if unknown.
   * The result is {@code null} when the commit metadata holds an empty internal schema.
   */
  public InternalSchema getSchemaByVersionId(long versionId) {
    return resolve(configs::get, versionId);
  }

  /**
   * Returns the history as configuration entries, to ship it inside a configuration.
   */
  public Map<String, String> toConfigs() {
    return Collections.unmodifiableMap(configs);
  }

  /**
   * Returns whether the configuration holds a history written by {@link #toConfigs()}.
   *
   * @param getConfig returns the value of a configuration key, or {@code null} if absent
   */
  public static boolean isPresentIn(Function<String, String> getConfig) {
    return getConfig.apply(VERSIONS_KEY) != null;
  }

  /**
   * Same as {@link #getSchemaByVersionId} on a history written by {@link #toConfigs()} into a configuration,
   * reading only the entries needed for this version id.
   *
   * @param getConfig returns the value of a configuration key, or {@code null} if absent
   */
  public static InternalSchema resolve(Function<String, String> getConfig, long versionId) {
    String commitTime = String.valueOf(versionId);
    for (String entry : splitList(getConfig.apply(COMMITS_KEY))) {
      int separator = entry.indexOf('=');
      if (entry.substring(0, separator).equals(commitTime)) {
        String target = entry.substring(separator + 1);
        return target.charAt(0) == SCHEMA_REF_MARKER
            ? SerDeHelper.fromJson(getConfig.apply(SCHEMA_KEY_PREFIX + target.substring(1))).orElse(null)
            : parseSchema(getConfig.apply(VERSION_KEY_PREFIX + target));
      }
    }
    Long floorVersionId = null;
    for (String version : splitList(getConfig.apply(VERSIONS_KEY))) {
      long id = Long.parseLong(version);
      if (id <= versionId && (floorVersionId == null || id > floorVersionId)) {
        floorVersionId = id;
      }
    }
    return floorVersionId == null ? InternalSchema.getEmptyInternalSchema() : parseSchema(getConfig.apply(VERSION_KEY_PREFIX + floorVersionId));
  }

  /**
   * Returns whether the key is one {@link #toConfigs()} writes.
   */
  public static boolean isConfigKey(String key) {
    return key.startsWith(CONFIG_PREFIX);
  }

  private static InternalSchema parseSchema(String json) {
    return SerDeHelper.fromJson(json).orElse(InternalSchema.getEmptyInternalSchema());
  }

  private static List<String> splitList(String value) {
    return StringUtils.isNullOrEmpty(value) ? Collections.emptyList() : Arrays.asList(value.split(","));
  }

  /**
   * A commit time names a schema version only in its canonical numeric form, which is how
   * {@link InternalSchemaCache#getInternalSchemaByVersionId} matches it.
   */
  private static Option<Long> parseVersionId(String commitTime) {
    try {
      long versionId = Long.parseLong(commitTime);
      return String.valueOf(versionId).equals(commitTime) ? Option.of(versionId) : Option.empty();
    } catch (NumberFormatException e) {
      return Option.empty();
    }
  }

  /**
   * Returns what each commit file says about its schema, keyed by commit file name; a commit file that no longer
   * exists (archived after the timeline was loaded) is left out. Files not cached yet are read in parallel.
   */
  private static Map<String, CommitSchema> readCommitSchemas(HoodieTableMetaClient metaClient, List<String> commitFileNames) {
    Map<String, CommitSchema> result = new HashMap<>();
    List<StoragePath> toRead = new ArrayList<>();
    for (String commitFileName : commitFileNames) {
      StoragePath commitFile = new StoragePath(metaClient.getTimelinePath(), commitFileName);
      CommitSchema cached = COMMIT_SCHEMA_CACHE.getIfPresent(commitFile.toString());
      if (cached != null) {
        result.put(commitFileName, cached);
      } else {
        toRead.add(commitFile);
      }
    }
    if (toRead.isEmpty()) {
      return result;
    }
    ExecutorService pool = Executors.newFixedThreadPool(Math.min(toRead.size(), MAX_COMMIT_READ_PARALLELISM),
        new CustomizedThreadFactory("internal-schema-history-load", true));
    try {
      List<CompletableFuture<Option<CommitSchema>>> futures = toRead.stream()
          .map(commitFile -> CompletableFuture.supplyAsync(() -> readCommitSchema(metaClient, commitFile), pool))
          .collect(Collectors.toList());
      List<Option<CommitSchema>> commitSchemas = FutureUtils.allOf(futures).join();
      for (int i = 0; i < toRead.size(); i++) {
        if (commitSchemas.get(i).isPresent()) {
          StoragePath commitFile = toRead.get(i);
          if (commitSchemas.get(i).get().cacheable) {
            COMMIT_SCHEMA_CACHE.put(commitFile.toString(), commitSchemas.get(i).get());
          }
          result.put(commitFile.getName(), commitSchemas.get(i).get());
        }
      }
    } catch (CompletionException e) {
      throw e.getCause() instanceof RuntimeException ? (RuntimeException) e.getCause() : new HoodieException(e.getCause());
    } finally {
      pool.shutdownNow();
    }
    return result;
  }

  private static Option<CommitSchema> readCommitSchema(HoodieTableMetaClient metaClient, StoragePath commitFile) {
    byte[] content;
    try {
      content = InternalSchemaCache.readCommitFile(metaClient.getStorage(), commitFile);
    } catch (FileNotFoundException e) {
      log.warn("Commit file {} no longer exists, resolving its files from the schema history", commitFile);
      return Option.empty();
    } catch (IOException e) {
      throw new HoodieIOException("Could not read commit file " + commitFile, e);
    }
    HoodieCommitMetadata metadata;
    try {
      metadata = InternalSchemaCache.deserializeCommitMetadata(content, commitFile, metaClient.getTimelineLayout());
    } catch (Exception e) {
      log.warn("Cannot parse commit file {}, resolving its files from the schema history", commitFile, e);
      return Option.of(new CommitSchema(false, null, null, 0, false));
    }
    return Option.of(CommitSchema.of(metadata));
  }

  /**
   * The schema facts of one commit that {@link #load} needs: its {@code latest_schema}, or else the internal
   * schema converted from its Avro schema.
   */
  private static final class CommitSchema {
    private final boolean hasLatestSchema;
    private final InternalSchema latestSchema;
    private final InternalSchema avroSchema;
    private final int weight;
    // false when the commit file could not be parsed, so the next load reads it again
    private final boolean cacheable;

    private CommitSchema(boolean hasLatestSchema, InternalSchema latestSchema, InternalSchema avroSchema, int weight, boolean cacheable) {
      this.hasLatestSchema = hasLatestSchema;
      this.latestSchema = latestSchema;
      this.avroSchema = avroSchema;
      this.weight = weight;
      this.cacheable = cacheable;
    }

    private static CommitSchema of(HoodieCommitMetadata metadata) {
      String latestSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
      if (latestSchemaStr != null) {
        try {
          return new CommitSchema(true, SerDeHelper.fromJson(latestSchemaStr).orElse(null), null, latestSchemaStr.length(), true);
        } catch (RuntimeException e) {
          log.warn("Cannot parse the internal schema of a commit, resolving its files like a commit without one", e);
        }
      }
      String avroSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
      InternalSchema avroSchema = StringUtils.isNullOrEmpty(avroSchemaStr)
          ? null
          : InternalSchemaConverter.convert(HoodieSchemaUtils.createHoodieWriteSchema(avroSchemaStr, false));
      return new CommitSchema(false, null, avroSchema, avroSchemaStr == null ? 0 : avroSchemaStr.length(), latestSchemaStr == null);
    }
  }
}
