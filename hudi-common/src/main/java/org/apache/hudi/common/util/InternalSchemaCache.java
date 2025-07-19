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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * An internal cache implementation for managing different version of schemas.
 * This is a Global cache; all threads in one container/executor share the same cache.
 * A map of (tablePath, HistorySchemas) is maintained.
 */
public class InternalSchemaCache {
  private static final Logger LOG = LoggerFactory.getLogger(InternalSchemaCache.class);
  // Use segment lock to reduce competition.
  // the lock size should be powers of 2 for better hash.
  private static final Object[] LOCK_LIST = new Object[16];

  static {
    for (int i = 0; i < LOCK_LIST.length; i++) {
      LOCK_LIST[i] = new Object();
    }
  }

  // historySchemas cache maintain a map about (tablePath, HistorySchemas).
  // this is a Global cache, all threads in one container/executor share the same cache.
  private static final Cache<String, TreeMap<Long, InternalSchema>>
      HISTORICAL_SCHEMA_CACHE = Caffeine.newBuilder().maximumSize(1000).weakValues().build();

  /**
   * Search internalSchema based on versionID.
   *
   * <p>The internalSchema is fetched from cache or history schema file directly, and should not
   * be fetched from commit meta file since the overhead of scanning timeline is higher.
   *
   * @param versionID  schema version_id need to search
   * @param metaClient current hoodie metaClient
   * @return internalSchema
   */
  public static InternalSchema searchSchemaAndCache(long versionID, HoodieTableMetaClient metaClient) {
    String tablePath = metaClient.getBasePath().toString();
    // use segment lock to reduce competition.
    synchronized (LOCK_LIST[tablePath.hashCode() & (LOCK_LIST.length - 1)]) {
      TreeMap<Long, InternalSchema> historicalSchemas = HISTORICAL_SCHEMA_CACHE.getIfPresent(tablePath);
      if (historicalSchemas == null || InternalSchemaUtils.searchSchema(versionID, historicalSchemas) == null) {
        historicalSchemas = getHistoricalSchemas(metaClient);
        HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
      } else {
        long maxVersionId = historicalSchemas.keySet().stream().max(Long::compareTo).get();
        if (versionID > maxVersionId) {
          historicalSchemas = getHistoricalSchemas(metaClient);
          HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
        }
      }
      return InternalSchemaUtils.searchSchema(versionID, historicalSchemas);
    }
  }

  private static TreeMap<Long, InternalSchema> getHistoricalSchemas(HoodieTableMetaClient metaClient) {
    TreeMap<Long, InternalSchema> result = new TreeMap<>();
    FileBasedInternalSchemaStorageManager schemasManager = new FileBasedInternalSchemaStorageManager(metaClient);
    String historySchemaStr = schemasManager.getHistorySchemaStr();
    if (!StringUtils.isNullOrEmpty(historySchemaStr)) {
      result = SerDeHelper.parseSchemas(historySchemaStr);
    }
    return result;
  }

  /**
   * Get internalSchema and avroSchema for compaction/cluster operation.
   *
   * @param metaClient                     current hoodie metaClient
   * @param compactionAndClusteringInstant first instant before current compaction/cluster instant
   * @return (internalSchemaStrOpt, avroSchemaStrOpt) a pair of InternalSchema/avroSchema
   */
  public static Pair<Option<String>, Option<String>> getInternalSchemaAndAvroSchemaForClusteringAndCompaction(HoodieTableMetaClient metaClient, String compactionAndClusteringInstant) {
    // try to load internalSchema to support Schema Evolution
    HoodieTimeline timelineBeforeCurrentCompaction = metaClient.getCommitsAndCompactionTimeline().findInstantsBefore(compactionAndClusteringInstant).filterCompletedInstants();
    Option<HoodieInstant> lastInstantBeforeCurrentCompaction = timelineBeforeCurrentCompaction.lastInstant();
    if (lastInstantBeforeCurrentCompaction.isPresent()) {
      // try to find internalSchema
      HoodieCommitMetadata metadata;
      try {
        metadata = timelineBeforeCurrentCompaction.readCommitMetadata(lastInstantBeforeCurrentCompaction.get());
      } catch (Exception e) {
        throw new HoodieException(String.format("cannot read metadata from commit: %s", lastInstantBeforeCurrentCompaction.get()), e);
      }
      String internalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
      if (internalSchemaStr != null) {
        String existingSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        return Pair.of(Option.of(internalSchemaStr), Option.of(existingSchemaStr));
      }
    }
    return Pair.of(Option.empty(), Option.empty());
  }

  /**
   * Give a schema versionId return its internalSchema.
   * This method will be called by spark tasks, we should minimize time cost.
   * We try our best to not use metaClient， since the initialization of metaClient is time cost
   * step1：
   * try to parser internalSchema from HoodieInstant directly
   * step2：
   * if we cannot parser internalSchema in step1， (eg: current versionId HoodieInstant has been archived)
   * try to find internalSchema in historySchema.
   * step3:
   * if we cannot parser internalSchema in step2  (eg: schema evolution is not enabled when we create hoodie table, however after some inserts we enable schema evolution)
   * try to convert table schema to internalSchema.
   *
   * @param versionId           the internalSchema version to be search.
   * @param tablePath           table path
   * @param storage             {@link HoodieStorage} instance.
   * @param validCommits        current validate commits, use to make up the commit file path/verify the validity of the history schema files
   * @param timelineLayout      {@link TimelineLayout} instance, used to get {@link InstantFileNameParser}/{@link CommitMetadataSerDe}/{@link InstantGenerator}/{@link TimelinePathProvider} instance.
   * @param tableConfig         {@link HoodieTableConfig} instance, used to get the timeline path.
   * @return a internalSchema.
   */
  public static InternalSchema getInternalSchemaByVersionId(long versionId, String tablePath, HoodieStorage storage, String validCommits,
                                                            TimelineLayout timelineLayout, HoodieTableConfig tableConfig) {
    InstantFileNameParser fileNameParser = timelineLayout.getInstantFileNameParser();
    CommitMetadataSerDe commitMetadataSerDe = timelineLayout.getCommitMetadataSerDe();
    InstantGenerator instantGenerator = timelineLayout.getInstantGenerator();
    TimelinePathProvider timelinePathProvider = timelineLayout.getTimelinePathProvider();
    StoragePath timelinePath = timelinePathProvider.getTimelinePath(tableConfig, new StoragePath(tablePath));

    String avroSchema = "";
    Set<String> commitSet = Arrays.stream(validCommits.split(",")).collect(Collectors.toSet());
    List<String> validateCommitList =
        commitSet.stream().map(fileNameParser::extractTimestamp).collect(Collectors.toList());

    //step1:
    StoragePath candidateCommitFile = commitSet.stream()
        .filter(fileName -> fileNameParser.extractTimestamp(fileName).equals(versionId + ""))
        .findFirst().map(f -> new StoragePath(timelinePath, f)).orElse(null);
    if (candidateCommitFile != null) {
      try {
        HoodieCommitMetadata metadata;
        try (InputStream is = storage.open(candidateCommitFile)) {
          metadata = commitMetadataSerDe.deserialize(instantGenerator.createNewInstant(
                  new StoragePathInfo(candidateCommitFile, -1, false, (short) 0, 0L, 0L)),
              is, () -> false, HoodieCommitMetadata.class);
        } catch (IOException e) {
          throw e;
        }
        String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
        avroSchema = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        if (latestInternalSchemaStr != null) {
          return SerDeHelper.fromJson(latestInternalSchemaStr).orElse(null);
        }
      } catch (Exception e1) {
        // swallow this exception.
        LOG.warn("Cannot find internal schema from commit file {}. Falling back to parsing historical internal schema", candidateCommitFile);
      }
    }
    // step2:
    FileBasedInternalSchemaStorageManager fileBasedInternalSchemaStorageManager =
        new FileBasedInternalSchemaStorageManager(storage, new StoragePath(tablePath));
    String latestHistorySchema =
        fileBasedInternalSchemaStorageManager.getHistorySchemaStrByGivenValidCommits(validateCommitList);
    if (latestHistorySchema.isEmpty()) {
      return InternalSchema.getEmptyInternalSchema();
    }
    InternalSchema fileSchema =
        InternalSchemaUtils.searchSchema(versionId, SerDeHelper.parseSchemas(latestHistorySchema));
    // step3:
    return fileSchema.isEmptySchema()
        ? StringUtils.isNullOrEmpty(avroSchema)
        ? InternalSchema.getEmptyInternalSchema()
        : AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(avroSchema)))
        : fileSchema;
  }

  public static InternalSchema getInternalSchemaByVersionId(long versionId, String tablePath, HoodieStorage storage, String validCommits, TimelineLayout timelineLayout) {
    return getInternalSchemaByVersionId(versionId, tablePath, storage, validCommits, timelineLayout, HoodieTableConfig.loadFromHoodieProps(storage, tablePath));
  }

  public static InternalSchema getInternalSchemaByVersionId(long versionId, HoodieTableMetaClient metaClient) {
    InstantFileNameGenerator factory = metaClient.getInstantFileNameGenerator();
    String validCommitLists = metaClient
        .getCommitsAndCompactionTimeline().filterCompletedInstants().getInstantsAsStream().map(factory::getFileName).collect(Collectors.joining(","));
    return getInternalSchemaByVersionId(versionId, metaClient.getBasePath().toString(), metaClient.getStorage(),
        validCommitLists, metaClient.getTimelineLayout(), metaClient.getTableConfig());
  }
}

