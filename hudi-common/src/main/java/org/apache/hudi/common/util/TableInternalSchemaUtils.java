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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.io.FileBaseInternalSchemasManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TableInternalSchemaUtils {
  // use segment lock to reduce competition.
  // the lock size should be powers of 2 for better hash.
  private static Object[] lockList = new Object[16];

  static {
    for (int i = 0; i < lockList.length; i++) {
      lockList[i] = new Object();
    }
  }

  // historySchemas cache maintain a map about (tablePath, HistorySchemas).
  // this is a Global cache, all threads in one container/executor share the same cache.
  private static final Cache<String, TreeMap<Long, InternalSchema>>
      HISTORICAL_SCHEMA_CACHE = Caffeine.newBuilder().maximumSize(1000).weakValues().build();

  /**
   * search internalSchema based on versionID.
   * first step: try to get internalSchema from hoodie commit files, we no need to add lock.
   * if we cannot get internalSchema by first step, then we try to get internalSchema from cache.
   *
   * @param versionID schema version_id need to search.
   * @param tablePath current hoodie table base path.
   * @param hadoopConf hadoopConf.
   * @return internalSchema.
   */
  public static InternalSchema searchSchemaAndCache(long versionID, String tablePath, Configuration hadoopConf) {
    Option<InternalSchema> candidateSchema = searchSchema(versionID, tablePath, hadoopConf);
    if (candidateSchema.isPresent()) {
      return candidateSchema.get();
    }
    // use segment lock to reduce competition.
    synchronized (lockList[tablePath.hashCode() & (lockList.length - 1)]) {
      TreeMap<Long, InternalSchema> historicalSchemas = HISTORICAL_SCHEMA_CACHE.getIfPresent(tablePath);
      if (historicalSchemas == null || SerDeHelper.searchSchema(versionID, historicalSchemas) == null) {
        historicalSchemas = getHistoricalSchemas(tablePath, hadoopConf);
        HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
      } else {
        long maxVersionId = historicalSchemas.keySet().stream().max(Long::compareTo).get();
        if (versionID > maxVersionId) {
          historicalSchemas = getHistoricalSchemas(tablePath, hadoopConf);
          HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
        }
      }
      return SerDeHelper.searchSchema(versionID, historicalSchemas);
    }
  }

  private static TreeMap<Long, InternalSchema> getHistoricalSchemas(String tablePath, Configuration hadoopConf) {
    TreeMap<Long, InternalSchema> result = new TreeMap<>();
    FileBaseInternalSchemasManager schemasManager = new FileBaseInternalSchemasManager(hadoopConf, new Path(tablePath));
    String historySchemaStr = schemasManager.getHistorySchemaStr();
    if (!StringUtils.isNullOrEmpty(historySchemaStr)) {
      result = SerDeHelper.parseSchemas(historySchemaStr);
    }
    return result;
  }

  private static Option<InternalSchema> searchSchema(long versionID, String tablePath, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath).setConf(hadoopConf).build();
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      List<HoodieInstant> instants = timeline.getInstants().filter(f -> f.getTimestamp().equals(String.valueOf(versionID))).collect(Collectors.toList());
      if (instants.isEmpty()) {
        return Option.empty();
      }
      byte[] data = timeline.getInstantDetails(instants.get(0)).get();
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
      String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATESTSCHEMA);
      return SerDeHelper.fromJson(latestInternalSchemaStr);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }
}

