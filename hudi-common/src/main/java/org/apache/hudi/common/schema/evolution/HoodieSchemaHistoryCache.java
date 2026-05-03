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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

/**
 * HoodieSchema-shaped façade over the global schema-history cache. Delegates to
 * {@link InternalSchemaCache} so the underlying segment-locked Caffeine cache is
 * shared across the legacy and new code paths during migration — there's only one
 * cache per JVM, regardless of which API the caller used.
 *
 * <p>String-based entry points (commit-metadata blob lookup) pass through
 * unchanged; methods that previously returned {@link InternalSchema} now return
 * {@link HoodieSchema} via {@link HoodieSchemaInternalSchemaBridge}, with field
 * ids preserved.</p>
 */
public final class HoodieSchemaHistoryCache {

  private HoodieSchemaHistoryCache() {
  }

  /**
   * Resolves a schema by version id, hitting the cache when possible and falling
   * back to the schema-history files on disk. Returns null when no schema with
   * that id exists for the table.
   */
  public static HoodieSchema searchSchemaAndCache(long versionId, HoodieTableMetaClient metaClient) {
    InternalSchema internal = InternalSchemaCache.searchSchemaAndCache(versionId, metaClient);
    return wrap(internal);
  }

  /**
   * Looks up the internalSchema and avroSchema strings carried in the latest
   * commit before the given clustering/compaction instant. Pass-through —
   * already string-based, no schema-type conversion.
   */
  public static Pair<Option<String>, Option<String>> getInternalSchemaAndAvroSchemaForClusteringAndCompaction(
      HoodieTableMetaClient metaClient, String compactionAndClusteringInstant) {
    return InternalSchemaCache.getInternalSchemaAndAvroSchemaForClusteringAndCompaction(
        metaClient, compactionAndClusteringInstant);
  }

  /**
   * Schema lookup intended for the Spark task path: avoids constructing a full
   * {@link HoodieTableMetaClient} since metaClient init is expensive at task scope.
   * Caller supplies the timeline layout (and optionally the table config); both
   * are needed to resolve commit-file paths and instant naming.
   */
  public static HoodieSchema getSchemaByVersionId(long versionId,
                                                  String tablePath,
                                                  HoodieStorage storage,
                                                  String validCommits,
                                                  TimelineLayout timelineLayout) {
    InternalSchema internal = InternalSchemaCache.getInternalSchemaByVersionId(
        versionId, tablePath, storage, validCommits, timelineLayout);
    return wrap(internal);
  }

  public static HoodieSchema getSchemaByVersionId(long versionId,
                                                  String tablePath,
                                                  HoodieStorage storage,
                                                  String validCommits,
                                                  TimelineLayout timelineLayout,
                                                  HoodieTableConfig tableConfig) {
    InternalSchema internal = InternalSchemaCache.getInternalSchemaByVersionId(
        versionId, tablePath, storage, validCommits, timelineLayout, tableConfig);
    return wrap(internal);
  }

  /**
   * Schema lookup using a metaClient. Convenient when the caller already has one.
   */
  public static HoodieSchema getSchemaByVersionId(long versionId, HoodieTableMetaClient metaClient) {
    InternalSchema internal = InternalSchemaCache.getInternalSchemaByVersionId(versionId, metaClient);
    return wrap(internal);
  }

  private static HoodieSchema wrap(InternalSchema internal) {
    if (internal == null) {
      return null;
    }
    String recordName = (internal.getRecord() != null && internal.getRecord().name() != null)
        ? internal.getRecord().name() : "hoodieSchema";
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(internal, recordName);
  }
}
