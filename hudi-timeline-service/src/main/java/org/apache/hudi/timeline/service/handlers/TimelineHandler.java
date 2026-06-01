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

package org.apache.hudi.timeline.service.handlers;

import java.io.IOException;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * REST Handler servicing timeline requests.
 */
public class TimelineHandler extends Handler {

  public TimelineHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                         FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
  }

  public List<InstantDTO> getLastInstant(String basePath) {
    return viewManager.getFileSystemView(basePath).getLastInstant().map(InstantDTO::fromInstant)
        .map(Arrays::asList).orElse(Collections.emptyList());
  }

  public TimelineDTO getTimeline(String basePath) {
    return TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }

  public org.apache.hudi.common.table.timeline.dto.v2.TimelineDTO getTimelineV2(String basePath) {
    return org.apache.hudi.common.table.timeline.dto.v2.TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }

  public Object getInstantDetails(String basePath, String requestedTime, String action, String state)
      throws IOException {
    HoodieTimeline hoodieTimeline = viewManager.getFileSystemView(basePath).getTimeline();
    try {
      HoodieInstant requestedInstant = new HoodieInstant(
          HoodieInstant.State.valueOf(state), action, requestedTime,
          InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);

      Object result;
      switch (requestedInstant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          result = hoodieTimeline.readCommitMetadata(requestedInstant);
          break;
        case HoodieTimeline.CLEAN_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readCleanMetadata(requestedInstant)
              : hoodieTimeline.readCleanerPlan(requestedInstant);
          break;
        case HoodieTimeline.ROLLBACK_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readRollbackMetadata(requestedInstant)
              : hoodieTimeline.readRollbackPlan(requestedInstant);
          break;
        case HoodieTimeline.RESTORE_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readRestoreMetadata(requestedInstant)
              : hoodieTimeline.readRestorePlan(requestedInstant);
          break;
        case HoodieTimeline.SAVEPOINT_ACTION:
          result = hoodieTimeline.readSavepointMetadata(requestedInstant);
          break;
        case HoodieTimeline.COMPACTION_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readCommitMetadata(requestedInstant)
              : hoodieTimeline.readCompactionPlan(requestedInstant);
          break;
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
        case HoodieTimeline.CLUSTERING_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readCommitMetadata(requestedInstant)
              : hoodieTimeline.readRequestedReplaceMetadata(requestedInstant);
          break;
        case HoodieTimeline.INDEXING_ACTION:
          result = requestedInstant.isCompleted()
              ? hoodieTimeline.readCommitMetadata(requestedInstant)
              : hoodieTimeline.readIndexPlan(requestedInstant);
          break;
        default:
          result = null;
          break;
      }

      // Avro-generated objects (SpecificRecordBase) cannot be serialized by
      // RequestHandler's ObjectMapper+AfterburnerModule due to module access
      // restrictions on Avro's internal Schema classes. Convert to plain Maps
      // using JsonUtils which accesses fields directly, bypassing getSchema().
      if (result instanceof SpecificRecordBase) {
        return JsonUtils.getObjectMapper().convertValue(result, Map.class);
      }
      return result;
    } catch (Exception e) {
      // Input parameters might be invalid
      return null;
    }
  }

  public Map<String, Object> getTableConfig(String basePath) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(conf.newInstance())
        .setBasePath(basePath)
        .build();
    TreeMap<String, String> sorted = new TreeMap<>();
    metaClient.getTableConfig().getProps().forEach((k, v) -> sorted.put(k.toString(), v.toString()));
    Map<String, Object> result = new HashMap<>();
    result.put("properties", sorted);
    return result;
  }

  public Map<String, Object> getSchemaHistory(String basePath, int limit) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(conf.newInstance())
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    Map<String, Object> result = new HashMap<>();

    // Get current schema
    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      result.put("currentSchema", schemaResolver.getTableSchema().toString());
    } catch (Exception e) {
      result.put("currentSchema", null);
    }

    // Get schema history from completed commits
    HoodieTimeline commitsTimeline = metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = commitsTimeline.getInstants();

    // Limit to most recent N instants for performance
    int startIdx = Math.max(0, instants.size() - limit);
    List<HoodieInstant> limitedInstants = instants.subList(startIdx, instants.size());

    List<Map<String, String>> history = new ArrayList<>();
    String previousSchema = null;

    for (HoodieInstant instant : limitedInstants) {
      try {
        HoodieCommitMetadata commitMetadata = commitsTimeline.readCommitMetadata(instant);
        String schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        if (schemaStr != null && !schemaStr.isEmpty() && !schemaStr.equals(previousSchema)) {
          Map<String, String> entry = new LinkedHashMap<>();
          entry.put("instant", instant.requestedTime());
          entry.put("completionTime", instant.getCompletionTime());
          entry.put("action", instant.getAction());
          entry.put("schema", schemaStr);
          history.add(entry);
          previousSchema = schemaStr;
        }
      } catch (Exception e) {
        // Skip instants whose metadata can't be read
      }
    }

    result.put("history", history);

    // Get internal schema history from .schema directory (richer evolution tracking)
    try {
      FileBasedInternalSchemaStorageManager schemaManager = new FileBasedInternalSchemaStorageManager(metaClient);
      String internalSchemaStr = schemaManager.getHistorySchemaStr();
      if (internalSchemaStr != null && !internalSchemaStr.isEmpty()) {
        result.put("internalSchemaHistory", internalSchemaStr);
      }
    } catch (Exception e) {
      // .schema directory may not exist for all tables
    }

    return result;
  }
}
