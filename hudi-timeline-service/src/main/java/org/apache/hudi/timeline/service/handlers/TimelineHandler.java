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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.timeline.dto.ui.UiTimelineDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import io.javalin.http.BadRequestResponse;
import io.javalin.http.NotFoundResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.InputStream;
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
@Slf4j
public class TimelineHandler extends Handler {

  public TimelineHandler(StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                         FileSystemViewManager viewManager) {
    super(conf, timelineServiceConfig, viewManager);
  }

  /**
   * Owns and closes the instant content stream so deserialization failures cannot leak the handle.
   */
  @FunctionalInterface
  private interface InstantStreamReader<T> {
    T apply(InputStream in) throws IOException;
  }

  // A fresh metaClient is built per request rather than cached: a cached metaClient is permanently
  // stale, and at human-click frequency a per-request build is cheap and always current.
  private HoodieTableMetaClient createMetaClient(String basePath) {
    try {
      return HoodieTableMetaClient.builder()
          .setConf(conf.newInstance())
          .setBasePath(basePath)
          .build();
    } catch (TableNotFoundException e) {
      throw new BadRequestResponse("Not a valid Hudi table path: " + basePath);
    }
  }

  private <T> T readInstantContent(HoodieTimeline timeline, HoodieInstant instant, InstantStreamReader<T> reader)
      throws IOException {
    try (InputStream in = timeline.getInstantContentStream(instant)) {
      return reader.apply(in);
    }
  }

  private HoodieCommitMetadata readCommitMetadata(CommitMetadataSerDe serde, HoodieTimeline timeline,
                                                  HoodieInstant instant) throws IOException {
    return readInstantContent(timeline, instant,
        in -> serde.deserialize(instant, in, () -> timeline.isEmpty(instant), HoodieCommitMetadata.class));
  }

  private <T> T readNonEmpty(CommitMetadataSerDe serde, HoodieTimeline timeline, HoodieInstant instant, Class<T> clazz)
      throws IOException {
    return readInstantContent(timeline, instant, in -> serde.deserialize(instant, in, () -> false, clazz));
  }

  public List<InstantDTO> getLastInstant(String basePath) {
    return viewManager.getFileSystemView(basePath).getLastInstant().map(InstantDTO::fromInstant)
        .map(Arrays::asList).orElse(Collections.emptyList());
  }

  public TimelineDTO getTimeline(String basePath) {
    return TimelineDTO.fromTimeline(viewManager.getFileSystemView(basePath).getTimeline());
  }

  public UiTimelineDTO getUiTimeline(String basePath) {
    // The active timeline is used, not the file-system-view write timeline: the latter drops
    // clean/rollback/savepoint/restore/indexing actions and all requested/inflight states.
    return UiTimelineDTO.fromTimeline(createMetaClient(basePath).getActiveTimeline());
  }

  public Object getInstantDetails(String basePath, String requestedTime, String action, String state) {
    HoodieInstant.State parsedState;
    try {
      parsedState = HoodieInstant.State.valueOf(state);
    } catch (IllegalArgumentException e) {
      throw new BadRequestResponse("Invalid instant state: " + state);
    }

    HoodieTableMetaClient metaClient = createMetaClient(basePath);
    HoodieTimeline activeTimeline = metaClient.getActiveTimeline();
    CommitMetadataSerDe serde = metaClient.getCommitMetadataSerDe();

    // Resolve the instant against the timeline rather than constructing it from request params:
    // an attacker-controlled instant would otherwise flow into a StoragePath whose URI.normalize
    // collapses ".." segments, enabling path traversal.
    HoodieInstant instant = activeTimeline.getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(requestedTime)
            && i.getAction().equals(action)
            && i.getState() == parsedState)
        .findFirst()
        .orElseThrow(() -> new NotFoundResponse(
            "Instant not found in active timeline: " + requestedTime + " " + action + " " + parsedState));

    try {
      Object result;
      switch (instant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          result = readCommitMetadata(serde, activeTimeline, instant);
          break;
        case HoodieTimeline.CLEAN_ACTION:
          result = instant.isCompleted()
              ? readNonEmpty(serde, activeTimeline, instant, HoodieCleanMetadata.class)
              : readNonEmpty(serde, activeTimeline, instant, HoodieCleanerPlan.class);
          break;
        case HoodieTimeline.ROLLBACK_ACTION:
          result = instant.isCompleted()
              ? readNonEmpty(serde, activeTimeline, instant, HoodieRollbackMetadata.class)
              : readNonEmpty(serde, activeTimeline, instant, HoodieRollbackPlan.class);
          break;
        case HoodieTimeline.RESTORE_ACTION:
          result = instant.isCompleted()
              ? readNonEmpty(serde, activeTimeline, instant, HoodieRestoreMetadata.class)
              : readNonEmpty(serde, activeTimeline, instant, HoodieRestorePlan.class);
          break;
        case HoodieTimeline.SAVEPOINT_ACTION:
          result = readNonEmpty(serde, activeTimeline, instant, HoodieSavepointMetadata.class);
          break;
        case HoodieTimeline.COMPACTION_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
          result = instant.isCompleted()
              ? readCommitMetadata(serde, activeTimeline, instant)
              : readNonEmpty(serde, activeTimeline, instant, HoodieCompactionPlan.class);
          break;
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
        case HoodieTimeline.CLUSTERING_ACTION:
          result = instant.isCompleted()
              ? readCommitMetadata(serde, activeTimeline, instant)
              : readNonEmpty(serde, activeTimeline, instant, HoodieRequestedReplaceMetadata.class);
          break;
        case HoodieTimeline.INDEXING_ACTION:
          result = instant.isCompleted()
              ? readCommitMetadata(serde, activeTimeline, instant)
              : readNonEmpty(serde, activeTimeline, instant, HoodieIndexPlan.class);
          break;
        default:
          throw new BadRequestResponse("Unsupported action: " + action);
      }

      // Avro-generated objects (SpecificRecordBase) cannot be serialized by
      // RequestHandler's ObjectMapper+AfterburnerModule due to module access
      // restrictions on Avro's internal Schema classes. Convert to plain Maps
      // using JsonUtils which accesses fields directly, bypassing getSchema().
      if (result instanceof SpecificRecordBase) {
        return JsonUtils.getObjectMapper().convertValue(result, Map.class);
      }
      return result;
    } catch (BadRequestResponse | NotFoundResponse e) {
      throw e;
    } catch (Exception e) {
      log.warn("Failed to read instant details for basePath={}, requestedTime={}, action={}, state={}",
          basePath, requestedTime, action, state, e);
      throw new HoodieException("Failed to read instant details", e);
    }
  }

  public Map<String, Object> getTableConfig(String basePath) {
    HoodieTableMetaClient metaClient = createMetaClient(basePath);
    TreeMap<String, String> sorted = new TreeMap<>();
    metaClient.getTableConfig().getProps().forEach((k, v) -> sorted.put(k.toString(), v.toString()));
    Map<String, Object> result = new HashMap<>();
    result.put("properties", sorted);
    return result;
  }

  public Map<String, Object> getSchemaHistory(String basePath, int limit) {
    HoodieTableMetaClient metaClient = createMetaClient(basePath);
    CommitMetadataSerDe serde = metaClient.getCommitMetadataSerDe();

    Map<String, Object> result = new HashMap<>();

    // Non-throwing accessor: a table with no commits yields null rather than a 500.
    result.put("currentSchema",
        new TableSchemaResolver(metaClient)
            .getTableSchemaIfPresent(metaClient.getTableConfig().populateMetaFields())
            .map(schema -> schema.toString())
            .orElse(null));

    HoodieTimeline commitsTimeline = metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = commitsTimeline.getInstants();

    // Scan only the most recent N instants for performance.
    int startIdx = Math.max(0, instants.size() - limit);
    List<HoodieInstant> scanned = instants.subList(startIdx, instants.size());

    List<Map<String, String>> history = new ArrayList<>();
    String previousSchema = null;

    for (HoodieInstant instant : scanned) {
      try {
        HoodieCommitMetadata commitMetadata = readCommitMetadata(serde, commitsTimeline, instant);
        String schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
        if (schemaStr != null && !schemaStr.isEmpty() && !schemaStr.equals(previousSchema)) {
          Map<String, String> entry = new LinkedHashMap<>();
          entry.put("type", previousSchema == null ? "baseline" : "change");
          entry.put("instant", instant.requestedTime());
          entry.put("completionTime", instant.getCompletionTime());
          entry.put("action", instant.getAction());
          entry.put("schema", schemaStr);
          history.add(entry);
          previousSchema = schemaStr;
        }
      } catch (Exception e) {
        log.debug("Skipping instant {} whose commit metadata could not be read", instant, e);
      }
    }

    result.put("history", history);

    Map<String, Object> window = new HashMap<>();
    window.put("oldestInstantScanned", scanned.isEmpty() ? null : scanned.get(0).requestedTime());
    window.put("truncated", startIdx > 0);
    result.put("window", window);

    // Richer evolution tracking from the .schema directory (may be absent for many tables).
    try {
      String internalSchemaStr = new FileBasedInternalSchemaStorageManager(metaClient).getHistorySchemaStr();
      if (internalSchemaStr != null && !internalSchemaStr.isEmpty()) {
        result.put("internalSchemaHistory", internalSchemaStr);
      }
    } catch (Exception e) {
      log.warn("Failed to read internal schema history for basePath={}", basePath, e);
    }

    return result;
  }
}
