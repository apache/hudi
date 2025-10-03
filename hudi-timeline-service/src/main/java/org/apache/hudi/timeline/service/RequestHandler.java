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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.marker.MarkerOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RemotePartitionHelper;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.handlers.BaseFileHandler;
import org.apache.hudi.timeline.service.handlers.FileSliceHandler;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;
import org.apache.hudi.timeline.service.handlers.RemotePartitionerHandler;
import org.apache.hudi.timeline.service.handlers.TimelineHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Main REST Handler class that handles and delegates calls to timeline relevant handlers.
 */
public class RequestHandler {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
  private static final TypeReference<List<String>> LIST_TYPE_REFERENCE = new TypeReference<List<String>>() {
  };

  private final TimelineService.Config timelineServiceConfig;
  private final FileSystemViewManager viewManager;
  private final Javalin app;
  private final TimelineHandler instantHandler;
  private final FileSliceHandler sliceHandler;
  private final BaseFileHandler dataFileHandler;
  private final MarkerHandler markerHandler;
  private RemotePartitionerHandler partitionerHandler;
  private final Registry metricsRegistry = Registry.getRegistry("TimelineService");
  private final ScheduledExecutorService asyncResultService;

  public RequestHandler(Javalin app, StorageConfiguration<?> conf, TimelineService.Config timelineServiceConfig,
                        FileSystemViewManager viewManager) {
    this.timelineServiceConfig = timelineServiceConfig;
    this.viewManager = viewManager;
    this.app = app;
    this.instantHandler = new TimelineHandler(conf, timelineServiceConfig, viewManager);
    this.sliceHandler = new FileSliceHandler(conf, timelineServiceConfig, viewManager);
    this.dataFileHandler = new BaseFileHandler(conf, timelineServiceConfig, viewManager);
    if (timelineServiceConfig.enableMarkerRequests) {
      this.markerHandler = new MarkerHandler(
          conf, timelineServiceConfig, viewManager, metricsRegistry);
    } else {
      this.markerHandler = null;
    }
    if (timelineServiceConfig.enableRemotePartitioner) {
      this.partitionerHandler = new RemotePartitionerHandler(conf, timelineServiceConfig, viewManager);
    }
    if (timelineServiceConfig.async) {
      this.asyncResultService = Executors.newSingleThreadScheduledExecutor();
    } else {
      this.asyncResultService = null;
    }
  }

  /**
   * Serializes the result into JSON String.
   *
   * @param ctx             Javalin context
   * @param obj             object to serialize
   * @param metricsRegistry {@code Registry} instance for storing metrics
   * @return JSON String from the input object
   * @throws JsonProcessingException
   */
  public static String jsonifyResult(
      Context ctx, Object obj, Registry metricsRegistry)
      throws JsonProcessingException {
    HoodieTimer timer = HoodieTimer.start();
    boolean prettyPrint = ctx.queryParam("pretty") != null;
    String result =
        prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
            : OBJECT_MAPPER.writeValueAsString(obj);
    final long jsonifyTime = timer.endTimer();
    metricsRegistry.add("WRITE_VALUE_CNT", 1);
    metricsRegistry.add("WRITE_VALUE_TIME", jsonifyTime);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Jsonify TimeTaken={}", jsonifyTime);
    }
    return result;
  }

  private static String getBasePathParam(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.BASEPATH_PARAM, String.class).getOrThrow(e -> new HoodieException("Basepath is invalid"));
  }

  private static String getPartitionParam(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.PARTITION_PARAM, String.class).getOrDefault("");
  }

  private static String getFileIdParam(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.FILEID_PARAM, String.class).getOrThrow(e -> new HoodieException("FILEID is invalid"));
  }

  private static List<String> getInstantsParam(Context ctx) {
    return Arrays.asList(ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.INSTANTS_PARAM, String.class).getOrThrow(e -> new HoodieException("INSTANTS_PARAM is invalid"))
        .split(RemoteHoodieTableFileSystemView.MULTI_VALUE_SEPARATOR));
  }

  private static String getMaxInstantParamMandatory(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM, String.class).getOrThrow(e -> new HoodieException("MAX_INSTANT_PARAM is invalid"));
  }

  private static String getCurrentInstantParamMandatory(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.CURRENT_INSTANT_PARAM, String.class).getOrThrow(e -> new HoodieException("MAX_INSTANT_PARAM is invalid"));
  }

  private static String getMaxInstantParamOptional(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM, String.class).getOrDefault("");
  }

  private static String getMinInstantParam(Context ctx) {
    return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.MIN_INSTANT_PARAM, String.class).getOrDefault("");
  }

  private static String getMarkerDirParam(Context ctx) {
    return ctx.queryParamAsClass(MarkerOperation.MARKER_DIR_PATH_PARAM, String.class).getOrDefault("");
  }

  private static boolean getIncludeFilesInPendingCompactionParam(Context ctx) {
    return Boolean.parseBoolean(
        ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM, String.class)
            .getOrThrow(e -> new HoodieException("INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM is invalid")));
  }

  public void register() {
    registerDataFilesAPI();
    registerFileSlicesAPI();
    registerTimelineAPI();
    if (markerHandler != null) {
      registerMarkerAPI();
    }
    if (partitionerHandler != null) {
      registerRemotePartitionerAPI();
    }
  }

  public void stop() {
    if (markerHandler != null) {
      markerHandler.stop();
    }
    if (asyncResultService != null) {
      asyncResultService.shutdown();
    }
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    if (timelineServiceConfig.async) {
      writeValueAsStringAsync(ctx, obj);
    } else {
      writeValueAsStringSync(ctx, obj);
    }
  }

  private void writeValueAsStringSync(Context ctx, Object obj) throws JsonProcessingException {
    String result = jsonifyResult(ctx, obj, metricsRegistry);
    ctx.result(result);
  }

  private void writeValueAsStringAsync(Context ctx, Object obj) {
    ctx.future(CompletableFuture.supplyAsync(() -> {
      try {
        return jsonifyResult(ctx, obj, metricsRegistry);
      } catch (JsonProcessingException e) {
        throw new HoodieException("Failed to JSON encode the value", e);
      }
    }, asyncResultService));
  }

  /**
   * Register Timeline API calls.
   */
  private void registerTimelineAPI() {
    app.get(RemoteHoodieTableFileSystemView.LAST_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LAST_INSTANT", 1);
      List<InstantDTO> dtos = instantHandler.getLastInstant(getBasePathParam(ctx));
      writeValueAsString(ctx, dtos);
    }, false));

    app.get(RemoteHoodieTableFileSystemView.TIMELINE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("TIMELINE", 1);
      TimelineDTO dto = instantHandler.getTimeline(getBasePathParam(ctx));
      writeValueAsString(ctx, dto);
    }, false));
  }

  /**
   * Register Data-Files API calls.
   */
  private void registerDataFilesAPI() {
    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFiles(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_DATA_FILE", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFile(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getFileIdParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_ALL_DATA_FILES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_ALL_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFiles(getBasePathParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILES_BEFORE_ON_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFilesBeforeOrOn(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getMaxInstantParamMandatory(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_LATEST_BASE_FILES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_LATEST_BASE_FILES_BEFORE_ON_INSTANT", 1);
      Map<String, List<BaseFileDTO>> dtos = dataFileHandler.getAllLatestDataFilesBeforeOrOn(
          getBasePathParam(ctx),
          getMaxInstantParamMandatory(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILE_ON_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFileOn(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.INSTANT_PARAM, String.class).get(),
          getFileIdParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_DATA_FILES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getAllDataFiles(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILES_RANGE_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFilesInRange(
          getBasePathParam(ctx),
          getInstantsParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));
  }

  /**
   * Register File Slices API calls.
   */
  private void registerFileSlicesAPI() {
    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlices(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICES_INFLIGHT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICES_INFLIGHT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlicesIncludingInflight(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICES_STATELESS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICES_STATELESS", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlicesStateless(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICE", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlice(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getFileIdParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_UNCOMPACTED_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_UNCOMPACTED_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestUnCompactedFileSlices(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getAllFileSlices(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICE_RANGE_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSliceInRange(
          getBasePathParam(ctx),
          getInstantsParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICES_MERGED_BEFORE_ON_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestMergedFileSlicesBeforeOrOn(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getMaxInstantParamMandatory(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_INFLIGHT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_INFLIGHT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getMaxInstantParamMandatory(ctx),
          getCurrentInstantParamMandatory(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICE_MERGED_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICE_MERGED_BEFORE_ON_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestMergedFileSliceBeforeOrOn(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getMaxInstantParamMandatory(ctx),
          getFileIdParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICES_BEFORE_ON_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlicesBeforeOrOn(
          getBasePathParam(ctx),
          getPartitionParam(ctx),
          getMaxInstantParamMandatory(ctx),
          getIncludeFilesInPendingCompactionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_LATEST_SLICES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_LATEST_SLICES_BEFORE_ON_INSTANT", 1);
      Map<String, List<FileSliceDTO>> dtos = sliceHandler.getAllLatestFileSlicesBeforeOrOn(
          getBasePathParam(ctx),
          getMaxInstantParamMandatory(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.PENDING_COMPACTION_OPS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("PEDING_COMPACTION_OPS", 1);
      List<CompactionOpDTO> dtos = sliceHandler.getPendingCompactionOperations(getBasePathParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.PENDING_LOG_COMPACTION_OPS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("PEDING_LOG_COMPACTION_OPS", 1);
      List<CompactionOpDTO> dtos = sliceHandler.getPendingLogCompactionOperations(getBasePathParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_FILEGROUPS_FOR_PARTITION_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_FILEGROUPS_FOR_PARTITION", 1);
      List<FileGroupDTO> dtos = sliceHandler.getAllFileGroups(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_FILEGROUPS_FOR_PARTITION_STATELESS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_FILEGROUPS_FOR_PARTITION_STATELESS", 1);
      List<FileGroupDTO> dtos = sliceHandler.getAllFileGroupsStateless(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.post(RemoteHoodieTableFileSystemView.REFRESH_TABLE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("REFRESH_TABLE", 1);
      boolean success = sliceHandler.refreshTable(getBasePathParam(ctx));
      writeValueAsString(ctx, success);
    }, false));

    app.post(RemoteHoodieTableFileSystemView.LOAD_PARTITIONS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LOAD_PARTITIONS", 1);
      String basePath = getBasePathParam(ctx);
      try {
        List<String> partitionPaths = OBJECT_MAPPER.readValue(ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.PARTITIONS_PARAM, String.class)
            .getOrThrow(e -> new HoodieException("Partitions param is invalid")), LIST_TYPE_REFERENCE);
        boolean success = sliceHandler.loadPartitions(basePath, partitionPaths);
        writeValueAsString(ctx, success);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse request parameter", e);
      }
    }, false));

    app.post(RemoteHoodieTableFileSystemView.LOAD_ALL_PARTITIONS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LOAD_ALL_PARTITIONS", 1);
      boolean success = sliceHandler.loadAllPartitions(getBasePathParam(ctx));
      writeValueAsString(ctx, success);
    }, false));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON", 1);
      List<FileGroupDTO> dtos = sliceHandler.getReplacedFileGroupsBeforeOrOn(
          getBasePathParam(ctx),
          getMaxInstantParamOptional(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_BEFORE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_BEFORE", 1);
      List<FileGroupDTO> dtos = sliceHandler.getReplacedFileGroupsBefore(
          getBasePathParam(ctx),
          getMaxInstantParamOptional(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_AFTER_OR_ON_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_AFTER_OR_ON", 1);
      List<FileGroupDTO> dtos = sliceHandler.getReplacedFileGroupsAfterOrOn(
          getBasePathParam(ctx),
          getMinInstantParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_PARTITION_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_PARTITION", 1);
      List<FileGroupDTO> dtos = sliceHandler.getAllReplacedFileGroups(
          getBasePathParam(ctx),
          getPartitionParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.PENDING_CLUSTERING_FILEGROUPS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("PENDING_CLUSTERING_FILEGROUPS", 1);
      List<ClusteringOpDTO> dtos = sliceHandler.getFileGroupsInPendingClustering(getBasePathParam(ctx));
      writeValueAsString(ctx, dtos);
    }, true));
  }

  private void registerMarkerAPI() {
    app.get(MarkerOperation.ALL_MARKERS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_MARKERS", 1);
      Set<String> markers = markerHandler.getAllMarkers(getMarkerDirParam(ctx));
      writeValueAsString(ctx, markers);
    }, false));

    app.get(MarkerOperation.CREATE_AND_MERGE_MARKERS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("CREATE_AND_MERGE_MARKERS", 1);
      Set<String> markers = markerHandler.getCreateAndMergeMarkers(getMarkerDirParam(ctx));
      writeValueAsString(ctx, markers);
    }, false));

    // Operation is used only for version 6 tables
    app.get(MarkerOperation.APPEND_MARKERS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("APPEND_MARKERS", 1);
      Set<String> markers = markerHandler.getAppendMarkers(
          ctx.queryParamAsClass(MarkerOperation.MARKER_DIR_PATH_PARAM, String.class).getOrDefault(""));
      writeValueAsString(ctx, markers);
    }, false));

    app.get(MarkerOperation.MARKERS_DIR_EXISTS_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("MARKERS_DIR_EXISTS", 1);
      boolean exist = markerHandler.doesMarkerDirExist(getMarkerDirParam(ctx));
      writeValueAsString(ctx, exist);
    }, false));

    app.post(MarkerOperation.CREATE_MARKER_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("CREATE_MARKER", 1);
      ctx.future(markerHandler.createMarker(
          ctx,
          getMarkerDirParam(ctx),
          ctx.queryParamAsClass(MarkerOperation.MARKER_NAME_PARAM, String.class).getOrDefault(""),
          ctx.queryParamAsClass(MarkerOperation.MARKER_BASEPATH_PARAM, String.class).getOrDefault("")));
    }, false));

    app.post(MarkerOperation.DELETE_MARKER_DIR_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("DELETE_MARKER_DIR", 1);
      boolean success = markerHandler.deleteMarkers(getMarkerDirParam(ctx));
      writeValueAsString(ctx, success);
    }, false));
  }

  private void registerRemotePartitionerAPI() {
    app.get(RemotePartitionHelper.URL, new ViewHandler(ctx -> {
      int partition = partitionerHandler.gePartitionIndex(
          ctx.queryParamAsClass(RemotePartitionHelper.NUM_BUCKETS_PARAM, String.class).getOrDefault(""),
          ctx.queryParamAsClass(RemotePartitionHelper.PARTITION_PATH_PARAM, String.class).getOrDefault(""),
          ctx.queryParamAsClass(RemotePartitionHelper.PARTITION_NUM_PARAM, String.class).getOrDefault(""));
      writeValueAsString(ctx, partition);
    }, false));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class ViewHandler implements Handler {

    private final Handler handler;
    private final boolean performRefreshCheck;
    private final UserGroupInformation ugi;

    ViewHandler(Handler handler, boolean performRefreshCheck) {
      this.handler = handler;
      this.performRefreshCheck = performRefreshCheck;
      try {
        ugi = UserGroupInformation.getCurrentUser();
      } catch (Exception e) {
        LOG.warn("Fail to get ugi", e);
        throw new HoodieException(e);
      }
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        boolean success = true;
        long beginTs = System.currentTimeMillis();
        boolean synced = false;
        boolean refreshCheck = performRefreshCheck && !isRefreshCheckDisabledInQuery(context);
        long refreshCheckTimeTaken = 0;
        long handleTimeTaken = 0;
        long finalCheckTimeTaken = 0;
        try {
          if (refreshCheck) {
            long beginRefreshCheck = System.currentTimeMillis();
            synced = syncIfLocalViewBehind(context);
            long endRefreshCheck = System.currentTimeMillis();
            refreshCheckTimeTaken = endRefreshCheck - beginRefreshCheck;
          }

          long handleBeginMs = System.currentTimeMillis();
          handler.handle(context);
          long handleEndMs = System.currentTimeMillis();
          handleTimeTaken = handleEndMs - handleBeginMs;

          if (refreshCheck) {
            long beginFinalCheck = System.currentTimeMillis();
            if (isLocalViewBehind(context)) {
              String lastKnownInstantFromClient = getLastInstantTsParam(context);
              String timelineHashFromClient = getTimelineHashParam(context);
              HoodieTimeline localTimeline =
                  viewManager.getFileSystemView(context.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM)).getTimeline();
              if (shouldThrowExceptionIfLocalViewBehind(localTimeline, timelineHashFromClient)) {
                String errMsg = String.format("Last known instant from client was %s but server has the following timeline %s",
                        lastKnownInstantFromClient, localTimeline.getInstants());
                throw new BadRequestResponse(errMsg);
              }
            }
            long endFinalCheck = System.currentTimeMillis();
            finalCheckTimeTaken = endFinalCheck - beginFinalCheck;
          }
        } catch (RuntimeException re) {
          success = false;
          if (re instanceof BadRequestResponse) {
            LOG.warn("Bad request response due to client view behind server view. {}", re.getMessage());
          } else {
            LOG.error(String.format("Got runtime exception servicing request %s", context.queryString()), re);
          }
          throw re;
        } finally {
          long endTs = System.currentTimeMillis();
          long timeTakenMillis = endTs - beginTs;
          metricsRegistry.add("TOTAL_API_TIME", timeTakenMillis);
          metricsRegistry.add("TOTAL_REFRESH_TIME", refreshCheckTimeTaken);
          metricsRegistry.add("TOTAL_HANDLE_TIME", handleTimeTaken);
          metricsRegistry.add("TOTAL_CHECK_TIME", finalCheckTimeTaken);
          metricsRegistry.add("TOTAL_API_CALLS", 1);

          if (LOG.isDebugEnabled()) {
            LOG.debug("TimeTakenMillis[Total={}, Refresh={}, handle={}, Check={}], Success={}, Query={}, Host={}, synced={}",
                    timeTakenMillis, refreshCheckTimeTaken, handleTimeTaken, finalCheckTimeTaken, success, context.queryString(), context.host(), synced);
          }
        }
        return null;
      });
    }

    /**
     * Determines if local view of table's timeline is behind that of client's view.
     */
    private boolean isLocalViewBehind(Context ctx) {
      String basePath = ctx.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM);
      String lastKnownInstantFromClient = getLastInstantTsParam(ctx);
      String timelineHashFromClient = getTimelineHashParam(ctx);
      HoodieTimeline localTimeline =
          viewManager.getFileSystemView(basePath).getTimeline().filterCompletedOrMajorOrMinorCompactionInstants();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Client [ LastTs={}, TimelineHash={}], localTimeline={}",lastKnownInstantFromClient, timelineHashFromClient, localTimeline.getInstants());
      }

      if ((!localTimeline.getInstantsAsStream().findAny().isPresent())
          && HoodieTimeline.INVALID_INSTANT_TS.equals(lastKnownInstantFromClient)) {
        return false;
      }

      String localTimelineHash = localTimeline.getTimelineHash();
      // refresh if timeline hash mismatches
      if (!localTimelineHash.equals(timelineHashFromClient)) {
        return true;
      }

      // As a safety check, even if hash is same, ensure instant is present
      return !localTimeline.containsOrBeforeTimelineStarts(lastKnownInstantFromClient);
    }

    /**
     * Syncs data-set view if local view is behind.
     */
    private boolean syncIfLocalViewBehind(Context ctx) {
      String basePath = ctx.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM);
      SyncableFileSystemView view = viewManager.getFileSystemView(basePath);
      synchronized (view) {
        if (isLocalViewBehind(ctx)) {
          String lastKnownInstantFromClient = getLastInstantTsParam(ctx);
          HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline();
          if (LOG.isInfoEnabled()) {
            LOG.info("Syncing view as client passed last known instant {} as last known instant but server has the following last instant on timeline: {}",
                lastKnownInstantFromClient, localTimeline.lastInstant());
          }
          view.sync();
          return true;
        }
      }
      return false;
    }

    /**
     * Determine whether to throw an exception when local view of table's timeline is behind that of client's view.
     */
    private boolean shouldThrowExceptionIfLocalViewBehind(HoodieTimeline localTimeline, String timelineHashFromClient) {
      Option<HoodieInstant> lastInstant = localTimeline.lastInstant();
      // When performing async clean, we may have one more .clean.completed after lastInstantTs.
      // In this case, we do not need to throw an exception.
      return !lastInstant.isPresent() || !lastInstant.get().getAction().equals(HoodieTimeline.CLEAN_ACTION)
          || !localTimeline.findInstantsBefore(lastInstant.get().requestedTime()).getTimelineHash().equals(timelineHashFromClient);
    }

    private boolean isRefreshCheckDisabledInQuery(Context ctx) {
      return Boolean.parseBoolean(ctx.queryParam(RemoteHoodieTableFileSystemView.REFRESH_OFF));
    }

    private String getLastInstantTsParam(Context ctx) {
      return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.LAST_INSTANT_TS, String.class).getOrDefault(HoodieTimeline.INVALID_INSTANT_TS);
    }

    private String getTimelineHashParam(Context ctx) {
      return ctx.queryParamAsClass(RemoteHoodieTableFileSystemView.TIMELINE_HASH, String.class).getOrDefault("");
    }
  }
}
