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

package com.uber.hoodie.timeline.service;

import static com.uber.hoodie.common.table.view.RemoteHoodieTableFileSystemView.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.SyncableFileSystemView;
import com.uber.hoodie.common.table.timeline.dto.CompactionOpDTO;
import com.uber.hoodie.common.table.timeline.dto.DataFileDTO;
import com.uber.hoodie.common.table.timeline.dto.FileGroupDTO;
import com.uber.hoodie.common.table.timeline.dto.FileSliceDTO;
import com.uber.hoodie.common.table.timeline.dto.InstantDTO;
import com.uber.hoodie.common.table.timeline.dto.TimelineDTO;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.timeline.service.handlers.DataFileHandler;
import com.uber.hoodie.timeline.service.handlers.FileSliceHandler;
import com.uber.hoodie.timeline.service.handlers.TimelineHandler;
import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Main REST Handler class that handles local view staleness and delegates calls to slice/data-file/timeline handlers
 */
public class FileSystemViewHandler {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger logger = LogManager.getLogger(FileSystemViewHandler.class);

  private final FileSystemViewManager viewManager;
  private final Javalin app;
  private final Configuration conf;
  private final TimelineHandler instantHandler;
  private final FileSliceHandler sliceHandler;
  private final DataFileHandler dataFileHandler;

  public FileSystemViewHandler(Javalin app, Configuration conf, FileSystemViewManager viewManager) throws IOException {
    this.viewManager = viewManager;
    this.app = app;
    this.conf = conf;
    this.instantHandler = new TimelineHandler(conf, viewManager);
    this.sliceHandler = new FileSliceHandler(conf, viewManager);
    this.dataFileHandler = new DataFileHandler(conf, viewManager);
  }

  public void register() {
    registerDataFilesAPI();
    registerFileSlicesAPI();
    registerTimelineAPI();
  }

  /**
   * Determines if local view of dataset's timeline is behind that of client's view
   * @param ctx
   * @return
   */
  private boolean isLocalViewBehind(Context ctx) {
    String basePath = ctx.queryParam(BASEPATH_PARAM);
    String lastKnownInstantFromClient = ctx.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT_TS);
    String timelineHashFromClient = ctx.queryParam(TIMELINE_HASH, "");
    HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline()
        .filterCompletedAndCompactionInstants();
    if (logger.isDebugEnabled()) {
      logger.debug("Client [ LastTs=" + lastKnownInstantFromClient
          + ", TimelineHash=" + timelineHashFromClient + "], localTimeline="
          + localTimeline.getInstants().collect(Collectors.toList()));
    }

    if ((localTimeline.getInstants().count() == 0)
        && lastKnownInstantFromClient.equals(HoodieTimeline.INVALID_INSTANT_TS)) {
      return false;
    }

    String localTimelineHash = localTimeline.getTimelineHash();
    if (!localTimelineHash.equals(timelineHashFromClient)) {
      return true;
    }

    // As a safety check, even if hash is same, ensure instant is present
    return !localTimeline.containsOrBeforeTimelineStarts(lastKnownInstantFromClient);
  }

  /**
   * Syncs data-set view if local view is behind
   * @param ctx
   */
  private boolean syncIfLocalViewBehind(Context ctx) {
    if (isLocalViewBehind(ctx)) {
      String basePath = ctx.queryParam(BASEPATH_PARAM);
      String lastKnownInstantFromClient =
          ctx.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT_TS);
      SyncableFileSystemView view = viewManager.getFileSystemView(basePath);
      synchronized (view) {
        if (isLocalViewBehind(ctx)) {
          HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline();
          logger.warn("Syncing view as client passed last known instant " + lastKnownInstantFromClient
              + " as last known instant but server has the folling timeline :"
              + localTimeline.getInstants().collect(Collectors.toList()));
          view.sync();
          return true;
        }
      }
    }
    return false;
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    boolean prettyPrint = ctx.queryParam("pretty") != null ? true : false;
    long beginJsonTs = System.currentTimeMillis();
    String result = prettyPrint ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
        : mapper.writeValueAsString(obj);
    long endJsonTs = System.currentTimeMillis();
    logger.debug("Jsonify TimeTaken=" + (endJsonTs - beginJsonTs));
    ctx.result(result);
  }

  /**
   * Register Timeline API calls
   */
  private void registerTimelineAPI() {
    app.get(LAST_INSTANT, new ViewHandler(ctx -> {
      List<InstantDTO> dtos = instantHandler.getLastInstant(ctx.validatedQueryParam(BASEPATH_PARAM).getValue());
      writeValueAsString(ctx, dtos);
    }, false));

    app.get(TIMELINE, new ViewHandler(ctx -> {
      TimelineDTO dto = instantHandler.getTimeline(ctx.validatedQueryParam(BASEPATH_PARAM).getValue());
      writeValueAsString(ctx, dto);
    }, false));
  }

  /**
   * Register Data-Files API calls
   */
  private void registerDataFilesAPI() {
    app.get(LATEST_PARTITION_DATA_FILES_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFiles(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_DATA_FILE_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFile(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.validatedQueryParam(FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_ALL_DATA_FILES, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFiles(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFilesBeforeOrOn(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.validatedQueryParam(MAX_INSTANT_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFileOn(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.queryParam(INSTANT_PARAM), ctx.validatedQueryParam(FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_DATA_FILES, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getAllDataFiles(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFilesInRange(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              Arrays.asList(ctx.validatedQueryParam(INSTANTS_PARAM).getOrThrow().split(",")));
      writeValueAsString(ctx, dtos);
    }, true));
  }

  /**
   * Register File Slices API calls
   */
  private void registerFileSlicesAPI() {
    app.get(LATEST_PARTITION_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlices(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_SLICE_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlice(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.validatedQueryParam(FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_UNCOMPACTED_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestUnCompactedFileSlices(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getAllFileSlices(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_SLICES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSliceInRange(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              Arrays.asList(ctx.validatedQueryParam(INSTANTS_PARAM).getOrThrow().split(",")));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestMergedFileSlicesBeforeOrOn(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.validatedQueryParam(MAX_INSTANT_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_SLICES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlicesBeforeOrOn(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
              ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow(),
              ctx.validatedQueryParam(MAX_INSTANT_PARAM).getOrThrow(),
              Boolean.valueOf(ctx.validatedQueryParam(INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM).getOrThrow()));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(PENDING_COMPACTION_OPS, new ViewHandler(ctx -> {
      List<CompactionOpDTO> dtos = sliceHandler.getPendingCompactionOperations(
          ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_FILEGROUPS_FOR_PARTITION_URL, new ViewHandler(ctx -> {
      List<FileGroupDTO> dtos = sliceHandler.getAllFileGroups(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow(),
          ctx.validatedQueryParam(PARTITION_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.post(REFRESH_DATASET, new ViewHandler(ctx -> {
      boolean success = sliceHandler.refreshDataset(ctx.validatedQueryParam(BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, success);
    }, false));
  }

  private static boolean isRefreshCheckDisabledInQuery(Context ctxt) {
    return Boolean.valueOf(ctxt.queryParam(REFRESH_OFF));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class ViewHandler implements Handler {

    private final Handler handler;
    private final boolean performRefreshCheck;

    ViewHandler(Handler handler, boolean performRefreshCheck) {
      this.handler = handler;
      this.performRefreshCheck = performRefreshCheck;
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
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
          String errMsg = "Last known instant from client was "
              + context.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT_TS)
              + " but server has the following timeline "
              +  viewManager.getFileSystemView(context.queryParam(BASEPATH_PARAM))
              .getTimeline().getInstants().collect(Collectors.toList());
          Preconditions.checkArgument(!isLocalViewBehind(context), errMsg);
          long endFinalCheck = System.currentTimeMillis();
          finalCheckTimeTaken = endFinalCheck - beginFinalCheck;
        }
      } catch (RuntimeException re) {
        success = false;
        logger.error("Got runtime exception servicing request " + context.queryString(), re);
        throw re;
      } finally {
        long endTs = System.currentTimeMillis();
        long timeTakenMillis = endTs - beginTs;
        logger.info(String.format("TimeTakenMillis[Total=%d, Refresh=%d, handle=%d, Check=%d], "
                + "Success=%s, Query=%s, Host=%s, synced=%s", timeTakenMillis, refreshCheckTimeTaken, handleTimeTaken,
            finalCheckTimeTaken, success, context.queryString(), context.host(), synced));
      }
    }
  }
}
