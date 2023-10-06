/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.table.timeline.dto.InstantStateDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * REST Handler servicing instant state requests.
 * <p>
 * The instant states are cached in timeline server and will be refreshed after the instant states in file system were changed.
 */
public class InstantStateHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(InstantStateHandler.class);

  /**
   * Base url for  instant state requests.
   */
  private static final String BASE_URL = "/v1/hoodie/instantstate";

  /**
   * Param for  instant state requests, which contains a uniqueId for different writers.
   */
  public static final String INSTANT_STATE_DIR_PATH_PARAM = "instantstatedirpath";

  /**
   * GET requests. Returns all the instant states under instant state path.
   */
  public static final String ALL_INSTANT_STATE_URL = String.format("%s/%s", BASE_URL, "all");

  /**
   * POST requests. Refresh the instant state data cached in memory.
   */
  public static final String REFRESH_INSTANT_STATE = String.format("%s/%s", BASE_URL, "refresh/");

  /**
   * Cached instant state data, instant state path -> list of instant states in fs.
   */
  private final ConcurrentHashMap<String, List<InstantStateDTO>> cachedInstantStates;

  /**
   * Number of requests after the last refresh.
   */
  private final AtomicLong requestCount;

  public InstantStateHandler(Configuration conf, TimelineService.Config timelineServiceConfig, FileSystem fileSystem,
                             FileSystemViewManager viewManager) throws IOException {
    super(conf, timelineServiceConfig, fileSystem, viewManager);
    this.cachedInstantStates = new ConcurrentHashMap<>();
    this.requestCount = new AtomicLong();
  }

  /**
   * Read instant states from cache of file system.
   *
   * @return Instant states under the input instant state path.
   */
  public List<InstantStateDTO> getAllInstantStates(String instantStatePath) {
    if (requestCount.incrementAndGet() >= timelineServiceConfig.instantStateForceRefreshRequestNumber) {
      // Do refresh for every N requests to ensure the writers won't be blocked forever
      refresh(instantStatePath);
    }
    return cachedInstantStates.computeIfAbsent(instantStatePath, k -> scanInstantState(new Path(k)));
  }

  /**
   * Refresh the checkpoint messages cached. Will be called when coordinator start/commit/abort instant.
   *
   * @return Whether refreshing is successful.
   */
  public boolean refresh(String instantStatePath) {
    try {
      cachedInstantStates.put(instantStatePath, scanInstantState(new Path(instantStatePath)));
      requestCount.set(0);
    } catch (Exception e) {
      LOG.error("Failed to load instant states, path: " + instantStatePath, e);
      return false;
    }
    return true;
  }

  /**
   * Scan the instant states from file system.
   */
  public List<InstantStateDTO> scanInstantState(Path instantStatePath) {
    try {
      // Check instantStatePath exists before list status, see HUDI-5915
      if (this.fileSystem.exists(instantStatePath)) {
        return Arrays.stream(this.fileSystem.listStatus(instantStatePath)).map(InstantStateDTO::fromFileStatus).collect(Collectors.toList());
      } else {
        return Collections.emptyList();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load instant states, path: " + instantStatePath, e);
    }
  }

}
