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

package org.apache.hudi.sink.meta;

import org.apache.hudi.common.table.timeline.dto.InstantStateDTO;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.timeline.service.handlers.InstantStateHandler;
import org.apache.hudi.util.HttpRequestClient;
import org.apache.hudi.util.HttpRequestClient.RequestMethod;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Timeline server based CkpMetadata, will read ckpMessages from timeline-server instead of from file system directly.
 */
public class TimelineBasedCkpMetadata extends CkpMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineBasedCkpMetadata.class);

  private final HttpRequestClient httpRequestClient;

  public TimelineBasedCkpMetadata(FileSystem fs, String basePath, String uniqueId, HoodieWriteConfig writeConfig) {
    super(fs, basePath, uniqueId);
    this.httpRequestClient = new HttpRequestClient(writeConfig);
    LOG.info("Timeline server based CkpMetadata enabled");
  }

  @Override
  public void startInstant(String instant) {
    super.startInstant(instant);
    sendRefreshRequest();
  }

  @Override
  public void commitInstant(String instant) {
    super.commitInstant(instant);
    sendRefreshRequest();
  }

  @Override
  public void abortInstant(String instant) {
    super.abortInstant(instant);
    sendRefreshRequest();
  }

  @Override
  protected Stream<CkpMessage> fetchCkpMessages(Path ckpMetaPath) throws IOException {
    // Read ckp messages from timeline server
    Stream<CkpMessage> ckpMessageStream;
    try {
      List<InstantStateDTO> instantStateDTOList = httpRequestClient.executeRequestWithRetry(
          InstantStateHandler.ALL_INSTANT_STATE_URL, getRequestParams(ckpMetaPath.toString()),
          new TypeReference<List<InstantStateDTO>>() {
          }, RequestMethod.GET);
      ckpMessageStream = instantStateDTOList.stream().map(c -> new CkpMessage(c.getInstant(), c.getState()));
    } catch (Exception e) {
      LOG.error("Failed to execute scan ckp metadata, fall back to read from file system...", e);
      // If we failed to request timeline server, read ckp messages from file system directly.
      ckpMessageStream = super.fetchCkpMessages(ckpMetaPath);
    }
    return ckpMessageStream;
  }

  private Map<String, String> getRequestParams(String dirPath) {
    return Collections.singletonMap(InstantStateHandler.INSTANT_STATE_DIR_PATH_PARAM, dirPath);
  }

  /**
   * Refresh the ckp messages that cached in timeline server.
   */
  private void sendRefreshRequest() {
    try {
      boolean success = httpRequestClient.executeRequestWithRetry(
          InstantStateHandler.REFRESH_INSTANT_STATE, getRequestParams(path.toString()),
          new TypeReference<Boolean>() {
          }, RequestMethod.POST);
      if (!success) {
        LOG.warn("Timeline server responses with failed refresh");
      }
    } catch (Exception e) {
      // Do not propagate the exception because the server will also do auto refresh
      LOG.error("Failed to execute refresh", e);
    }
  }

}
