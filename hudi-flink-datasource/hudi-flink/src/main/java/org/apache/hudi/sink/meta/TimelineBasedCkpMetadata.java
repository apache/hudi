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

import org.apache.hudi.common.table.timeline.dto.CkpMetadataDTO;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.timeline.service.handlers.CkpMetadataHandler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
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

  private final HoodieWriteConfig writeConfig;
  private ObjectMapper mapper;

  public TimelineBasedCkpMetadata(FileSystem fs, String basePath, String uniqueId, HoodieWriteConfig writeConfig) {
    super(fs, basePath, uniqueId);
    this.writeConfig = writeConfig;
    mapper = new ObjectMapper();
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
  protected Stream<CkpMessage> readCkpMessages(Path ckpMetaPath) throws IOException {
    // Read ckp messages from timeline server
    Stream<CkpMessage> ckpMessageStream;
    try {
      List<CkpMetadataDTO> ckpMetadataDTOList = executeRequestToTimelineServerWithRetry(
          CkpMetadataHandler.ALL_CKP_METADATA_URL, getRequestParams(ckpMetaPath.toString()),
          new TypeReference<List<CkpMetadataDTO>>() {
          }, RequestMethod.GET);
      ckpMessageStream = ckpMetadataDTOList.stream().map(c -> new CkpMessage(c.getInstant(), c.getState()));
    } catch (HoodieException e) {
      LOG.error("Failed to execute scan ckp metadata, fall back to read from file system...", e);
      // If we failed to request timeline server, read ckp messages from file system directly.
      ckpMessageStream = super.readCkpMessages(ckpMetaPath);
    }
    return ckpMessageStream;
  }

  private Map<String, String> getRequestParams(String dirPath) {
    return Collections.singletonMap(CkpMetadataHandler.CKP_METADATA_DIR_PATH_PARAM, dirPath);
  }

  /**
   * Refresh the ckp messages that cached in timeline server.
   */
  private void sendRefreshRequest() {
    try {
      boolean success = executeRequestToTimelineServerWithRetry(
          CkpMetadataHandler.REFRESH_CKP_METADATA, getRequestParams(path.toString()),
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

  private <T> T executeRequestToTimelineServerWithRetry(String requestPath, Map<String, String> queryParameters,
                                                        TypeReference reference, RequestMethod method) {
    int retry = 5;
    while (--retry >= 0) {
      long start = System.currentTimeMillis();
      try {
        return executeRequestToTimelineServer(requestPath, queryParameters, reference, method);
      } catch (IOException e) {
        LOG.warn("Failed to execute ckp request (" + requestPath + ") to timeline server", e);
      } finally {
        LOG.info("Execute request : (" + requestPath + "), costs: " + (System.currentTimeMillis() - start) + " ms");
      }
    }
    throw new HoodieException("Failed to execute ckp request (" + requestPath + ")");
  }

  private <T> T executeRequestToTimelineServer(String requestPath, Map<String, String> queryParameters,
                                               TypeReference reference, RequestMethod method) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(this.writeConfig.getViewStorageConfig().getRemoteViewServerHost())
            .setPort(this.writeConfig.getViewStorageConfig().getRemoteViewServerPort())
            .setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    int timeout = this.writeConfig.getViewStorageConfig().getRemoteTimelineClientTimeoutSecs() * 1000;
    Response response;
    switch (method) {
      case GET:
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
      case POST:
      default:
        response = Request.Post(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
    }
    String content = response.returnContent().asString();
    return (T) mapper.readValue(content, reference);
  }

  enum RequestMethod {
    GET, POST
  }

}
