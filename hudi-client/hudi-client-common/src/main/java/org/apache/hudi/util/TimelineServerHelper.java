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

package org.apache.hudi.util;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Helper class for executing timeline server requests.
 */
public class TimelineServerHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TimelineServerHelper.class);
  private final ObjectMapper mapper;
  private final String timelineServerHost;
  private final int timelineServerPort;
  private final int timeoutSecs;
  private final int maxRetry;

  public TimelineServerHelper(HoodieWriteConfig writeConfig) {
    this(writeConfig.getViewStorageConfig().getRemoteViewServerHost(),
        writeConfig.getViewStorageConfig().getRemoteViewServerPort(),
        writeConfig.getViewStorageConfig().getRemoteTimelineClientTimeoutSecs(),
        writeConfig.getViewStorageConfig().getRemoteTimelineClientMaxRetryNumbers());
  }

  public TimelineServerHelper(String timelineServerHost, int timelineServerPort, int timeoutSecs, int maxRetry) {
    this.mapper = new ObjectMapper();
    this.timelineServerHost = timelineServerHost;
    this.timelineServerPort = timelineServerPort;
    this.timeoutSecs = timeoutSecs;
    this.maxRetry = maxRetry;
  }

  public <T> T executeRequestToTimelineServerWithRetry(String requestPath, Map<String, String> queryParameters,
                                                       TypeReference reference, RequestMethod method) {
    int retry = maxRetry;
    while (--retry >= 0) {
      long start = System.currentTimeMillis();
      try {
        return executeRequestToTimelineServer(requestPath, queryParameters, reference, method);
      } catch (IOException e) {
        LOG.warn("Failed to execute request (" + requestPath + ") to timeline server", e);
      } finally {
        LOG.info("Execute request : (" + requestPath + "), costs: " + (System.currentTimeMillis() - start) + " ms");
      }
    }
    throw new HoodieException("Failed to execute timeline server request (" + requestPath + ")");
  }

  public <T> T executeRequestToTimelineServer(String requestPath, Map<String, String> queryParameters,
                                              TypeReference reference, RequestMethod method) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(timelineServerHost).setPort(timelineServerPort).setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    LOG.debug("Sending request : (" + url + ")");
    Response response;
    int timeout = this.timeoutSecs * 1000; // msec
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

  public enum RequestMethod {
    GET, POST
  }

}
