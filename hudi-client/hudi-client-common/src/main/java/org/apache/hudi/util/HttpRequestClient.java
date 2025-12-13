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
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Helper class for executing timeline server requests.
 */
@Slf4j
public class HttpRequestClient {
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
  private final String serverHost;
  private final int serverPort;
  private final int timeoutSecs;
  private final int maxRetry;

  public HttpRequestClient(HoodieWriteConfig writeConfig) {
    this(writeConfig.getViewStorageConfig().getRemoteViewServerHost(),
        writeConfig.getViewStorageConfig().getRemoteViewServerPort(),
        writeConfig.getViewStorageConfig().getRemoteTimelineClientTimeoutSecs(),
        writeConfig.getViewStorageConfig().getRemoteTimelineClientMaxRetryNumbers());
  }

  public HttpRequestClient(String serverHost, int serverPort, int timeoutSecs, int maxRetry) {
    this.serverHost = serverHost;
    this.serverPort = serverPort;
    this.timeoutSecs = timeoutSecs;
    this.maxRetry = maxRetry;
  }

  public <T> T executeRequestWithRetry(String requestPath, Map<String, String> queryParameters,
                                       TypeReference<T> reference, RequestMethod method) {
    int retry = maxRetry;
    while (--retry >= 0) {
      try {
        return executeRequest(requestPath, queryParameters, reference, method);
      } catch (IOException e) {
        log.warn("Failed to execute request ({}) to timeline server", requestPath, e);
      }
    }
    throw new HoodieException("Failed to execute timeline server request (" + requestPath + ")");
  }

  public <T> T executeRequest(String requestPath, Map<String, String> queryParameters,
                              TypeReference<T> reference, RequestMethod method) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(serverHost).setPort(serverPort).setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    log.debug("Sending request : ( {} )", url);
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
    return MAPPER.readValue(content, reference);
  }

  public enum RequestMethod {
    GET, POST
  }

}
