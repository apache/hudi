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

package org.apache.hudi.timeline;

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.common.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for a client to trigger HTTP calls (GET or POST)
 * to the Timeline Server from the executors.
 */
public abstract class TimelineServiceClientBase implements Serializable {

  private RetryHelper<Response, IOException> retryHelper;

  protected TimelineServiceClientBase(FileSystemViewStorageConfig config) {
    if (config.getBooleanOrDefault(FileSystemViewStorageConfig.REMOTE_RETRY_ENABLE)) {
      retryHelper = new RetryHelper<>(
          config.getRemoteTimelineClientMaxRetryIntervalMs(),
          config.getRemoteTimelineClientMaxRetryNumbers(),
          config.getRemoteTimelineInitialRetryIntervalMs(),
          config.getRemoteTimelineClientRetryExceptions(),
          "Sending request to timeline server");
    }
  }

  protected abstract Response executeRequest(Request request) throws IOException;

  public Response makeRequest(Request request) throws IOException {
    return  (retryHelper != null) ? retryHelper.start(() -> executeRequest(request)) : executeRequest(request);
  }

  public static class Request {
    private final TimelineServiceClient.RequestMethod method;
    private final String path;
    private final String body;
    private final Option<Map<String, String>> queryParameters;

    private Request(TimelineServiceClient.RequestMethod method, String path, Option<Map<String, String>> queryParameters, String body) {
      this.method = method;
      this.path = path;
      this.queryParameters = queryParameters;
      this.body = body;
    }

    public RequestMethod getMethod() {
      return method;
    }

    public String getPath() {
      return path;
    }

    public Option<Map<String, String>> getQueryParameters() {
      return queryParameters;
    }

    public String getBody() {
      return body == null ? StringUtils.EMPTY_STRING : body;
    }

    public static TimelineServiceClient.Request.Builder newBuilder(TimelineServiceClient.RequestMethod method, String path) {
      return new TimelineServiceClient.Request.Builder(method, path);
    }

    public static class Builder {
      private final TimelineServiceClient.RequestMethod method;
      private final String path;
      private Option<Map<String, String>> queryParameters;
      private String body;

      public Builder(TimelineServiceClient.RequestMethod method, String path) {
        this.method = method;
        this.path = path;
        this.queryParameters = Option.empty();
      }

      public Request.Builder addQueryParam(String key, String value) {
        queryParameters = (queryParameters.isPresent()) ? queryParameters : Option.of(new HashMap<>());
        queryParameters.get().put(key, value);
        return this;
      }

      public Request.Builder addQueryParams(Map<String, String> parameters) {
        queryParameters = (queryParameters.isPresent()) ? queryParameters : Option.of(new HashMap<>());
        queryParameters.get().putAll(parameters);
        return this;
      }

      public Request.Builder setBody(String jsonString) {
        this.body = jsonString;
        return this;
      }

      public TimelineServiceClient.Request build() {
        return new TimelineServiceClient.Request(method, path, queryParameters, body);
      }
    }
  }

  public static class Response {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
    private final InputStream content;

    public Response(InputStream content) {
      this.content = content;
    }

    public <T> T getDecodedContent(TypeReference reference) throws IOException {
      try {
        return (T) OBJECT_MAPPER.readValue(content, reference);
      } finally {
        content.close();
      }
    }
  }

  public enum RequestMethod {
    GET, POST
  }
}
