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

import org.apache.http.client.utils.URIBuilder;

import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of an HTTP network client to trigger HTTP calls (GET or POST)
 * to the Timeline Server from the executors.
 * This class uses the Fluent HTTP client part of the HTTPComponents.
 */
public class TimelineServiceClient extends TimelineServiceClientBase {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineServiceClient.class);
  private static final String DEFAULT_SCHEME = "http";

  protected final String timelineServerHost;
  protected final int timelineServerPort;
  protected final int timeoutMs;

  public TimelineServiceClient(FileSystemViewStorageConfig config) {
    super(config);
    this.timelineServerHost = config.getRemoteViewServerHost();
    this.timelineServerPort = config.getRemoteViewServerPort();
    this.timeoutMs = (int) TimeUnit.SECONDS.toMillis(config.getRemoteTimelineClientTimeoutSecs());
  }

  @Override
  protected Response executeRequest(Request request) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(timelineServerHost).setPort(timelineServerPort).setPath(request.getPath()).setScheme(DEFAULT_SCHEME);

    if (request.getQueryParameters().isPresent()) {
      request.getQueryParameters().get().forEach(builder::addParameter);
    }

    String url = builder.toString();
    LOG.debug("Sending request : ({})", url);
    org.apache.http.client.fluent.Response response = get(request.getMethod(), url, timeoutMs, request.getBody());
    return new Response(response.returnContent().asStream());
  }

  private org.apache.http.client.fluent.Response get(RequestMethod method, String url, int timeoutMs, String body) throws IOException {
    switch (method) {
      case GET:
        return org.apache.http.client.fluent.Request.Get(url).connectTimeout(timeoutMs).socketTimeout(timeoutMs).execute();
      case POST:
      default:
        return org.apache.http.client.fluent.Request.Post(url).connectTimeout(timeoutMs).socketTimeout(timeoutMs)
            .bodyString(body, ContentType.APPLICATION_JSON)
            .execute();
    }
  }
}
