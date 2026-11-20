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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link HttpTransport} over the JDK 11 {@link HttpClient}. One client per JVM and connect timeout is shared by every
 * task: an HttpClient cannot be closed before JDK 21, and sharing keeps one connection pool (and TLS session) per
 * executor instead of one per task. Proxies follow the JVM default {@link ProxySelector}
 * ({@code -Dhttps.proxyHost}/{@code -Dhttps.proxyPort}).
 */
final class JdkHttpTransport implements HttpTransport {

  /** Connect timeouts above this are capped: a connection that takes longer is better retried. */
  private static final int MAX_CONNECT_TIMEOUT_MS = 10_000;
  private static final Map<Integer, HttpClient> CLIENTS = new ConcurrentHashMap<>();

  @Override
  public HttpReply post(String url, Map<String, String> headers, byte[] body, int timeoutMs)
      throws IOException, InterruptedException {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
    headers.forEach(request::header);
    return send(request.build(), timeoutMs);
  }

  @Override
  public HttpReply get(String url, Map<String, String> headers, int timeoutMs) throws IOException, InterruptedException {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMillis(timeoutMs)).GET();
    headers.forEach(request::header);
    return send(request.build(), timeoutMs);
  }

  private static HttpReply send(HttpRequest request, int timeoutMs) throws IOException, InterruptedException {
    HttpResponse<byte[]> response = client(Math.min(timeoutMs, MAX_CONNECT_TIMEOUT_MS))
        .send(request, HttpResponse.BodyHandlers.ofByteArray());
    return new HttpReply(response.statusCode(), response.headers().map(), response.body());
  }

  private static HttpClient client(int connectTimeoutMs) {
    return CLIENTS.computeIfAbsent(connectTimeoutMs, timeout -> {
      ProxySelector proxy = ProxySelector.getDefault();
      return HttpClient.newBuilder()
          .connectTimeout(Duration.ofMillis(timeout))
          .proxy(proxy == null ? HttpClient.Builder.NO_PROXY : proxy)
          .build();
    });
  }
}
