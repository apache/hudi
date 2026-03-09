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

package org.apache.hudi.sink.muttley;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class FlinkHudiMuttleyClient {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkHudiMuttleyClient.class);

  public static final String URL = "http://localhost:%d/%s";
  public static final int DEFAULT_PORT = 5436;
  public static final String RPC_CALLER = "Rpc-caller";
  public static final String RPC_SERVICE = "Rpc-service";
  public static final String RPC_ROUTING_ZONE = "Rpc-Routing-Zone";
  public static final String RPC_ROUTING_DELEGATE = "Rpc-Routing-Delegate";
  public static final String XDC_ROUTING_DELEGATE = "crosszone";
  public static final String RPC_ENCODING_KEY = "Rpc-Encoding";
  public static final String RPC_PROCEDURE_KEY = "Rpc-Procedure";
  public static final String RPC_ENCODING_DEFAULT = "json";
  private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
  private static final int MAX_RETRIES = 3; // max retries (additional calls not counting first call) if failing

  private final String callerService;
  private final int port;
  private final HttpClient client;
  private final Duration readWriteTimeout;
  private final Option<String> routingZone;
  private final Option<String> servicePostfix;

  private FlinkHudiMuttleyClient(final String callerService, final int port,
                                 final Duration connectionTimeout, final Duration readWriteTimeout,
                                 final Option<String> routingZone,
                                 final Option<String> servicePostfix) {
    ValidationUtils.checkArgument(callerService != null && !callerService.isEmpty());
    ValidationUtils.checkArgument(routingZone != null);
    ValidationUtils.checkArgument(servicePostfix != null);

    this.callerService = callerService;
    this.port = port;
    this.readWriteTimeout = readWriteTimeout;
    this.routingZone = routingZone;
    this.servicePostfix = servicePostfix;

    this.client = HttpClient.newBuilder()
        .connectTimeout(connectionTimeout)
        .build();
  }

  protected FlinkHudiMuttleyClient(final String callerService, final int port,
                                   final Duration connectionTimeout, final Duration readWriteTimeout) {
    this(callerService, port, connectionTimeout, readWriteTimeout, Option.empty(), Option.empty());
  }

  protected String getUrl(final String path) {
    return String.format(URL, this.port, path);
  }

  protected HttpResponse<String> get(final String path) throws IOException, FlinkHudiMuttleyException {
    return this.process("GET", path, null);
  }

  protected HttpResponse<String> post(final String path, final String jsonPayload,
                          final String rpcProcedure) throws IOException, FlinkHudiMuttleyException {
    return this.process("POST", path, jsonPayload, Option.of(rpcProcedure));
  }

  protected HttpResponse<String> put(final String path, final String jsonPayload)
      throws IOException, FlinkHudiMuttleyException {
    return this.process("PUT", path, jsonPayload);
  }

  protected HttpResponse<String> delete(final String path)
      throws IOException, FlinkHudiMuttleyException {
    return this.process("DELETE", path, null);
  }

  private HttpResponse<String> process(final String method,
                           final String path,
                           final String jsonPayload) throws IOException, FlinkHudiMuttleyException {
    return process(method, path, jsonPayload, Option.empty());
  }

  private HttpResponse<String> process(final String method,
                           final String path,
                           final String jsonPayload,
                           final Option<String> rpcProcedure)
      throws IOException, FlinkHudiMuttleyException {
    final String url = getUrl(path);
    LOG.info("MuttleyClient request - Method: {}, URL: {}, Path: {}, RPC Procedure: {}",
        method, url, path, rpcProcedure.orElse("none"));
    LOG.debug("Request payload: {}", jsonPayload);

    // Build request body
    HttpRequest.BodyPublisher bodyPublisher = jsonPayload == null
        ? HttpRequest.BodyPublishers.noBody()
        : HttpRequest.BodyPublishers.ofString(jsonPayload);

    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(readWriteTimeout)
        .method(method, bodyPublisher);

    if (jsonPayload != null) {
      requestBuilder.header("Content-Type", JSON_CONTENT_TYPE);
    }

    // Add muttley headers
    Map<String, String> muttleyHeaders = getMuttleyRequestHeaders(servicePostfix);
    muttleyHeaders.forEach(requestBuilder::header);

    // Add zone routing headers
    if (routingZone.isPresent()) {
      getMuttleyZoneRoutingHeaders(routingZone.get()).forEach(requestBuilder::header);
    }

    // Add RPC procedure headers
    if (rpcProcedure.isPresent()) {
      requestBuilder.header(RPC_PROCEDURE_KEY, rpcProcedure.get());
      requestBuilder.header(RPC_ENCODING_KEY, RPC_ENCODING_DEFAULT);
    }

    final HttpRequest request = requestBuilder.build();
    LOG.info("Executing HTTP request with headers: {}", request.headers());

    // Execute with retry logic
    HttpResponse<String> response = executeWithRetry(request);

    int statusCode = response.statusCode();
    if (statusCode >= 200 && statusCode < 300) {
      LOG.debug("Request successful, returning response");
      return response;
    }

    String error = response.body();
    final String message = String.format("Service %s responded %d with error %s",
        this.getService(), statusCode, error);

    if (statusCode >= 400 && statusCode < 500) {
      LOG.error("path : {}, rpcProcedure : {}, statusCode : {}",
          path, rpcProcedure.orElse("No RPC Procedure"), statusCode);
      throw new FlinkHudiMuttleyClientException(message, statusCode);
    }

    if (statusCode >= 500) {
      LOG.error("path : {}, rpcProcedure : {}, statusCode : {}",
          path, rpcProcedure.orElse("No RPC Procedure"), statusCode);
      throw new FlinkHudiMuttleyServerException(message, statusCode);
    }

    throw new FlinkHudiMuttleyException(statusCode);
  }

  private HttpResponse<String> executeWithRetry(HttpRequest request) throws IOException {
    int maxTries = MAX_RETRIES + 1;
    HttpResponse<String> response = null;
    int tryCount = 0;

    do {
      try {
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          LOG.info("HTTP response received - Status code: {}, Successful: true", response.statusCode());
          return response;
        }
        LOG.info("HTTP response received - Status code: {}, Successful: false", response.statusCode());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("HTTP request interrupted", e);
      } catch (IOException e) {
        LOG.error("IOException during HTTP request execution (attempt {}/{}): {}",
            tryCount + 1, maxTries, e.getMessage(), e);
        if (tryCount + 1 >= maxTries) {
          throw e;
        }
      }
      tryCount++;
    } while (tryCount < maxTries);

    return response;
  }

  private Map<String, String> getMuttleyZoneRoutingHeaders(final String zone) {
    Map<String, String> headers = new HashMap<>();
    headers.put(RPC_ROUTING_DELEGATE, XDC_ROUTING_DELEGATE);
    headers.put(RPC_ROUTING_ZONE, zone);
    return headers;
  }

  protected Map<String, String> getExtraHeaders() throws IOException {
    return Collections.emptyMap();
  }

  private Map<String, String> getMuttleyRequestHeaders(
      final Option<String> servicePostfix) throws IOException {
    final String serviceName =
        servicePostfix.isPresent()
            ? String.format("%s%s", getService(), servicePostfix.get())
            : getService();
    final Map<String, String> muttleyRequestHeaders = new HashMap<>();
    muttleyRequestHeaders.put(RPC_CALLER, getCallerService());
    muttleyRequestHeaders.put(RPC_SERVICE, serviceName);
    muttleyRequestHeaders.putAll(getExtraHeaders());
    return muttleyRequestHeaders;
  }

  public String getCallerService() {
    return callerService;
  }

  public HttpClient getClient() {
    return client;
  }

  public abstract String getService();
}
