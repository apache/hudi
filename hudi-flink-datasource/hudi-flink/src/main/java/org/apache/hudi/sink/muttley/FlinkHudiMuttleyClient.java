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

import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final int MAX_RETRIES = 3; // max retries (additional calls not counting first call) if failing

  private final String callerService;
  private final int port;
  private final OkHttpClient client;

  private FlinkHudiMuttleyClient(final String callerService, final int port,
                                 final Duration connectionTimeout, final Duration readWriteTimeout,
                                 final Option<String> routingZone, final Option<OkHttpClient> clientOption,
                                 final Option<String> servicePostfix) {
    ValidationUtils.checkArgument(callerService != null && !callerService.isEmpty());
    ValidationUtils.checkArgument(routingZone != null);
    ValidationUtils.checkArgument(clientOption != null);
    ValidationUtils.checkArgument(servicePostfix != null);

    this.callerService = callerService;
    this.port = port;

    // add an interceptor to add muttley headers to the calls
    final Interceptor headerInterceptor = chain -> {
      final Request.Builder requestBuilder = chain.request().newBuilder();
      getMuttleyRequestHeaders(servicePostfix).forEach(requestBuilder::header);
      if (routingZone.isPresent()) {
        getMuttleyZoneRoutingHeaders(routingZone.get()).forEach(requestBuilder::header);
      }
      return chain.proceed(requestBuilder.build());
    };

    OkHttpClient baseClient = clientOption.orElse(new OkHttpClient());
    this.client = baseClient.newBuilder()
        .retryOnConnectionFailure(true)
        .connectTimeout(connectionTimeout)
        .readTimeout(readWriteTimeout)
        .writeTimeout(readWriteTimeout)
        .addInterceptor(headerInterceptor)
        .addInterceptor(new RetryInterceptor(MAX_RETRIES))
        .build();
  }

  protected FlinkHudiMuttleyClient(final String callerService, final int port,
                                   final Duration connectionTimeout, final Duration readWriteTimeout) {
    this(callerService, port, connectionTimeout, readWriteTimeout, Option.empty(), Option.empty(), Option.empty());
  }

  protected String getUrl(final String path) {
    return String.format(URL, this.port, path);
  }

  protected Response get(final String path) throws IOException, FlinkHudiMuttleyException {
    return this.process("GET", path, null);
  }

  protected Response post(final String path, final String jsonPayload,
                          final String rpcProcedure) throws IOException, FlinkHudiMuttleyException {
    return this.process("POST", path, jsonPayload, Option.of(rpcProcedure));
  }

  protected Response put(final String path, final String jsonPayload)
      throws IOException, FlinkHudiMuttleyException {
    return this.process("PUT", path, jsonPayload);
  }

  protected Response delete(final String path)
      throws IOException, FlinkHudiMuttleyException {
    return this.process("DELETE", path, null);
  }

  private Response process(final String method,
                           final String path,
                           final String jsonPayload) throws IOException, FlinkHudiMuttleyException {
    return process(method, path, jsonPayload, Option.empty());
  }

  private Response process(final String method,
                           final String path,
                           final String jsonPayload,
                           final Option<String> rpcProcedure)
      throws IOException, FlinkHudiMuttleyException {
    return process(method, path, jsonPayload, rpcProcedure, Option.empty());
  }

  private Response process(final String method,
                           final String path,
                           final String jsonPayload,
                           final Option<String> rpcProcedure,
                           final Option<Map<String, String>> headersOptional) throws FlinkHudiMuttleyException, IOException {
    final String url = getUrl(path);
    LOG.info("MuttleyClient request - Method: {}, URL: {}, Path: {}, RPC Procedure: {}",
        method, url, path, rpcProcedure.orElse("none"));
    LOG.debug("Request payload: {}", jsonPayload);

    final RequestBody body = jsonPayload == null ? null : RequestBody.create(jsonPayload, JSON);
    final Request.Builder requestBuilder = new Request.Builder()
        .url(url)
        .method(method, body);
    if (rpcProcedure.isPresent()) {
      requestBuilder.addHeader(RPC_PROCEDURE_KEY, rpcProcedure.get())
          .addHeader(RPC_ENCODING_KEY, RPC_ENCODING_DEFAULT);
    }

    if (headersOptional.isPresent()) {
      Map<String, String> headers = headersOptional.get();
      for (String key : headers.keySet()) {
        requestBuilder.addHeader(key, headers.get(key));
      }
    }

    final Request request = requestBuilder.build();
    LOG.info("Executing HTTP request with headers: {}", request.headers());

    Response response;
    try {
      response = getClient().newCall(request).execute();
      LOG.info("HTTP response received - Status code: {}, Successful: {}",
          response.code(), response.isSuccessful());
    } catch (IOException e) {
      LOG.error("IOException during HTTP request execution: {}", e.getMessage(), e);
      throw e;
    }

    if (response.isSuccessful()) {
      LOG.debug("Request successful, returning response");
      return response;
    }

    final int statusCode = response.code();

    String error;
    try (ResponseBody responseBody = response.body()) {
      error = new String(responseBody.bytes());
    } catch (IOException ex) {
      error = "Unable to get response body";
    }
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

  public OkHttpClient getClient() {
    return client;
  }

  public abstract String getService();

  private static class RetryInterceptor implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(RetryInterceptor.class);

    private final int maxTries;

    public RetryInterceptor(int maxRetries) {
      // maxTries = first call + maxRetries
      this.maxTries = (maxRetries >= 0 ? maxRetries : 0) + 1;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
      Request request = chain.request();
      Response response = null;
      boolean responseOK = false;
      int tryCount = 0;

      do {
        try {
          response = chain.proceed(request);
          responseOK = response == null ? false : response.isSuccessful();

          // If the response is not successful, and we haven't exhausted all retries,
          // close the response before trying again
          if (!responseOK && tryCount < maxTries && response != null) {
            response.close();
            response = null;
          }
        } catch (Exception e) {
          LOG.error("RetryInterceptor", "Request is not successful - " + tryCount, e);

          // If it is Exception, we should throw it out to Flink runtime.
          if (tryCount == maxTries) {
            throw e;
          }
        } finally {
          tryCount++;
        }
      } while (!responseOK && tryCount <= maxTries);

      return response;
    }
  }
}
