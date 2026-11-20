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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The HTTP transport provider stubs (Bedrock, Jina) and the OpenAI {@code StubEmbeddingServer} share: a local
 * server on {@code 127.0.0.1} and an ephemeral port that records every request as the stub's own record type and answers
 * it with what the stub's dispatcher returns (JSON content type). The stub keeps its endpoint rules and response bodies;
 * this class owns only the socket, the recording and the shutdown.
 *
 * @param <R> the stub's record of a request
 */
public final class StubHttpTransport<R> implements AutoCloseable {

  /** A request as received: method, raw and decoded path, headers by lower-case name (first value), UTF-8 body. */
  public static final class Exchange {
    public final String method;
    public final String rawPath;
    public final String path;
    public final Map<String, String> headers;
    public final String body;

    Exchange(String method, String rawPath, String path, Map<String, String> headers, String body) {
      this.method = method;
      this.rawPath = rawPath;
      this.path = path;
      this.headers = headers;
      this.body = body;
    }
  }

  /** The answer to a request: status, extra headers and body (an empty body is sent with no content). */
  public static final class Response {
    final int status;
    final Map<String, String> headers;
    final byte[] body;

    public Response(int status, Map<String, String> headers, byte[] body) {
      this.status = status;
      this.headers = headers;
      this.body = body;
    }
  }

  private final HttpServer server;
  private final Function<Exchange, R> recorder;
  private final Function<R, Response> dispatcher;
  private final List<R> requests = Collections.synchronizedList(new ArrayList<>());

  /**
   * Starts the server.
   *
   * @param recorder   builds the stub's record of a request; every request is recorded before it is answered
   * @param dispatcher the stub's answer to a recorded request
   */
  public StubHttpTransport(Function<Exchange, R> recorder, Function<R, Response> dispatcher) throws IOException {
    this.recorder = recorder;
    this.dispatcher = dispatcher;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  /** {@code http://127.0.0.1:<port>} */
  public String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /** The requests received so far, in arrival order (a copy). */
  public List<R> requests() {
    return new ArrayList<>(requests);
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (InputStream in = exchange.getRequestBody()) {
      Map<String, String> headers = new HashMap<>();
      exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), values.get(0)));
      R request = recorder.apply(new Exchange(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
          exchange.getRequestURI().getPath(), headers, new String(in.readAllBytes(), StandardCharsets.UTF_8)));
      requests.add(request);
      Response response = dispatcher.apply(request);
      response.headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(response.status, response.body.length == 0 ? -1 : response.body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response.body);
      }
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
