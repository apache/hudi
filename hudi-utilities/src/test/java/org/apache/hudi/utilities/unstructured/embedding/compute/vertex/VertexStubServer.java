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

package org.apache.hudi.utilities.unstructured.embedding.compute.vertex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A local HTTP endpoint that replays recorded Vertex {@code :predict} bodies (src/test/resources/unstructured/vertex, README
 * there names the source): a queue of one-shot replies, then by default the recorded image or text response matching the
 * request's instance. Every request is recorded (body, Authorization header and the status answered, never logged).
 * Optionally ({@link #expireTokensAfter}) each bearer token serves a fixed number of requests and is then answered HTTP 401
 * (the recorded UNAUTHENTICATED body), as Vertex answers an expired access token.
 */
public class VertexStubServer implements AutoCloseable {

  static final ObjectMapper MAPPER = new ObjectMapper();
  public static final String PATH = "/v1/projects/p/locations/l/publishers/google/models/multimodalembedding@001:predict";

  /** A request the stub received. */
  public static final class Recorded {
    public final String authorization;
    public final JsonNode body;
    public final int status;

    Recorded(String authorization, JsonNode body, int status) {
      this.authorization = authorization;
      this.body = body;
      this.status = status;
    }
  }

  private final HttpServer server;
  private final List<Recorded> requests = Collections.synchronizedList(new ArrayList<>());
  private final Queue<int[]> statusQueue = new LinkedList<>();
  private final Queue<String> bodyQueue = new LinkedList<>();
  /** The Retry-After header of each queued answer, null for none. */
  private final Queue<String> retryAfterQueue = new LinkedList<>();
  /** Arrival of each request on {@link #arrivalClock}, in order. */
  private final List<Long> arrivals = Collections.synchronizedList(new ArrayList<>());
  private volatile LongSupplier arrivalClock = () -> 0L;
  /** Requests each bearer token serves before it expires; 0 = tokens never expire. */
  private int tokenRequests;
  private Consumer<String> onExpiry = token -> { };
  private final Map<String, Integer> served = new HashMap<>();

  public VertexStubServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(PATH, this::handle);
    server.start();
  }

  public String endpoint() {
    return "http://127.0.0.1:" + server.getAddress().getPort() + PATH;
  }

  /** The next request gets {@code status} with the recorded body {@code resource} (a file name under unstructured/vertex). */
  public synchronized void enqueue(int status, String resource) {
    statusQueue.add(new int[] {status});
    bodyQueue.add(resource(resource));
    retryAfterQueue.add(null);
  }

  /** The next request gets HTTP 200 with {@code body}. */
  public synchronized void enqueueBody(String body) {
    statusQueue.add(new int[] {200});
    bodyQueue.add(body);
    retryAfterQueue.add(null);
  }

  /** The next request gets {@code status} with {@code body}. */
  public synchronized void enqueueStatus(int status, String body) {
    enqueueStatus(status, body, null);
  }

  /** The next request gets {@code status} with {@code body} and the {@code Retry-After} header {@code retryAfter} (null: none). */
  public synchronized void enqueueStatus(int status, String body, String retryAfter) {
    statusQueue.add(new int[] {status});
    bodyQueue.add(body);
    retryAfterQueue.add(retryAfter);
  }

  /** Records each request's arrival on {@code clock} (a test's fake clock), readable through {@link #arrivals()}. */
  public void recordArrivalsOn(LongSupplier clock) {
    this.arrivalClock = clock;
  }

  /**
   * From now on each Authorization value serves {@code requests} requests; any later request carrying it is answered HTTP 401
   * and serves nothing. {@code onExpiry} gets the expired token (the part after {@code Bearer }) right after its last served
   * request, before that request is answered, e.g. to write the next token where an external refresher would.
   */
  public synchronized void expireTokensAfter(int requests, Consumer<String> onExpiry) {
    this.tokenRequests = requests;
    this.onExpiry = onExpiry;
  }

  public List<Long> arrivals() {
    synchronized (arrivals) {
      return new ArrayList<>(arrivals);
    }
  }

  public List<Recorded> requests() {
    synchronized (requests) {
      return new ArrayList<>(requests);
    }
  }

  public static String resource(String name) {
    try (InputStream in = VertexStubServer.class.getResourceAsStream("/unstructured/vertex/" + name)) {
      if (in == null) {
        throw new IllegalArgumentException("no recorded body " + name);
      }
      return new String(readAll(in), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    byte[] requestBody = readAll(exchange.getRequestBody());
    JsonNode json = MAPPER.readTree(requestBody);
    arrivals.add(arrivalClock.getAsLong());
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    int status;
    String body;
    String retryAfter = null;
    synchronized (this) {
      if (tokenRequests > 0 && served.getOrDefault(authorization, 0) >= tokenRequests) {
        status = 401;
        body = resource("error-401.json");
      } else if (!statusQueue.isEmpty()) {
        status = statusQueue.poll()[0];
        body = bodyQueue.poll();
        retryAfter = retryAfterQueue.poll();
      } else {
        status = 200;
        body = resource(json.path("instances").path(0).has("image") ? "predict-image-response.json" : "predict-text-response.json");
      }
      if (tokenRequests > 0 && status != 401 && served.merge(authorization, 1, Integer::sum) == tokenRequests) {
        onExpiry.accept(authorization == null ? null : authorization.substring("Bearer ".length()));
      }
    }
    requests.add(new Recorded(authorization, json, status));
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    if (retryAfter != null) {
      exchange.getResponseHeaders().add("Retry-After", retryAfter);
    }
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int n;
    while ((n = in.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
