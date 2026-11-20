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

import org.apache.hudi.utilities.unstructured.embedding.compute.StubHttpTransport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A local HTTP endpoint replaying scripted responses to the OpenAI-compatible client, on the shared {@link StubHttpTransport}:
 * per path, a queue of one-shot {@link Responder}s and then a default one. Every request is recorded. Bodies are the
 * documented response shapes under {@code src/test/resources/unstructured/openai/} (README there names the sources) or built
 * in the same shape.
 */
public class StubEmbeddingServer implements AutoCloseable {

  static final ObjectMapper MAPPER = new ObjectMapper();

  /** A request the stub received. */
  public static final class Recorded {
    final String method;
    final String path;
    final Map<String, String> headers;
    final String body;

    Recorded(String method, String path, Map<String, String> headers, String body) {
      this.method = method;
      this.path = path;
      this.headers = headers;
      this.body = body;
    }

    /** The {@code input} texts of an embeddings request. */
    List<String> inputs() {
      List<String> inputs = new ArrayList<>();
      json().get("input").forEach(node -> inputs.add(node.asText()));
      return inputs;
    }

    JsonNode json() {
      try {
        return MAPPER.readTree(body);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /** Header value by lower-case name, or null. */
    String header(String name) {
      return headers.get(name.toLowerCase());
    }
  }

  /** A scripted reply. */
  public static final class Reply {
    final int status;
    final Map<String, String> headers;
    final byte[] body;

    Reply(int status, Map<String, String> headers, byte[] body) {
      this.status = status;
      this.headers = headers;
      this.body = body;
    }

    Reply withHeader(String name, String value) {
      Map<String, String> copy = new HashMap<>(headers);
      copy.put(name, value);
      return new Reply(status, copy, body);
    }
  }

  /** Builds the reply to a request. */
  public interface Responder extends Function<Recorded, Reply> {
  }

  private final StubHttpTransport<Recorded> transport;
  private final Map<String, LinkedList<Responder>> scripted = new HashMap<>();
  private final Map<String, Responder> defaults = new HashMap<>();

  public StubEmbeddingServer() throws IOException {
    transport = new StubHttpTransport<>(
        exchange -> new Recorded(exchange.method, exchange.path, exchange.headers, exchange.body), this::reply);
  }

  /** {@code http://127.0.0.1:<port>} */
  String url() {
    return transport.url();
  }

  /** Replies to the next request on {@code path} with {@code responder}, after those already queued. */
  synchronized StubEmbeddingServer then(String path, Responder responder) {
    scripted.computeIfAbsent(path, p -> new LinkedList<>()).add(responder);
    return this;
  }

  /** Replies with {@code responder} once the queue of {@code path} is empty. */
  synchronized StubEmbeddingServer otherwise(String path, Responder responder) {
    defaults.put(path, responder);
    return this;
  }

  List<Recorded> requests() {
    return transport.requests();
  }

  /** The embeddings requests received (POSTs), in order. */
  List<Recorded> posts() {
    List<Recorded> posts = new ArrayList<>();
    for (Recorded request : requests()) {
      if ("POST".equals(request.method)) {
        posts.add(request);
      }
    }
    return posts;
  }

  /** The next queued responder of the request's path, else that path's default, else a 404. */
  private StubHttpTransport.Response reply(Recorded request) {
    Responder responder;
    synchronized (this) {
      LinkedList<Responder> queue = scripted.get(request.path);
      responder = queue != null && !queue.isEmpty() ? queue.removeFirst() : defaults.get(request.path);
    }
    Reply reply = responder == null ? json(404, "{\"error\":{\"message\":\"no stub for " + request.path + "\"}}")
        : responder.apply(request);
    return new StubHttpTransport.Response(reply.status, reply.headers, reply.body);
  }

  static Reply json(int status, String body) {
    return new Reply(status, Collections.emptyMap(), body.getBytes(StandardCharsets.UTF_8));
  }

  /** A documented response shape from {@code src/test/resources/unstructured/openai/<name>}. */
  static Reply recorded(int status, String name) {
    try (InputStream in = StubEmbeddingServer.class.getResourceAsStream("/unstructured/openai/" + name)) {
      return new Reply(status, Collections.emptyMap(), in.readAllBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The vector the stub returns for {@code text}: deterministic in the text, so tests can recompute it. */
  static float[] vectorFor(String text, int dimension) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = (float) ((text.hashCode() * 31L + i) % 1000) / 1000f;
    }
    return vector;
  }

  /** Answers an embeddings request in the documented success shape, entries in REVERSED index order. */
  static Responder embeddings(int dimension) {
    return request -> {
      List<String> inputs = request.inputs();
      ObjectNode root = MAPPER.createObjectNode().put("object", "list");
      ArrayNode data = root.putArray("data");
      for (int i = inputs.size() - 1; i >= 0; i--) {
        ObjectNode entry = data.addObject().put("object", "embedding").put("index", i);
        ArrayNode embedding = entry.putArray("embedding");
        for (float value : vectorFor(inputs.get(i), dimension)) {
          embedding.add(value);
        }
      }
      root.put("model", request.json().path("model").asText());
      root.putObject("usage").put("prompt_tokens", 3 * inputs.size()).put("total_tokens", 3 * inputs.size());
      return json(200, root.toString());
    };
  }

  @Override
  public void close() {
    transport.close();
  }
}
