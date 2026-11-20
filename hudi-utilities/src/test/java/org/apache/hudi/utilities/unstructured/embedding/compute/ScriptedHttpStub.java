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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A local HTTP endpoint for the Bedrock and Jina client tests, on the shared {@link StubHttpTransport}: a queue of one-shot
 * {@link Reply} builders, then a default one; every request is recorded (method, raw path, headers, body). A Bedrock error
 * is a status, an {@code x-amzn-ErrorType} header naming the error code and a {@code {"message"}} body, the rest-json shape
 * the AWS SDK reads its error code from.
 */
public class ScriptedHttpStub implements AutoCloseable {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  /** A request the stub received. */
  public static final class Recorded {
    public final String method;
    public final String rawPath;
    public final Map<String, String> headers;
    public final String body;

    Recorded(String method, String rawPath, Map<String, String> headers, String body) {
      this.method = method;
      this.rawPath = rawPath;
      this.headers = headers;
      this.body = body;
    }

    public JsonNode json() {
      try {
        return MAPPER.readTree(body);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /** Header value by name (any case), or null. */
    public String header(String name) {
      return headers.get(name.toLowerCase());
    }
  }

  /** A scripted reply. */
  public static final class Reply {
    final int status;
    final Map<String, String> headers;
    final byte[] body;

    public Reply(int status, Map<String, String> headers, String body) {
      this.status = status;
      this.headers = headers;
      this.body = body.getBytes(StandardCharsets.UTF_8);
    }

    public static Reply json(int status, String body) {
      return new Reply(status, Collections.emptyMap(), body);
    }

    /** A Bedrock (rest-json) error: the code in {@code x-amzn-ErrorType}. */
    public static Reply awsError(int status, String errorCode, String message) {
      Map<String, String> headers = new HashMap<>();
      headers.put("x-amzn-ErrorType", errorCode);
      return new Reply(status, headers, "{\"message\":\"" + message + "\"}");
    }

    public Reply withHeader(String name, String value) {
      Map<String, String> copy = new HashMap<>(headers);
      copy.put(name, value);
      return new Reply(status, copy, new String(body, StandardCharsets.UTF_8));
    }
  }

  private final StubHttpTransport<Recorded> transport;
  private final LinkedList<Function<Recorded, Reply>> queue = new LinkedList<>();
  private Function<Recorded, Reply> otherwise = request -> Reply.json(404, "{\"message\":\"no stub reply\"}");

  public ScriptedHttpStub() throws IOException {
    transport = new StubHttpTransport<>(
        exchange -> new Recorded(exchange.method, exchange.rawPath, exchange.headers, exchange.body), this::reply);
  }

  /** {@code http://127.0.0.1:<port>} */
  public String url() {
    return transport.url();
  }

  /** The next request, after those already queued, gets {@code reply}. */
  public synchronized ScriptedHttpStub then(Function<Recorded, Reply> reply) {
    queue.add(reply);
    return this;
  }

  public ScriptedHttpStub then(Reply reply) {
    return then(request -> reply);
  }

  /** Replies with {@code reply} once the queue is empty. */
  public synchronized ScriptedHttpStub otherwise(Function<Recorded, Reply> reply) {
    otherwise = reply;
    return this;
  }

  public List<Recorded> requests() {
    return transport.requests();
  }

  /** The next queued reply, else the default one. */
  private StubHttpTransport.Response reply(Recorded request) {
    Function<Recorded, Reply> responder;
    synchronized (this) {
      responder = queue.isEmpty() ? otherwise : queue.removeFirst();
    }
    Reply reply = responder.apply(request);
    return new StubHttpTransport.Response(reply.status, reply.headers, reply.body);
  }

  /** A deterministic vector for {@code seed}, so tests can recompute what the stub answered. */
  public static float[] vectorFor(String seed, int dimension) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = (float) (Math.floorMod(seed.hashCode() * 31L + i, 1000L)) / 1000f;
    }
    return vector;
  }

  /** A JSON array of {@link #vectorFor}. */
  public static String vectorJson(String seed, int dimension) {
    StringBuilder sb = new StringBuilder("[");
    float[] vector = vectorFor(seed, dimension);
    for (int i = 0; i < vector.length; i++) {
      sb.append(i == 0 ? "" : ",").append(vector[i]);
    }
    return sb.append(']').toString();
  }

  @Override
  public void close() {
    transport.close();
  }
}
