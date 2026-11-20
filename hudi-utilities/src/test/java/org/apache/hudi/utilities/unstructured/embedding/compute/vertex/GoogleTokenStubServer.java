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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.LongSupplier;

/**
 * A local OAuth 2.0 token endpoint for Google credentials: a service-account key file written by
 * {@link #writeServiceAccountKey} names it as its {@code token_uri}, so the client's service-account credential
 * ({@code GoogleAccessTokens}) posts its signed JWT grant here (https://developers.google.com/identity/protocols/oauth2/service-account#httprest). Each grant gets
 * a new access token {@code stub-access-token-<n>} with the configured {@code expires_in}, unless a queued reply answers it.
 * The JWT grant is checked as Google's endpoint would (RS256 signature by the written key, iss, aud, scope, a lifetime of at
 * most one hour); a grant that fails a check is answered 400 {@code invalid_grant}. The same server also answers as a metadata
 * server ({@link #metadataHost()}, GET {@code /computeMetadata/v1/instance/service-accounts/default/token} with
 * {@code Metadata-Flavor: Google}, else 403). Every grant type ({@code metadata} for the metadata server) is recorded.
 * With {@link #cacheMetadataTokens} the metadata server behaves as the real one: it hands out its cached token, with the seconds
 * it has left as {@code expires_in}, until fewer than {@value #METADATA_CACHE_SECONDS} s remain.
 */
public class GoogleTokenStubServer implements AutoCloseable {

  public static final String PATH = "/token";
  public static final String METADATA_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";
  public static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";
  /** The real metadata server caches a token until it has 5 minutes left. */
  public static final long METADATA_CACHE_SECONDS = 300;
  public static final String CLIENT_EMAIL = "embedding@stub-project.iam.gserviceaccount.com";
  public static final String TOKEN_PREFIX = "stub-access-token-";

  private final HttpServer server;
  private final List<String> grants = Collections.synchronizedList(new ArrayList<>());
  private final Queue<Object[]> replies = new LinkedList<>();
  private volatile long expiresInSeconds = 3600;
  private int issued;
  private volatile PublicKey publicKey;
  private LongSupplier metadataClock;
  private String cachedToken;
  private long cachedExpiresAtMillis;
  private final List<String> rejections = Collections.synchronizedList(new ArrayList<>());

  public GoogleTokenStubServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(PATH, this::handle);
    server.createContext(METADATA_PATH, this::handleMetadata);
    server.start();
  }

  public String tokenUri() {
    return "http://127.0.0.1:" + server.getAddress().getPort() + PATH;
  }

  /** {@code host:port} of the metadata server, the form of {@code GCE_METADATA_HOST}. */
  public String metadataHost() {
    return "127.0.0.1:" + server.getAddress().getPort();
  }

  /** The metadata server caches its token as the real one does, on {@code clockMillis} (the client's clock in a test). */
  public synchronized void cacheMetadataTokens(LongSupplier clockMillis) {
    this.metadataClock = clockMillis;
  }

  /** Why each refused JWT grant was refused, in order. */
  public List<String> rejections() {
    synchronized (rejections) {
      return new ArrayList<>(rejections);
    }
  }

  /** The {@code expires_in} of every token issued from now on. */
  public void expiresIn(long seconds) {
    this.expiresInSeconds = seconds;
  }

  /** The next grant is answered {@code status} with {@code body} instead of a token. */
  public synchronized void enqueue(int status, String body) {
    replies.add(new Object[] {status, body});
  }

  /** The grant type of every request received, in order. */
  public List<String> grants() {
    synchronized (grants) {
      return new ArrayList<>(grants);
    }
  }

  /** How many access tokens were issued. */
  public synchronized int issued() {
    return issued;
  }

  /**
   * Writes a service-account key file (a fresh RSA key, never a real account) whose {@code token_uri} is this endpoint.
   */
  public Path writeServiceAccountKey(Path file) throws IOException {
    KeyPairGenerator generator;
    try {
      generator = KeyPairGenerator.getInstance("RSA");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    generator.initialize(2048);
    KeyPair pair = generator.generateKeyPair();
    publicKey = pair.getPublic();
    String pem = "-----BEGIN PRIVATE KEY-----\n"
        + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(pair.getPrivate().getEncoded())
        + "\n-----END PRIVATE KEY-----\n";
    ObjectNode key = VertexStubServer.MAPPER.createObjectNode();
    key.put("type", "service_account");
    key.put("project_id", "stub-project");
    key.put("private_key_id", "stub-key-id");
    key.put("private_key", pem);
    key.put("client_email", CLIENT_EMAIL);
    key.put("client_id", "100000000000000000001");
    key.put("token_uri", tokenUri());
    Files.write(file, VertexStubServer.MAPPER.writeValueAsBytes(key));
    return file;
  }

  private void handle(HttpExchange exchange) throws IOException {
    String form = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
    String grant = "";
    String assertion = "";
    for (String pair : form.split("&")) {
      if (pair.startsWith("grant_type=")) {
        grant = URLDecoder.decode(pair.substring("grant_type=".length()), "UTF-8");
      } else if (pair.startsWith("assertion=")) {
        assertion = URLDecoder.decode(pair.substring("assertion=".length()), "UTF-8");
      }
    }
    grants.add(grant);
    String refused = refusal(assertion);
    if (refused != null) {
      rejections.add(refused);
      respond(exchange, 400, "{\"error\":\"invalid_grant\",\"error_description\":\"" + refused + "\"}");
      return;
    }
    answer(exchange);
  }

  private void handleMetadata(HttpExchange exchange) throws IOException {
    grants.add("metadata");
    if (!"GET".equals(exchange.getRequestMethod()) || !"Google".equals(exchange.getRequestHeaders().getFirst("Metadata-Flavor"))) {
      respond(exchange, 403, "Missing Metadata-Flavor:Google header.");
      return;
    }
    String cached = null;
    synchronized (this) {
      if (metadataClock != null && replies.isEmpty()) {
        long now = metadataClock.getAsLong();
        if (cachedToken == null || cachedExpiresAtMillis - now < METADATA_CACHE_SECONDS * 1000L) {
          issued++;
          cachedToken = TOKEN_PREFIX + issued;
          cachedExpiresAtMillis = now + expiresInSeconds * 1000L;
        }
        cached = "{\"access_token\":\"" + cachedToken + "\",\"expires_in\":" + (cachedExpiresAtMillis - now) / 1000L
            + ",\"token_type\":\"Bearer\"}";
      }
    }
    if (cached != null) {
      respond(exchange, 200, cached);
      return;
    }
    answer(exchange);
  }

  /** A queued reply, else a new token. */
  private void answer(HttpExchange exchange) throws IOException {
    int status;
    String body;
    synchronized (this) {
      Object[] reply = replies.poll();
      if (reply != null) {
        status = (Integer) reply[0];
        body = (String) reply[1];
      } else {
        issued++;
        status = 200;
        body = "{\"access_token\":\"" + TOKEN_PREFIX + issued + "\",\"expires_in\":" + expiresInSeconds
            + ",\"token_type\":\"Bearer\"}";
      }
    }
    respond(exchange, status, body);
  }

  /** Why the JWT grant is refused, or null when it is a valid one for the written key. */
  private String refusal(String assertion) {
    String[] parts = assertion.split("\\.");
    if (parts.length != 3 || publicKey == null) {
      return "Invalid JWT: not three parts or no key written";
    }
    try {
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initVerify(publicKey);
      signature.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
      if (!signature.verify(Base64.getUrlDecoder().decode(parts[2]))) {
        return "Invalid JWT Signature.";
      }
      JsonNode header = VertexStubServer.MAPPER.readTree(Base64.getUrlDecoder().decode(parts[0]));
      JsonNode claims = VertexStubServer.MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
      long lifetime = claims.path("exp").asLong() - claims.path("iat").asLong();
      if (!"RS256".equals(header.path("alg").asText()) || !"JWT".equals(header.path("typ").asText())
          || !CLIENT_EMAIL.equals(claims.path("iss").asText()) || !tokenUri().equals(claims.path("aud").asText())
          || !SCOPE.equals(claims.path("scope").asText()) || lifetime <= 0 || lifetime > 3600) {
        return "Invalid JWT: header or claims " + header + " " + claims;
      }
      return null;
    } catch (GeneralSecurityException | IllegalArgumentException | IOException e) {
      return "Invalid JWT: " + e.getClass().getSimpleName();
    }
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
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
