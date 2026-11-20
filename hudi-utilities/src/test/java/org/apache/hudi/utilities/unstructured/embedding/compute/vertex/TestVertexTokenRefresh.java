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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Access-token refresh of the Vertex client: bearer tokens that expire while a client is in use. The Vertex stub
 * answers HTTP 401 to a token once it served its requests ({@link VertexStubServer#expireTokensAfter}); tokens come from a token
 * file an external refresher rewrites, or from a Google service-account credential whose token endpoint is
 * {@link GoogleTokenStubServer}. No live call.
 */
class TestVertexTokenRefresh {

  private static final String ALIAS = "mm";
  private static final int INPUTS = 7;
  /** The documented key and value, spelled out: they are the configuration contract. */
  private static final String SOURCE_SUFFIX = ".credential.source";
  private static final String GOOGLE = "google";

  @TempDir
  Path tempDir;
  private VertexStubServer stub;
  private GoogleTokenStubServer tokens;
  private Path tokenFile;
  private TypedProperties props;
  /** The client's clock: the wall clock unless a test moves it. */
  private final AtomicLong now = new AtomicLong(System.currentTimeMillis());
  /** Every wait the client asked for; each wait moves {@link #now} on. */
  private final List<Long> sleeps = Collections.synchronizedList(new ArrayList<>());
  private final RequestRateLimiter.Sleeper sleeper = millis -> {
    sleeps.add(millis);
    now.addAndGet(millis);
  };
  /** The environment the client reads. */
  private final Map<String, String> environment = new HashMap<>();

  @BeforeEach
  void setUp() throws IOException {
    stub = new VertexStubServer();
    tokens = new GoogleTokenStubServer();
    tokenFile = tempDir.resolve("token");
    write(tokenFile, "file-token-1\n");
    props = new TypedProperties();
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.endpoint());
  }

  @AfterEach
  void tearDown() {
    stub.close();
    tokens.close();
  }

  /** A token file rewritten by a refresher (a sidecar, a cron) is read again on a 401: every input embeds once. */
  @Test
  void tokenFileRewrittenByARefresherIsReadAgainOn401() throws Exception {
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
    AtomicInteger generation = new AtomicInteger(1);
    stub.expireTokensAfter(2, expired -> write(tokenFile, "file-token-" + generation.incrementAndGet() + "\n"));

    List<EmbeddingResult> results = client().embed(texts(INPUTS));

    assertEveryInputEmbeddedOnce(results);
    assertTrue(count401() >= 3, "tokens expired mid-call: " + statuses());
    assertEquals(4, sentTokens().size(), "four file generations sent: " + sentTokens());
  }

  /** A service-account credential refreshes from its token endpoint on a 401: every input embeds once. */
  @Test
  void serviceAccountCredentialRefreshesOn401() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    stub.expireTokensAfter(2, expired -> { });

    List<EmbeddingResult> results = client().embed(texts(INPUTS));

    assertEveryInputEmbeddedOnce(results);
    assertEquals(Collections.emptyList(), tokens.rejections(), "every signed grant passed the endpoint's checks");
    assertEquals(4, tokens.issued(), "the first token, then one refresh per expiry: " + tokens.grants());
    assertEquals(3, count401(), statuses().toString());
    tokens.grants().forEach(grant -> assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", grant));
    assertEquals(4, sentTokens().size(), sentTokens().toString());
  }

  /** A token close to its expiry is refreshed before the request: no 401 is ever answered. */
  @Test
  void serviceAccountCredentialRefreshesAheadOfExpiry() throws Exception {
    // inside the refresh margin (3 min 45 s): every token is due for refresh before its next use
    tokens.expiresIn(60);
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));

    List<EmbeddingResult> results = client().embed(texts(3));

    assertEveryInputEmbeddedOnce(results);
    assertEquals(0, count401(), statuses().toString());
    assertEquals(3, tokens.issued(), "one token per request: " + tokens.grants());
    assertEquals(3, sentTokens().size(), sentTokens().toString());
  }

  /** On the client's clock: a token is reused until its refresh margin, then replaced before any 401. */
  @Test
  void tokenIsReplacedWhenItsExpiryNears() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    VertexMultimodalEmbeddingClient client = client();

    assertEveryInputEmbeddedOnce(client.embed(texts(2)));
    assertEquals(1, tokens.issued());
    now.addAndGet(3600_000L - GoogleAccessTokens.REFRESH_MARGIN_MILLIS - 1_000L);
    client.embed(texts(1));
    assertEquals(1, tokens.issued(), "still outside the margin");
    now.addAndGet(2_000L);
    client.embed(texts(1));
    assertEquals(2, tokens.issued(), "inside the margin: replaced ahead of expiry");
    assertEquals(0, count401(), statuses().toString());
  }

  /** A token that lives longer than the client's work is fetched once and reused. */
  @Test
  void serviceAccountTokenIsReusedWhileFresh() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));

    assertEveryInputEmbeddedOnce(client().embed(texts(3)));
    assertEquals(1, tokens.issued(), tokens.grants().toString());
    assertEquals(1, sentTokens().size());
  }

  /** The token endpoint rejects the grant at a refresh mid-call: a configuration failure, the token never echoed. */
  @Test
  void rejectedRefreshIsAConfigFailure() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    stub.expireTokensAfter(2, expired -> tokens.enqueue(400,
        "{\"error\":\"invalid_grant\",\"error_description\":\"Invalid JWT Signature.\"}"));
    VertexMultimodalEmbeddingClient client = client();

    List<EmbeddingInput> inputs = texts(INPUTS);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));

    assertTrue(e.getMessage().contains("refresh"), e.getMessage());
    assertTrue(e.getMessage().contains("google"), e.getMessage());
    assertFalse(e.getMessage().contains(GoogleTokenStubServer.TOKEN_PREFIX), e.getMessage());
    assertEquals(2, okCount(), "the requests before the expiry were answered: " + statuses());
    assertEquals(3, client.lastCallStats().getRequests(), "two answered, then the expired one: " + client.lastCallStats());
  }

  /** The token endpoint rejects the first grant: a configuration failure before any Vertex request. */
  @Test
  void rejectedFirstGrantIsAConfigFailure() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    tokens.enqueue(401, "{\"error\":\"invalid_client\",\"error_description\":\"opaque-test-grant-value\"}");

    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));

    assertTrue(e.getMessage().contains("google file "), e.getMessage());
    assertFalse(e.getMessage().contains("opaque-test-grant-value"));
    assertEquals(0, stub.requests().size());
  }

  /** The token endpoint keeps failing with 5xx: the provider is unavailable (an outage), not the rows' fault. */
  @Test
  void unavailableTokenEndpointIsAnOutage() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    for (int i = 0; i < 10; i++) {
      tokens.enqueue(503, "{\"error\":\"backend_error\"}");
    }

    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingProviderUnavailableException e = assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(inputs));

    assertTrue(e.getMessage().contains("google file "), e.getMessage());
    assertTrue(e.getMessage().contains("HTTP 503"), e.getMessage());
    assertEquals(GoogleAccessTokens.MAX_ATTEMPTS, tokens.grants().size(), tokens.grants().toString());
    assertEquals(Arrays.asList(1_000L, 2_000L), sleeps, "backoff between attempts");
    assertEquals(0, stub.requests().size());
  }

  /** One failed attempt at the token endpoint is retried after a backoff: the call goes on. */
  @Test
  void transientTokenEndpointFailureIsRetried() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    tokens.enqueue(503, "{\"error\":\"backend_error\"}");

    assertEveryInputEmbeddedOnce(client().embed(texts(2)));
    assertEquals(Collections.singletonList(1_000L), sleeps);
    assertEquals(1, tokens.issued());
  }

  /**
   * No file and no GOOGLE_APPLICATION_CREDENTIALS: the metadata server (GKE workload identity), at the host GCE_METADATA_HOST
   * names, asked with {@code Metadata-Flavor: Google}; it is asked again on a 401.
   */
  @Test
  void metadataServerIsTheDefaultCredential() throws Exception {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", tokens.metadataHost());
    stub.expireTokensAfter(2, expired -> { });

    List<EmbeddingResult> results = client().embed(texts(5));

    assertEveryInputEmbeddedOnce(results);
    assertEquals(3, tokens.issued());
    tokens.grants().forEach(grant -> assertEquals("metadata", grant));
  }

  /** GOOGLE_APPLICATION_CREDENTIALS names the service-account key when no file is configured. */
  @Test
  void applicationCredentialsVariableNamesTheKey() throws Exception {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GOOGLE_APPLICATION_CREDENTIALS", tokens.writeServiceAccountKey(tempDir.resolve("adc.json")).toString());
    environment.put("GCE_METADATA_HOST", "127.0.0.1:9");

    assertEveryInputEmbeddedOnce(client().embed(texts(2)));
    assertEquals(Collections.singletonList("urn:ietf:params:oauth:grant-type:jwt-bearer"), tokens.grants());
    assertEquals(Collections.emptyList(), tokens.rejections(), "the signed grant passed the endpoint's checks");
  }

  /** A credential file of another type (e.g. workload identity federation) is refused at init, naming the type. */
  @Test
  void otherCredentialTypesAreRefused() {
    Path federation = tempDir.resolve("wif.json");
    write(federation, "{\"type\":\"external_account\",\"audience\":\"//iam.googleapis.com/x\"}");
    google(federation);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, this::client);
    assertTrue(e.getMessage().contains("external_account"), e.getMessage());
  }

  /**
   * The token source reads the configured environment variable, and again on a 401. A process's environment does not change
   * in production, so there a 401 on an env token stays a configuration failure; only a token file can be refreshed from outside.
   */
  @Test
  void tokenEnvironmentVariableIsReadAgainOn401() throws Exception {
    props.setProperty(key(VertexClientConfig.CREDENTIAL_ENV_SUFFIX), "VERTEX_TOKEN");
    environment.put("VERTEX_TOKEN", "env-token-1");
    AtomicInteger generation = new AtomicInteger(1);
    stub.expireTokensAfter(2, expired -> environment.put("VERTEX_TOKEN", "env-token-" + generation.incrementAndGet()));

    assertEveryInputEmbeddedOnce(client().embed(texts(5)));
    assertEquals(3, sentTokens().size(), sentTokens().toString());
  }

  /** A 401 for a token that was just refreshed is the credential's configuration (e.g. a wrong project): no loop. */
  @Test
  void http401AfterARefreshIsAConfigFailure() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    stub.enqueue(401, "error-401.json");
    stub.enqueue(401, "error-401.json");

    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));

    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
    assertEquals(2, tokens.issued(), "one refresh, no more: " + tokens.grants());
    assertEquals(2, stub.requests().size());
  }

  /** 403 is a permission the credential lacks, not an expiry: no refresh. */
  @Test
  void http403IsNotRefreshed() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    stub.enqueueStatus(403, "{\"error\":{\"code\":403,\"status\":\"PERMISSION_DENIED\"}}");

    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertEquals(1, tokens.issued());
    assertEquals(1, stub.requests().size());
  }

  @Test
  void malformedCredentialFileFailsInit() {
    google(tempDir.resolve("not-a-key.json"));
    write(tempDir.resolve("not-a-key.json"), "{\"type\":\"service_account\"}");
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, this::client);
    assertTrue(e.getMessage().contains("google file "), e.getMessage());
  }

  @Test
  void sourceIsValidatedOnTheDriver() {
    props.setProperty(key(SOURCE_SUFFIX), "vault");
    EmbeddingModelSpec spec = spec();
    IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientFactory.checkConfig(spec, props));
    assertTrue(unknown.getMessage().contains(key(SOURCE_SUFFIX)), unknown.getMessage());

    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    props.setProperty(key(VertexClientConfig.CREDENTIAL_ENV_SUFFIX), "SOME_TOKEN_ENV");
    IllegalArgumentException env = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientFactory.checkConfig(spec, props));
    assertTrue(env.getMessage().contains(key(VertexClientConfig.CREDENTIAL_ENV_SUFFIX)), env.getMessage());
  }

  /** A token endpoint that is not an http(s) URL is the configuration, refused at init, never a per-row failure. */
  @Test
  void invalidTokenEndpointIsRefusedAtInit() throws IOException {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", "host_with_underscore:8080");
    EmbeddingClientConfigException metadata = assertThrows(EmbeddingClientConfigException.class, this::client);
    assertTrue(metadata.getMessage().contains("GCE_METADATA_HOST"), metadata.getMessage());

    Path key = tokens.writeServiceAccountKey(tempDir.resolve("sa.json"));
    ObjectNode json = (ObjectNode) VertexStubServer.MAPPER.readTree(key.toFile());
    json.put("token_uri", "ftp://tokens.example/token");
    Files.write(key, VertexStubServer.MAPPER.writeValueAsBytes(json));
    google(key);
    EmbeddingClientConfigException tokenUri = assertThrows(EmbeddingClientConfigException.class, this::client);
    assertTrue(tokenUri.getMessage().contains("token_uri"), tokenUri.getMessage());
  }

  /** No key and no metadata server (a host off Google Cloud): no credential at all, the configuration, never an outage. */
  @Test
  void missingMetadataServerIsAConfigFailure() {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    // the default metadata host off Google Cloud, as the HTTP client reports it, without a DNS lookup
    GoogleAccessTokens google = new GoogleAccessTokens(VertexClientConfig.from(spec(), props), now::get, sleeper,
        environment::get, request -> {
          ConnectException unresolved = new ConnectException();
          unresolved.initCause(new UnresolvedAddressException());
          throw unresolved;
        });
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, google::current);
    assertTrue(e.getMessage().contains("no metadata server"), e.getMessage());
    assertTrue(e.getMessage().contains("metadata.google.internal"), e.getMessage());
    assertEquals(Collections.emptyList(), sleeps, "not retried as an outage");
  }

  /** A minted token that is not one printable token is never sent (the HTTP client would echo it) and never quoted. */
  @Test
  void nonPrintableMintedTokenIsAnOutageWithoutTheValue() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    tokens.enqueue(200, "{\"access_token\":\"ya29.leaked\\nvalue\",\"expires_in\":3600}");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingProviderUnavailableException e = assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(inputs));
    assertFalse(e.getMessage().contains("leaked"), e.getMessage());
    assertEquals(0, stub.requests().size());
  }

  /** The token is chosen after the rate-limit wait: a wait that runs into the refresh margin sends a new token. */
  @Test
  void tokenIsChosenAfterTheRateLimitWait() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    // 400 s of life, so 175 s before the 225 s margin; one request every 100 s
    tokens.expiresIn(400);
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "0.01");

    client().embed(texts(3));

    // t=0 s token 1; t=100 s token 1; t=200 s, inside the margin after the wait: token 2
    assertEquals(Arrays.asList("Bearer stub-access-token-1", "Bearer stub-access-token-1", "Bearer stub-access-token-2"),
        stub.requests().stream().map(r -> r.authorization).collect(Collectors.toList()));
    assertEquals(0, count401());
  }

  /** A rejected credential names the source actually used, e.g. the key GOOGLE_APPLICATION_CREDENTIALS names. */
  @Test
  void rejectionNamesTheCredentialActuallyUsed() throws Exception {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GOOGLE_APPLICATION_CREDENTIALS", tokens.writeServiceAccountKey(tempDir.resolve("adc.json")).toString());
    stub.enqueueStatus(403, "{\"error\":{\"code\":403,\"status\":\"PERMISSION_DENIED\"}}");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("adc.json (from GOOGLE_APPLICATION_CREDENTIALS)"), e.getMessage());
  }

  /**
   * Against a metadata server that caches its token until 5 minutes before expiry (as the real one does), the client asks
   * for a token only inside its refresh margin, and then gets a new one: the margin is inside the server's cache window.
   */
  @Test
  void metadataServerCachedTokenIsReplacedInsideTheMargin() throws Exception {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", tokens.metadataHost());
    tokens.cacheMetadataTokens(now::get);
    VertexMultimodalEmbeddingClient client = client();
    long start = now.get();

    client.embed(texts(2));
    // 350 s left: outside the client's margin, inside nothing: no request to the metadata server
    now.set(start + 3_250_000L);
    client.embed(texts(2));
    assertEquals(1, tokens.grants().size(), "no metadata request while the token is fresh: " + tokens.grants());
    // 224 s left: inside the client's margin and under the server's 300 s: a new token, asked for once
    now.set(start + 3_600_000L - GoogleAccessTokens.REFRESH_MARGIN_MILLIS + 1_000L);
    client.embed(texts(2));

    assertEquals(2, tokens.grants().size(), "one metadata request per token: " + tokens.grants());
    assertEquals(Arrays.asList("Bearer stub-access-token-1", "Bearer stub-access-token-1", "Bearer stub-access-token-1",
        "Bearer stub-access-token-1", "Bearer stub-access-token-2", "Bearer stub-access-token-2"),
        stub.requests().stream().map(r -> r.authorization).collect(Collectors.toList()));
  }

  /** A metadata server without a service account for the workload (404) is the configuration. */
  @Test
  void metadataServer404IsAConfigFailure() {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", tokens.metadataHost());
    tokens.enqueue(404, "service account not found");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertEquals(1, tokens.grants().size());
  }

  /** A private_key that is not a key never reaches a message. */
  @Test
  void unusablePrivateKeyIsNeverQuoted() throws IOException {
    Path key = tokens.writeServiceAccountKey(tempDir.resolve("sa.json"));
    ObjectNode json = (ObjectNode) VertexStubServer.MAPPER.readTree(key.toFile());
    json.put("private_key", "-----BEGIN PRIVATE KEY-----\nSECRETMATERIAL\n-----END PRIVATE KEY-----\n");
    Files.write(key, VertexStubServer.MAPPER.writeValueAsBytes(json));
    google(key);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, this::client);
    assertFalse(e.getMessage().contains("SECRETMATERIAL"), e.getMessage());
  }

  /** An interrupt while waiting to retry the token endpoint ends the call as an interrupt, never as an outage. */
  @Test
  void interruptDuringTokenBackoffPropagates() throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    tokens.enqueue(503, "{}");
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient(now::get, millis -> {
      throw new InterruptedException("test");
    }, environment::get);
    client.init(spec(), props);
    List<EmbeddingInput> inputs = texts(1);
    assertThrows(InterruptedException.class, () -> client.embed(inputs));
  }

  /**
   * Once the metadata server issued a token it exists: an unresolvable host at a refresh (a DNS blip) is retried and
   * then an outage, never "no credential".
   */
  @Test
  void unknownHostAfterATokenIsAnOutage() throws Exception {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", tokens.metadataHost());
    HttpClient http = HttpClient.newHttpClient();
    AtomicInteger sends = new AtomicInteger();
    GoogleAccessTokens google = new GoogleAccessTokens(VertexClientConfig.from(spec(), props), now::get, sleeper,
        environment::get, request -> {
          if (sends.incrementAndGet() == 1) {
            return http.send(request, HttpResponse.BodyHandlers.ofByteArray());
          }
          throw new UnknownHostException("metadata host");
        });

    assertEquals(GoogleTokenStubServer.TOKEN_PREFIX + "1", google.current());
    EmbeddingProviderUnavailableException e = assertThrows(EmbeddingProviderUnavailableException.class,
        () -> google.refresh(GoogleTokenStubServer.TOKEN_PREFIX + "1"));

    assertTrue(e.getMessage().contains("UnknownHostException"), e.getMessage());
    assertEquals(Arrays.asList(1_000L, 2_000L), sleeps, "retried as an outage");
  }

  /** A metadata server that refuses the connection (e.g. while it restarts) is an outage, retried, never configuration. */
  @Test
  void refusedMetadataConnectionIsAnOutage() throws Exception {
    int port;
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      port = socket.getLocalPort();
    }
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    environment.put("GCE_METADATA_HOST", "127.0.0.1:" + port);

    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = texts(1);
    EmbeddingProviderUnavailableException e = assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(inputs));

    assertTrue(e.getMessage().contains("ConnectException"), e.getMessage());
    assertEquals(Arrays.asList(1_000L, 2_000L), sleeps);
    assertEquals(0, stub.requests().size());
  }

  @Test
  void refreshResendRateWaitStopsAtCallDeadline() throws Exception {
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "0.1");
    props.setProperty(key(".request.deadline.ms"), "1000");
    props.setProperty(key(".retry.max.attempts"), "1");
    VertexMultimodalEmbeddingClient client = client();
    write(tokenFile, "file-token-2\n");
    stub.enqueue(401, "error-401.json");
    long start = now.get();
    EmbeddingResult result = client.embed(texts(1)).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().contains("deadline exceeded"), result.getError());
    assertEquals(1, stub.requests().size(), "the refreshed request is never sent after the deadline");
    assertEquals(start + 1000L, now.get());
    assertEquals(Collections.singletonList(1000L), sleeps);
    assertEquals(1, client.lastCallStats().getRequests());
    assertEquals(1000L, client.lastCallStats().getRateLimitWaitMillis());
  }

  @ParameterizedTest
  @CsvSource({"1000, false", "1500, false", "1000, true", "1500, true", "2000, false", "2000, true"})
  void bearerPreparationCannotSendAtOrAfterCallDeadline(long deadline, boolean resend) throws Exception {
    google(tokens.writeServiceAccountKey(tempDir.resolve("sa.json")));
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "2");
    props.setProperty(key(".request.deadline.ms"), String.valueOf(deadline));
    props.setProperty(key(".outage.consecutive.failures"), "1");
    tokens.enqueue(200, "{\"access_token\":\"initial-test-token\",\"expires_in\":" + (resend ? 3600 : 60) + "}");
    if (resend) {
      stub.enqueue(401, "error-401.json");
      tokens.enqueue(200, "{\"access_token\":\"refreshed-test-token\",\"expires_in\":60}");
    }
    tokens.enqueue(503, "{}");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingResult> results = client.embed(texts(resend ? 1 : 2));
    EmbeddingResult last = results.get(results.size() - 1);
    boolean expired = deadline <= 1500L;
    if (expired) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, last.getStatus());
      assertTrue(last.getError().contains("deadline exceeded"), last.getError());
      assertEquals(1, stub.requests().size());
      assertEquals(1, client.lastCallStats().getRequests());
    } else {
      assertEveryInputEmbeddedOnce(results);
      assertEquals(2, stub.requests().size());
      assertEquals(2, client.lastCallStats().getRequests());
    }
    assertEquals(0, client.lastCallStats().getRetries());
    assertEquals(500L, client.lastCallStats().getRateLimitWaitMillis());
    assertEquals(Arrays.asList(500L, 1000L), sleeps);
    if (!resend && expired) {
      assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
      assertEquals(EmbeddingResult.Status.OK, client.embed(texts(1)).get(0).getStatus(),
          "a never-sent request must not open the provider outage");
    }
  }

  // ---------------------------------------------------------------- helpers

  private void google(Path keyFile) {
    props.setProperty(key(SOURCE_SUFFIX), GOOGLE);
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), keyFile.toString());
  }

  private void assertEveryInputEmbeddedOnce(List<EmbeddingResult> results) {
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    }
    List<String> answered = stub.requests().stream().filter(r -> r.status == 200)
        .map(r -> r.body.path("instances").path(0).path("text").asText()).collect(Collectors.toList());
    assertEquals(results.size(), answered.size(), "one answered request per input: " + answered);
    assertEquals(results.size(), new HashSet<>(answered).size(), "no input answered twice: " + answered);
  }

  private long count401() {
    return stub.requests().stream().filter(r -> r.status == 401).count();
  }

  private long okCount() {
    return stub.requests().stream().filter(r -> r.status == 200).count();
  }

  private List<Integer> statuses() {
    return stub.requests().stream().map(r -> r.status).collect(Collectors.toList());
  }

  private Set<String> sentTokens() {
    return stub.requests().stream().map(r -> r.authorization).collect(Collectors.toSet());
  }

  private VertexMultimodalEmbeddingClient client() {
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient(now::get, sleeper, environment::get);
    client.init(spec(), props);
    return client;
  }

  private static List<EmbeddingInput> texts(int n) {
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      inputs.add(new EmbeddingInput("f" + i, 0, EmbeddingTableSchema.MODALITY_TEXT, "chunk " + i));
    }
    return inputs;
  }

  private static EmbeddingModelSpec spec() {
    return new EmbeddingModelSpec(ALIAS, "emb_mm", 128, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001");
  }

  private static void write(Path file, String content) {
    try {
      Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
