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

import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.SingleTokenReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * {@value VertexClientConfig#CREDENTIAL_SOURCE_GOOGLE}: a Google credential that mints its own access tokens and mints a new
 * one before the current one expires, so a long compute task keeps working past the one-hour lifetime of a Google access token.
 * The credential, in the order of Google's Application Default Credentials
 * (https://cloud.google.com/docs/authentication/application-default-credentials):
 * <ul>
 *   <li>a service-account key file: {@value VertexClientConfig#CREDENTIAL_FILE_SUFFIX} when set, else the file the environment
 *       variable {@value #APPLICATION_CREDENTIALS_ENV} names. Each token is granted for a JWT signed with the key (RS256, scope
 *       {@value #SCOPE}, at most one hour) posted to the key's {@code token_uri}
 *       (https://developers.google.com/identity/protocols/oauth2/service-account);</li>
 *   <li>else the metadata server of the machine (GKE workload identity, a Compute Engine service account): GET
 *       {@value #METADATA_TOKEN_PATH} on {@value #DEFAULT_METADATA_HOST}, or the host:port in {@value #METADATA_HOST_ENV}
 *       (https://docs.cloud.google.com/compute/docs/access/authenticate-workloads).</li>
 * </ul>
 * A token is replaced once less than {@value #REFRESH_MARGIN_MILLIS} ms of its {@code expires_in} remain: under the five minutes
 * before expiry at which the metadata server stops handing out its cached token. Other credential files (workload identity
 * federation, user credentials) are refused at init.
 * Failures, never a row's: a key file that is missing, not a service-account key or not a usable RSA key, a token endpoint that
 * is not an http(s) URL, a metadata server whose host does not resolve before it issued any token (no credential at all, as
 * Application Default Credentials reports it), or a token endpoint answer 4xx other than 408/429 (e.g. {@code invalid_grant}), is the configuration; a token
 * endpoint that cannot be reached otherwise (e.g. connection refused while a metadata server restarts) or answers 408, 429 or
 * 5xx on each of {@value #MAX_ATTEMPTS} attempts is an outage. Messages name the source, never a token or
 * the key.
 */
final class GoogleAccessTokens extends VertexAccessTokens {

  static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";
  static final String APPLICATION_CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";
  static final String METADATA_HOST_ENV = "GCE_METADATA_HOST";
  static final String DEFAULT_METADATA_HOST = "metadata.google.internal";
  // by design: the GCE metadata server's documented token path; its host is configurable (GCE_METADATA_HOST)
  static final String METADATA_TOKEN_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";
  // by design: Google's documented default OAuth endpoint, used only when the service-account key has no token_uri
  static final String DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token";
  static final String JWT_BEARER_GRANT = "urn:ietf:params:oauth:grant-type:jwt-bearer";
  static final long REFRESH_MARGIN_MILLIS = 225_000L;
  static final int MAX_ATTEMPTS = 3;
  static final long FIRST_BACKOFF_MILLIS = 1_000L;
  private static final long JWT_LIFETIME_SECONDS = 3600L;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private static final Transport HTTP_TRANSPORT = request -> HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());

  /** Sends one token request: the HTTP client in production; a test seam. */
  interface Transport {
    HttpResponse<byte[]> send(HttpRequest request) throws IOException, InterruptedException;
  }

  private final String source;
  private final Duration timeout;
  private final LongSupplier clock;
  private final RequestRateLimiter.Sleeper sleeper;
  private final Transport transport;
  /** The service-account key, or null for the metadata server. */
  private final ServiceAccountKey key;
  private final String metadataUrl;
  /** Where tokens are requested: the key's token_uri or the metadata server, checked once at init. */
  private final URI tokenEndpoint;
  private String token;
  private long expiresAtMillis;

  /**
   * @param environment looks up an environment variable ({@code System::getenv} in production)
   * @throws EmbeddingClientConfigException for a key file that cannot be read or is not a usable service-account key
   */
  GoogleAccessTokens(VertexClientConfig config, LongSupplier clock, RequestRateLimiter.Sleeper sleeper,
                     UnaryOperator<String> environment) {
    this(config, clock, sleeper, environment, HTTP_TRANSPORT);
  }

  /** Test seam: also the transport of the token requests. */
  GoogleAccessTokens(VertexClientConfig config, LongSupplier clock, RequestRateLimiter.Sleeper sleeper,
                     UnaryOperator<String> environment, Transport transport) {
    this.timeout = Duration.ofMillis(config.requestTimeoutMs());
    this.clock = clock;
    this.sleeper = sleeper;
    this.transport = transport;
    String file = config.credentialFile();
    String fromEnv = environment.apply(APPLICATION_CREDENTIALS_ENV);
    if (file == null && fromEnv != null && !fromEnv.trim().isEmpty()) {
      file = fromEnv.trim();
      this.source = "google file " + file + " (from " + APPLICATION_CREDENTIALS_ENV + ")";
    } else {
      this.source = config.describeCredential();
    }
    if (file != null) {
      this.key = ServiceAccountKey.read(file, source);
      this.metadataUrl = null;
      this.tokenEndpoint = endpoint(key.tokenUri, "the token_uri of " + source);
    } else {
      String host = environment.apply(METADATA_HOST_ENV);
      this.key = null;
      this.metadataUrl = "http://" + (host == null || host.trim().isEmpty() ? DEFAULT_METADATA_HOST : host.trim())
          + METADATA_TOKEN_PATH;
      this.tokenEndpoint = endpoint(metadataUrl, "the metadata server URL (" + METADATA_HOST_ENV + ")");
    }
  }

  /**
   * An absolute http(s) URL the HTTP client accepts; checked at init so a bad one is the configuration, never a per-request
   * failure.
   */
  private static URI endpoint(String url, String what) {
    try {
      URI uri = new URI(url);
      if (("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) && uri.getHost() != null) {
        HttpRequest.newBuilder(uri);
        return uri;
      }
    } catch (URISyntaxException | IllegalArgumentException e) {
      // refused below
    }
    throw new EmbeddingClientConfigException(what + " is not an http(s) URL with a host: " + url);
  }

  @Override
  String describe() {
    return key != null ? source : source + " (metadata server " + metadataUrl + ")";
  }

  @Override
  String current() throws InterruptedException {
    if (token == null || clock.getAsLong() >= expiresAtMillis - REFRESH_MARGIN_MILLIS) {
      fetch("obtain");
    }
    return token;
  }

  @Override
  boolean refresh(String rejected) throws InterruptedException {
    fetch("refresh");
    return !token.equals(rejected);
  }

  /** One token from the token endpoint, retrying what an outage explains; sets {@link #token} and its expiry. */
  private void fetch(String action) throws InterruptedException {
    String last = null;
    long backoff = FIRST_BACKOFF_MILLIS;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      if (attempt > 1) {
        sleeper.sleep(backoff);
        backoff *= 2;
      }
      HttpResponse<byte[]> response;
      try {
        response = transport.send(request());
      } catch (IOException e) {
        if (key == null && token == null && isUnknownHost(e)) {
          // as Application Default Credentials: no key file and no metadata server means no credential at all. Once a token came
          // from the metadata server it exists: an unresolvable host at a refresh is a DNS blip, retried, then an outage
          throw new EmbeddingClientConfigException("Cannot " + action + " the Vertex access token from " + describe()
              + ": no service-account key is configured (" + VertexClientConfig.CREDENTIAL_FILE_SUFFIX + ", "
              + APPLICATION_CREDENTIALS_ENV + ") and there is no metadata server: its host does not resolve");
        }
        last = e.getClass().getSimpleName() + ": " + e.getMessage();
        continue;
      }
      int status = response.statusCode();
      if (status == 200) {
        accept(response.body(), action);
        return;
      }
      // Token endpoint bodies can echo a signed grant or another credential.
      last = "HTTP " + status;
      if (status >= 400 && status < 500 && status != 408 && status != 429) {
        throw new EmbeddingClientConfigException("Cannot " + action + " the Vertex access token from " + describe()
            + ": the token endpoint answered " + last);
      }
    }
    throw new EmbeddingProviderUnavailableException("Cannot " + action + " the Vertex access token from " + describe()
        + " after " + MAX_ATTEMPTS + " attempts: " + last);
  }

  /** The host does not resolve: the HTTP client reports it as an UnknownHostException or an UnresolvedAddressException cause. */
  private static boolean isUnknownHost(Throwable failure) {
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (cause instanceof UnknownHostException || cause instanceof UnresolvedAddressException) {
        return true;
      }
    }
    return false;
  }

  private HttpRequest request() {
    if (key == null) {
      return HttpRequest.newBuilder(tokenEndpoint).timeout(timeout).header("Metadata-Flavor", "Google").GET().build();
    }
    String form = "grant_type=" + URLEncoder.encode(JWT_BEARER_GRANT, StandardCharsets.UTF_8)
        + "&assertion=" + URLEncoder.encode(key.assertion(clock.getAsLong() / 1000L), StandardCharsets.UTF_8);
    return HttpRequest.newBuilder(tokenEndpoint).timeout(timeout)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8)).build();
  }

  /** Reads {@code access_token} and {@code expires_in} of a 200 answer; never echoes the body, which holds the token. */
  private void accept(byte[] body, String action) {
    JsonNode answer;
    try {
      answer = MAPPER.readTree(body);
    } catch (IOException e) {
      throw new EmbeddingProviderUnavailableException("Cannot " + action + " the Vertex access token from " + describe()
          + ": the token endpoint answered a body that is not JSON");
    }
    String value = answer.path("access_token").asText("");
    long expiresIn = answer.path("expires_in").asLong(0L);
    if (value.isEmpty() || expiresIn <= 0) {
      throw new EmbeddingProviderUnavailableException("Cannot " + action + " the Vertex access token from " + describe()
          + ": the token endpoint answered without access_token or a positive expires_in");
    }
    if (!SingleTokenReader.isSinglePrintableToken(value)) {
      throw new EmbeddingProviderUnavailableException("Cannot " + action + " the Vertex access token from " + describe()
          + ": the token endpoint answered an access_token with whitespace, control or non-ASCII characters");
    }
    token = value;
    expiresAtMillis = clock.getAsLong() + expiresIn * 1000L;
  }

  /** The fields of a service-account key file this flow uses. */
  private static final class ServiceAccountKey {

    private final String clientEmail;
    private final String keyId;
    private final String tokenUri;
    private final PrivateKey privateKey;

    private ServiceAccountKey(String clientEmail, String keyId, String tokenUri, PrivateKey privateKey) {
      this.clientEmail = clientEmail;
      this.keyId = keyId;
      this.tokenUri = tokenUri;
      this.privateKey = privateKey;
    }

    /** Never quotes the file: it holds the private key. */
    static ServiceAccountKey read(String file, String source) {
      JsonNode json;
      try {
        json = MAPPER.readTree(Files.readAllBytes(Paths.get(file)));
      } catch (IOException e) {
        throw new EmbeddingClientConfigException("Cannot read the Vertex credential from " + source + ": "
            + e.getClass().getSimpleName());
      }
      String type = json.path("type").asText("");
      if (!"service_account".equals(type)) {
        throw new EmbeddingClientConfigException("The Vertex credential in " + source + " has type '" + type
            + "'; only a service-account key (type service_account) is supported, or no file for the metadata server");
      }
      String email = json.path("client_email").asText("");
      String pem = json.path("private_key").asText("");
      if (email.isEmpty() || pem.isEmpty()) {
        throw new EmbeddingClientConfigException("The service-account key in " + source
            + " lacks client_email or private_key");
      }
      PrivateKey privateKey;
      try {
        String base64 = pem.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "")
            .replaceAll("\\s", "");
        privateKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
      } catch (GeneralSecurityException | IllegalArgumentException e) {
        throw new EmbeddingClientConfigException("The private_key in " + source + " is not a PKCS#8 RSA key: "
            + e.getClass().getSimpleName());
      }
      String tokenUri = json.path("token_uri").asText("");
      return new ServiceAccountKey(email, json.path("private_key_id").asText(""),
          tokenUri.isEmpty() ? DEFAULT_TOKEN_URI : tokenUri, privateKey);
    }

    /** The signed JWT grant: RS256 over header.claims, base64url without padding. */
    String assertion(long nowSeconds) {
      ObjectNode header = MAPPER.createObjectNode().put("alg", "RS256").put("typ", "JWT");
      if (!keyId.isEmpty()) {
        header.put("kid", keyId);
      }
      ObjectNode claims = MAPPER.createObjectNode().put("iss", clientEmail).put("scope", SCOPE).put("aud", tokenUri)
          .put("iat", nowSeconds).put("exp", nowSeconds + JWT_LIFETIME_SECONDS);
      Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
      try {
        String signingInput = encoder.encodeToString(MAPPER.writeValueAsBytes(header)) + "."
            + encoder.encodeToString(MAPPER.writeValueAsBytes(claims));
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
        return signingInput + "." + encoder.encodeToString(signature.sign());
      } catch (IOException | GeneralSecurityException e) {
        throw new EmbeddingClientConfigException("Cannot sign the service-account grant with the key in " + clientEmail
            + "'s key file: " + e.getClass().getSimpleName());
      }
    }
  }
}
