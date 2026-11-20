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

package org.apache.hudi.utilities.unstructured.embedding.compute.bedrock;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Recorded;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Reply;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiClientConfig;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Bedrock clients against a stubbed Bedrock runtime endpoint ({@code .endpoint} → {@link ScriptedHttpStub}) through the
 * real AWS SDK client: request shapes per family, the SigV4 signature from the SDK default credential chain (no provider
 * configured), and the outcome classification: throttling retried, a rejected input isolated and quarantined, a rejected
 * configuration failing the call, the outage short-circuit, and one rate-limit token per attempt.
 */
public class TestBedrockEmbeddingClient {

  private static final String ALIAS = "br";
  private static final String KEY_ID = "AKIDSTUBTEST0001";
  private static final String SECRET = "stub-secret-key-not-real-0001";
  private static final String REGION = "us-west-2";
  private static final String TITAN_TEXT = "amazon.titan-embed-text-v2:0";
  private static final String COHERE = "cohere.embed-v4:0";
  private static String savedKeyId;
  private static String savedSecret;

  private ScriptedHttpStub stub;
  private TypedProperties props;

  @BeforeAll
  public static void credentialsOnTheDefaultChain() {
    // the first link of the SDK default chain: system properties; nothing is configured on the client
    savedKeyId = System.getProperty("aws.accessKeyId");
    savedSecret = System.getProperty("aws.secretAccessKey");
    System.setProperty("aws.accessKeyId", KEY_ID);
    System.setProperty("aws.secretAccessKey", SECRET);
  }

  @AfterAll
  public static void restoreCredentials() {
    restore("aws.accessKeyId", savedKeyId);
    restore("aws.secretAccessKey", savedSecret);
  }

  @BeforeEach
  public void setUp() throws IOException {
    stub = new ScriptedHttpStub();
    props = new TypedProperties();
    props.setProperty(key(BedrockClientConfig.ENDPOINT_SUFFIX), stub.url());
    props.setProperty(key(BedrockClientConfig.REGION_SUFFIX), REGION);
    props.setProperty(key(OpenAiClientConfig.RETRY_INITIAL_BACKOFF_MS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_BACKOFF_MS_SUFFIX), "5");
  }

  @AfterEach
  public void tearDown() {
    stub.close();
  }

  @Test
  public void titanTextRequestIsSignedByTheDefaultChainAndReturnsTheVector() throws Exception {
    stub.otherwise(BedrockStubReplies.embeddings(256));
    BedrockEmbeddingClient client = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256);
    List<EmbeddingResult> results = client.embed(Collections.singletonList(text("a", "a cat on a mat")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus(), results.get(0).getError());
    assertArrayEquals(ScriptedHttpStub.vectorFor("a cat on a mat", 256), results.get(0).getVector());
    assertEquals(3, client.lastCallStats().getProviderPromptTokens(), "Titan's inputTextTokenCount");
    Recorded request = stub.requests().get(0);
    assertEquals("POST", request.method);
    assertEquals("/model/amazon.titan-embed-text-v2%3A0/invoke", request.rawPath);
    assertEquals("a cat on a mat", request.json().get("inputText").asText());
    assertEquals(256, request.json().get("dimensions").asInt());
    String auth = request.header("Authorization");
    assertTrue(auth.startsWith("AWS4-HMAC-SHA256 Credential=" + KEY_ID + "/"), auth);
    assertTrue(auth.contains("/" + REGION + "/bedrock/aws4_request"), auth);
    assertFalse(auth.contains(SECRET));
  }

  /** The SDK client is built with no credentials provider, so it uses the SDK default chain. */
  @Test
  public void sdkClientHasNoCredentialsProviderOverride() {
    BedrockClientConfig config = BedrockClientConfig.from(spec(TITAN_TEXT, 256), props, BedrockModelFamily.TITAN_EMBED_TEXT_V2);
    try (BedrockRuntimeClient client = SdkBedrockInvoker.buildClient(config)) {
      IdentityProvider<? extends AwsCredentialsIdentity> provider = client.serviceClientConfiguration().credentialsProvider();
      assertNotNull(provider);
      assertInstanceOf(DefaultCredentialsProvider.class, provider, "the default chain, not an override: " + provider);
    }
    assertFalse(config.toString().contains(KEY_ID));
  }

  @Test
  public void regionFallsBackToTheDefaultRegionChain() throws Exception {
    props.remove(key(BedrockClientConfig.REGION_SUFFIX));
    // a timeout no other test uses: its own SDK client, built with no region
    props.setProperty(key(OpenAiClientConfig.REQUEST_TIMEOUT_MS_SUFFIX), "29001");
    String saved = System.getProperty("aws.region");
    System.setProperty("aws.region", "eu-central-1");
    try {
      stub.otherwise(BedrockStubReplies.embeddings(256));
      EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
          .embed(Collections.singletonList(text("a", "hello"))).get(0);
      assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
      assertTrue(stub.requests().get(0).header("Authorization").contains("/eu-central-1/bedrock/aws4_request"));
    } finally {
      restore("aws.region", saved);
    }
  }

  @Test
  public void cohereTextsArePackedIntoOneRequestInOrder() throws Exception {
    stub.otherwise(BedrockStubReplies.embeddings(512));
    BedrockEmbeddingClient client = client(new BedrockCohereEmbeddingClient(), COHERE, 512);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "one"), text("b", "two"), text("c", "three")));
    assertEquals(1, stub.requests().size());
    JsonNode body = stub.requests().get(0).json();
    assertEquals("search_document", body.get("input_type").asText());
    assertEquals("float", body.get("embedding_types").get(0).asText());
    assertEquals(512, body.get("output_dimension").asInt());
    assertEquals(3, body.get("texts").size());
    assertEquals("/model/cohere.embed-v4%3A0/invoke", stub.requests().get(0).rawPath);
    assertEquals(11, client.lastCallStats().getProviderPromptTokens(), "the X-Amzn-Bedrock-Input-Token-Count header");
    String[] texts = {"one", "two", "three"};
    for (int i = 0; i < 3; i++) {
      assertArrayEquals(ScriptedHttpStub.vectorFor(texts[i], 512), results.get(i).getVector());
    }
  }

  @Test
  public void cohereRequestsRespectMaxInputs() throws Exception {
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "2");
    stub.otherwise(BedrockStubReplies.embeddings(256));
    List<EmbeddingResult> results = client(new BedrockCohereEmbeddingClient(), COHERE, 256)
        .embed(Arrays.asList(text("a", "1"), text("b", "2"), text("c", "3"), text("d", "4"), text("e", "5")));
    assertEquals(3, stub.requests().size());
    results.forEach(r -> assertEquals(EmbeddingResult.Status.OK, r.getStatus()));
  }

  @Test
  public void unsupportedModalityOnTheTextFamilyIsPermanentWithoutARequest() throws Exception {
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(new EmbeddingInput("unsupported", 0, "unsupported", null))).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertTrue(result.getError().contains("not supported"), result.getError());
    assertEquals(0, stub.requests().size());
  }

  @Test
  public void throttlingIsRetriedHonouringRetryAfter() throws Exception {
    stub.then(Reply.awsError(429, "ThrottlingException", "Too many requests").withHeader("Retry-After", "2"))
        .otherwise(BedrockStubReplies.embeddings(256));
    List<Long> sleeps = new ArrayList<>();
    AtomicLong now = new AtomicLong(1_000_000L);
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_BACKOFF_MS_SUFFIX), "5000");
    BedrockEmbeddingClient client = client(seamed(BedrockModelFamily.TITAN_EMBED_TEXT_V2, now, sleeps), TITAN_TEXT, 256);
    EmbeddingResult result = client.embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(Collections.singletonList(2000L), sleeps, "waited the provider's Retry-After");
    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(2, stats.getRequests());
    assertEquals(1, stats.getRetries());
  }

  @Test
  public void throttlingThatPersistsLeavesTheInputRetryable() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "3");
    stub.otherwise(request -> Reply.awsError(429, "ThrottlingException", "Too many requests"));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().contains("ThrottlingException") && result.getError().contains("after 3 attempts"),
        result.getError());
    assertEquals(3, stub.requests().size());
  }

  /**
   * The SDK's clock-skew codes (RequestExpired: "The request is no longer valid due to expired timestamps.",
   * https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html, read 2026-10-27) are retried: the SDK
   * adjusts its clock offset on the failed response, so the next attempt is signed again with the corrected time.
   */
  @Test
  public void clockSkewIsRetriedThenSucceeds() throws Exception {
    stub.then(Reply.awsError(400, "RequestExpired", "The request is no longer valid due to expired timestamps."))
        .then(Reply.awsError(403, "RequestTimeTooSkewed", "The difference between the request time and the current time is too large."))
        .then(Reply.awsError(400, "RequestInTheFuture", "Request is in the future."))
        .otherwise(BedrockStubReplies.embeddings(256));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(4, stub.requests().size());
  }

  /** A skew that persists past the retries leaves its rows RETRYABLE, so the run fails, never PERMANENT row errors. */
  @Test
  public void clockSkewThatPersistsIsNeverARowError() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "10");
    stub.then(BedrockStubReplies.embeddings(256))
        .otherwise(request -> Reply.awsError(400, "RequestExpired", "The request is no longer valid due to expired timestamps."));
    List<EmbeddingResult> results = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Arrays.asList(text("a", "x"), text("b", "y"), text("c", "z")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus(), results.get(0).getError());
    for (EmbeddingResult result : results.subList(1, 3)) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus(), result.getError());
      assertTrue(result.getError().contains("RequestExpired") && result.getError().contains("after 2 attempts"), result.getError());
    }
    assertEquals(5, stub.requests().size());
  }

  /**
   * NotAuthorized ("You don't have permission to perform this action.", HTTP 400 on
   * https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html, read 2026-10-27) rejects the
   * configuration, also after the client embedded something: never a row error.
   */
  @Test
  public void notAuthorizedIsAConfigFailureEvenAfterASuccess() throws Exception {
    stub.then(BedrockStubReplies.embeddings(256))
        .otherwise(request -> Reply.awsError(400, "NotAuthorized", "You don't have permission to perform this action."));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Arrays.asList(text("a", "x"), text("b", "y"))));
    assertTrue(e.getMessage().contains("NotAuthorized"), e.getMessage());
  }

  /** InvalidClientTokenId (403, "The X.509 certificate or AWS access key ID provided does not exist in our records.") too. */
  @Test
  public void invalidClientTokenIdIsAConfigFailure() throws Exception {
    stub.then(BedrockStubReplies.embeddings(256))
        .otherwise(request -> Reply.awsError(403, "InvalidClientTokenId", "The security token included in the request is invalid."));
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Arrays.asList(text("a", "x"), text("b", "y"))));
  }

  /**
   * RequestAbortedException ("aborted before a response could be returned",
   * https://docs.aws.amazon.com/bedrock/latest/APIReference/CommonErrors.html, read 2026-10-27) is retried.
   */
  @Test
  public void abortedRequestIsRetriedThenSucceeds() throws Exception {
    stub.then(Reply.awsError(400, "RequestAbortedException", "The request was aborted before a response could be returned."))
        .otherwise(BedrockStubReplies.embeddings(256));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(2, stub.requests().size());
  }

  /** An abort that persists past the retries leaves its rows RETRYABLE (the run fails), never PERMANENT row errors. */
  @Test
  public void abortedRequestThatPersistsIsNeverARowError() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "10");
    stub.then(BedrockStubReplies.embeddings(256))
        .otherwise(request -> Reply.awsError(400, "RequestAbortedException", "The request was aborted before a response could be returned."));
    List<EmbeddingResult> results = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Arrays.asList(text("a", "x"), text("b", "y")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus(), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus(), results.get(1).getError());
  }

  /**
   * An InvalidSignatureException for an expired signature is a clock skew the SDK corrects (AwsErrorCode treats the code as a possible
   * skew), so it is retried and signed again, like RequestExpired; any other InvalidSignatureException stays the configuration.
   */
  @Test
  public void expiredSignatureIsRetriedOtherSignatureFailuresAreConfig() throws Exception {
    stub.then(Reply.awsError(403, "InvalidSignatureException", "Signature expired: 20261027T000000Z is now earlier than 20261027T001000Z"))
        .otherwise(BedrockStubReplies.embeddings(256));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(2, stub.requests().size());

    stub.otherwise(request -> Reply.awsError(403, "InvalidSignatureException", "The request signature we calculated does not match"));
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Collections.singletonList(text("b", "y"))));
  }

  /** Cohere Embed v4 is asked to refuse an over-long input ("NONE returns an error if the input exceeds the limit"), never to cut it. */
  @Test
  public void cohereRequestRefusesTruncation() throws Exception {
    stub.otherwise(BedrockStubReplies.embeddings(256));
    client(new BedrockCohereEmbeddingClient(), COHERE, 256).embed(Collections.singletonList(text("a", "x")));
    assertEquals("NONE", stub.requests().get(0).json().path("truncate").asText());
  }

  @Test
  public void transientServerErrorsAreRetried() throws Exception {
    stub.then(Reply.awsError(503, "ServiceUnavailableException", "busy"))
        .then(Reply.awsError(408, "ModelTimeoutException", "slow"))
        .then(Reply.awsError(429, "ModelNotReadyException", "warming"))
        .otherwise(BedrockStubReplies.embeddings(256));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(4, stub.requests().size());
  }

  @Test
  public void everyAttemptTakesARateLimitToken() throws Exception {
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "1");
    stub.then(Reply.awsError(429, "ThrottlingException", "slow down")).otherwise(BedrockStubReplies.embeddings(256));
    List<Long> sleeps = new ArrayList<>();
    AtomicLong now = new AtomicLong(1_000_000L);
    BedrockEmbeddingClient client = client(seamed(BedrockModelFamily.TITAN_EMBED_TEXT_V2, now, sleeps), TITAN_TEXT, 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "x"), text("b", "y")));
    results.forEach(r -> assertEquals(EmbeddingResult.Status.OK, r.getStatus(), r.getError()));
    // three attempts (a throttled once, then a, then b) at 1 request/s with a burst of 1: two waits for a token of 1000 ms, less
    // the backoff sleep (at most 1 ms) that refills part of one; without a token per retry the wait would be one 1000 ms
    assertEquals(3, stub.requests().size());
    assertTrue(client.lastCallStats().getRateLimitWaitMillis() >= 1990, client.lastCallStats().toString());
  }

  @Test
  public void rejectedInputAfterASuccessIsPermanentAndItsBatchMatesEmbed() throws Exception {
    stub.otherwise(request -> request.json().path("texts").size() > 1
        || request.json().path("texts").path(0).asText().equals("bad")
        ? Reply.awsError(400, "ValidationException", "Malformed input request")
        : BedrockStubReplies.embeddings(256).apply(request));
    BedrockEmbeddingClient client = client(new BedrockCohereEmbeddingClient(), COHERE, 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "good"), text("b", "bad"), text("c", "fine")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    assertTrue(results.get(1).getError().contains("ValidationException"), results.get(1).getError());
    assertEquals(EmbeddingResult.Status.OK, results.get(2).getStatus());
    assertEquals(4, stub.requests().size(), "the packed request, then each input alone");
    assertEquals(3, client.lastCallStats().getBisectRequests());
  }

  @Test
  public void rejectionBeforeAnythingWasEmbeddedFailsAsConfiguration() throws Exception {
    stub.otherwise(request -> Reply.awsError(400, "ValidationException", "The provided model identifier is invalid."));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Arrays.asList(text("a", "x"), text("b", "y"))));
    assertTrue(e.getMessage().contains("model identifier is invalid"), e.getMessage());
  }

  /**
   * A call of one input that Bedrock rejects (an over-long text, say) settles it PERMANENT, as the OpenAI and Jina clients do: one
   * rejected input cannot tell the binding's fault from the row's, and failing the run would fail every later run on that row.
   */
  @Test
  public void singleRejectedInputBeforeAnythingWasEmbeddedIsPermanent() throws Exception {
    stub.otherwise(request -> Reply.awsError(400, "ValidationException", "Too many input tokens"));
    List<EmbeddingResult> results = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "an over-long text")));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus(), results.get(0).getError());
    assertTrue(results.get(0).getError().contains("Too many input tokens"), results.get(0).getError());
  }

  /** A rejection beside inputs that were only throttled proves nothing about the configuration yet: all stay RETRYABLE. */
  @Test
  public void rejectionBesideThrottledInputsIsRetriedNotAConfigFailure() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "10");
    stub.otherwise(request -> request.json().path("inputText").asText().equals("bad")
        ? Reply.awsError(400, "ValidationException", "Too many input tokens")
        : Reply.awsError(429, "ThrottlingException", "Too many requests"));
    List<EmbeddingResult> results = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Arrays.asList(text("a", "bad"), text("b", "good")));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(0).getStatus());
    assertTrue(results.get(0).getError().contains("ValidationException"), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus());
  }

  @Test
  public void unresolvableCredentialsAreAConfigFailure() {
    BedrockEmbeddingClient client = new BedrockEmbeddingClient(config -> (modelId, body) -> BedrockInvoker.BedrockReply.error(
        BedrockInvoker.BedrockReply.NO_CREDENTIALS, "CredentialsUnavailable", "Unable to load credentials from any of the "
            + "providers in the chain", null), System::currentTimeMillis, Thread::sleep, new Random(1)) {
      @Override
      public BedrockModelFamily family() {
        return BedrockModelFamily.TITAN_EMBED_TEXT_V2;
      }
    };
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(client, TITAN_TEXT, 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("CredentialsUnavailable"), e.getMessage());
  }

  @Test
  public void transientCredentialChainFailuresAreRecognised() {
    assertTrue(SdkBedrockInvoker.hasTransientMarker("Unable to load credentials from any of the providers in the chain "
        + "AwsCredentialsProviderChain(...): [WebIdentityTokenCredentialsProvider(): Rate exceeded (Service: Sts, Status Code: 400)"));
    // the instance-metadata link's whole text in auth 2.29.52, whatever the cause (a timeout included)
    assertTrue(SdkBedrockInvoker.hasTransientMarker("... InstanceProfileCredentialsProvider(): Failed to load credentials from "
        + "IMDS.]"));
    assertFalse(SdkBedrockInvoker.hasTransientMarker("Unable to load credentials from any of the providers in the chain "
        + "AwsCredentialsProviderChain(...): [SystemPropertyCredentialsProvider(): Unable to load credentials from system settings. "
        + "Access key must be specified either via environment variable (AWS_ACCESS_KEY_ID) or system property (aws.accessKeyId).]"));
  }

  @Test
  public void interruptedCallStopsImmediatelyAndIsNotAnOutage() throws Exception {
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "1");
    stub.otherwise(BedrockStubReplies.embeddings(256));
    BedrockEmbeddingClient client = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256);
    Thread.currentThread().interrupt();
    try {
      assertThrows(InterruptedException.class, () -> client.embed(Arrays.asList(text("a", "x"), text("b", "y"))));
    } finally {
      Thread.interrupted();
    }
    assertEquals(0, stub.requests().size());
    assertEquals(EmbeddingResult.Status.OK, client.embed(Collections.singletonList(text("c", "z"))).get(0).getStatus());
  }

  @Test
  public void accessDeniedIsAConfigFailureWithoutSecrets() {
    stub.otherwise(request -> Reply.awsError(403, "AccessDeniedException", "You don't have access to the model"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("AccessDeniedException"), e.getMessage());
    assertFalse(e.getMessage().contains(SECRET));
    assertEquals(1, stub.requests().size(), "a config failure is not retried");
  }

  @Test
  public void wrongDimensionAnswerIsAConfigFailure() {
    stub.otherwise(BedrockStubReplies.embeddings(1024));
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256).embed(Collections.singletonList(text("a", "x"))));
  }

  @Test
  public void malformedAnswerIsRetryable() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    stub.otherwise(request -> Reply.json(200, "{\"embeddings\":{\"float\":[]}}"));
    EmbeddingResult result = client(new BedrockCohereEmbeddingClient(), COHERE, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().startsWith("malformed answer"), result.getError());
  }

  @Test
  public void outageShortCircuitStopsCalling() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "2");
    stub.otherwise(request -> Reply.awsError(500, "InternalServerException", "boom"));
    BedrockEmbeddingClient client = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "x"), text("b", "y"), text("c", "z")));
    results.forEach(r -> assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, r.getStatus()));
    assertTrue(results.get(2).getError().contains("outage"), results.get(2).getError());
    assertEquals(2, stub.requests().size());
    assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(Collections.singletonList(text("d", "w"))));
  }

  @Test
  public void configurationIsCheckedAtInit() {
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 1536), "Titan text v2 has no 1536 output");
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new BedrockCohereEmbeddingClient(), TITAN_TEXT, 256), "a Titan id on the Cohere client");
    props.setProperty(key(BedrockClientConfig.ENDPOINT_SUFFIX), "https://user:pw@bedrock.example.com");
    assertThrows(EmbeddingClientConfigException.class, () -> client(new BedrockCohereEmbeddingClient(), COHERE, 256));
    // the driver-side check of the factory refuses the same configuration before any Spark work
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(spec(COHERE, 256), props));
  }

  @Test
  public void opaqueArnIsServedByTheClientClassItNames() throws Exception {
    String arn = "arn:aws:bedrock:us-west-2:123456789012:application-inference-profile/abc123";
    stub.otherwise(BedrockStubReplies.embeddings(256));
    EmbeddingResult result = client(new BedrockCohereEmbeddingClient(), arn, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertTrue(stub.requests().get(0).rawPath.startsWith("/model/arn%3Aaws%3Abedrock"), stub.requests().get(0).rawPath);
  }

  @Test
  public void emptyTextIsPermanentWithoutARequest() throws Exception {
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "  "))).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertTrue(result.isKnownInputFailure(), "local validation must not become a whole-call refusal");
    assertEquals(0, stub.requests().size());
  }

  // ---------------------------------------------------------------- helpers

  @ParameterizedTest
  @ValueSource(strings = {"1e100", "NaN"})
  public void nonFiniteVectorElementsAreRetryable(String element) throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    String values = element + ",0.25".repeat(255);
    stub.otherwise(request -> Reply.json(200, "{\"embedding\":[" + values + "]}"));
    EmbeddingResult result = client(new BedrockTitanTextEmbeddingClient(), TITAN_TEXT, 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().startsWith("malformed answer"), result.getError());
    assertNull(result.getVector());
    assertEquals(1, stub.requests().size());
  }

  private BedrockEmbeddingClient client(BedrockEmbeddingClient client, String model, int dimension) {
    client.init(spec(model, dimension), props);
    return client;
  }

  /** A client of {@code family} on a fake clock whose sleeper advances it and records each wait. */
  private static BedrockEmbeddingClient seamed(BedrockModelFamily family, AtomicLong now, List<Long> sleeps) {
    return new BedrockEmbeddingClient(SdkBedrockInvoker::create, now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    }, new Random(7)) {
      @Override
      public BedrockModelFamily family() {
        return family;
      }
    };
  }

  private static EmbeddingModelSpec spec(String model, int dimension) {
    return new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, dimension, null, BedrockEmbeddingClient.PROVIDER, model);
  }

  private static EmbeddingInput text(String sha, String text) {
    return new EmbeddingInput(sha, 0, EmbeddingTableSchema.MODALITY_TEXT, text);
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }

  private static void restore(String property, String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }
}
