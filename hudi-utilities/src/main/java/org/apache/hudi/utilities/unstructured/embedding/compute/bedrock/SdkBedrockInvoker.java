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

import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link BedrockInvoker} over the AWS SDK v2 {@link BedrockRuntimeClient}, which the utilities bundle relocates to
 * {@code org.apache.hudi.software.amazon.awssdk}. The client is built WITHOUT a credentials provider, so the SDK default chain
 * signs every request (system properties, environment, web identity token file, profile, container, instance profile); no
 * credential is ever configured or read by Hudi. SDK retries are off: the caller's retry loop sends every attempt, so each one
 * takes a rate-limit token and is counted. One SDK client per JVM per region, endpoint and timeout is shared by the tasks of an
 * executor, since each client owns a connection pool.
 */
public final class SdkBedrockInvoker implements BedrockInvoker {

  private static final Map<String, BedrockRuntimeClient> CLIENTS = new ConcurrentHashMap<>();
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
  /** The SDK's message when no link of the default chain yields credentials. */
  private static final String NO_CREDENTIALS = "Unable to load credentials";
  /**
   * Lower-case texts of a transient link failure in the credentials chain's aggregate message. The instance-metadata and
   * container links report any failure, a timeout included, only as "Failed to load credentials from IMDS." / "... from
   * metadata service." (auth 2.29.52), so those count as transient too: on a host with no such endpoint the run then fails after
   * the compute step's retries instead of at once.
   */
  private static final List<String> TRANSIENT_MARKERS = Collections.unmodifiableList(Arrays.asList(
      "throttl", "rate exceeded", "status code: 500", "status code: 502", "status code: 503", "status code: 504",
      "unable to execute http request", "timed out", "timeout", "connection reset", "connection refused",
      "failed to load credentials from imds", "failed to load credentials from metadata service"));
  /** InvokeModel's response header with the request's input token count. */
  static final String INPUT_TOKEN_COUNT_HEADER = "X-Amzn-Bedrock-Input-Token-Count";
  /** The SDK's message when no region is configured and the default region chain finds none. */
  private static final String NO_REGION = "Unable to load region";

  private final BedrockRuntimeClient client;

  private SdkBedrockInvoker(BedrockRuntimeClient client) {
    this.client = client;
  }

  /**
   * The invoker of {@code config}, over the JVM's shared SDK client for its region, endpoint and timeout.
   *
   * @throws EmbeddingClientConfigException when the SDK client cannot be built, e.g. no region is configured or found
   */
  public static SdkBedrockInvoker create(BedrockClientConfig config) {
    String cacheKey = config.region() + "|" + config.endpoint() + "|" + config.settings().requestTimeoutMs();
    try {
      return new SdkBedrockInvoker(CLIENTS.computeIfAbsent(cacheKey, k -> buildClient(config)));
    } catch (SdkClientException e) {
      String message = String.valueOf(e.getMessage());
      throw new EmbeddingClientConfigException("Cannot build the Bedrock runtime client for model " + config.alias()
          + (message.contains(NO_REGION) ? ": no AWS region; set " + config.regionKey() + " or AWS_REGION" : "") + ": " + message);
    }
  }

  /** The SDK client of {@code config}: region and endpoint when configured, no credentials provider, no SDK retries. */
  static BedrockRuntimeClient buildClient(BedrockClientConfig config) {
    Duration timeout = Duration.ofMillis(config.settings().requestTimeoutMs());
    BedrockRuntimeClientBuilder builder = BedrockRuntimeClient.builder()
        // explicit: with more than one SDK HTTP client on the classpath the SDK refuses to pick one
        .httpClientBuilder(ApacheHttpClient.builder().connectionTimeout(CONNECT_TIMEOUT).socketTimeout(timeout))
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .apiCallAttemptTimeout(timeout)
            .retryStrategy(AwsRetryStrategy.doNotRetry())
            .build());
    if (config.region() != null) {
      builder.region(Region.of(config.region()));
    }
    if (config.endpoint() != null) {
      builder.endpointOverride(URI.create(config.endpoint()));
    }
    return builder.build();
  }

  /**
   * Error answers come back as a {@link BedrockReply}; credentials that no link of the default chain has come back as
   * {@link BedrockReply#NO_CREDENTIALS}; a transport failure or a transient failure inside the chain (STS throttled or failing,
   * an instance-metadata timeout) is thrown as an {@link IOException}; an interrupt as an {@link InterruptedException}.
   */
  @Override
  public BedrockReply invoke(String modelId, byte[] body) throws IOException, InterruptedException {
    InvokeModelRequest request = InvokeModelRequest.builder()
        .modelId(modelId)
        .contentType("application/json")
        .accept("application/json")
        .body(SdkBytes.fromByteArrayUnsafe(body))
        .build();
    try {
      InvokeModelResponse response = client.invokeModel(request);
      return BedrockReply.ok(response.body().asByteArrayUnsafe(), inputTokens(response));
    } catch (AwsServiceException e) {
      return serviceError(e);
    } catch (SdkClientException e) {
      if (e instanceof AbortedException || Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        throw new InterruptedException("Bedrock request interrupted: " + e.getMessage());
      }
      String message = String.valueOf(e.getMessage());
      if (message.contains(NO_CREDENTIALS) && !isTransient(e.getCause()) && !hasTransientMarker(message)) {
        return BedrockReply.error(BedrockReply.NO_CREDENTIALS, "CredentialsUnavailable", e.getMessage(), null);
      }
      throw new IOException(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
    }
  }

  /** The error reply of a service exception; its error details are absent when a proxy or VPC endpoint answers instead of Bedrock. */
  private static BedrockReply serviceError(AwsServiceException e) {
    AwsErrorDetails details = e.awsErrorDetails();
    String code = details != null && details.errorCode() != null ? details.errorCode() : e.getClass().getSimpleName();
    String message = details != null && details.errorMessage() != null ? details.errorMessage() : e.getMessage();
    String retryAfter = details != null && details.sdkHttpResponse() != null
        ? details.sdkHttpResponse().firstMatchingHeader("Retry-After").orElse(null) : null;
    return BedrockReply.error(e.statusCode(), code, message, retryAfter);
  }

  /**
   * Whether the credentials chain's aggregate message, which lists each link's failure as text and carries no cause
   * (AwsCredentialsProviderChain in SDK 2.29.52), reports a transient one: a throttled or failing STS call, an HTTP request that
   * could not be executed, a timeout.
   */
  static boolean hasTransientMarker(String message) {
    String lower = message.toLowerCase(Locale.ROOT);
    for (String marker : TRANSIENT_MARKERS) {
      if (lower.contains(marker)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a failure under the credentials chain's aggregate error is transient: an I/O failure or timeout, or a service answer
   * that is throttling, retryable or 5xx (STS under IRSA). Each failing link of the chain is a cause or a suppressed exception.
   */
  private static boolean isTransient(Throwable failure) {
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (cause instanceof IOException || cause instanceof ApiCallTimeoutException
          || cause instanceof ApiCallAttemptTimeoutException) {
        return true;
      }
      if (cause instanceof SdkServiceException) {
        SdkServiceException service = (SdkServiceException) cause;
        if (service.isThrottlingException() || service.retryable() || service.statusCode() >= 500) {
          return true;
        }
      }
      for (Throwable suppressed : cause.getSuppressed()) {
        if (isTransient(suppressed)) {
          return true;
        }
      }
    }
    return false;
  }

  private static long inputTokens(InvokeModelResponse response) {
    String header = response.sdkHttpResponse() == null ? null
        : response.sdkHttpResponse().firstMatchingHeader(INPUT_TOKEN_COUNT_HEADER).orElse(null);
    if (header == null) {
      return -1L;
    }
    try {
      return Long.parseLong(header.trim());
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
