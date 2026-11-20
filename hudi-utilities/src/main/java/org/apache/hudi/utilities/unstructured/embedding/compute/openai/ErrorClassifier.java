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

import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Classifies an OpenAI-compatible provider response into a {@link ProviderResponseKind}: what the client does with
 * the request that drew it. The Bedrock and Jina clients classify their own answers.
 */
public final class ErrorClassifier {

  private static final String INSUFFICIENT_QUOTA = "insufficient_quota";
  /**
   * OpenAI's billing 429 codes: "This is a billing error, not a rate limit." "Retrying billing, spend, or quota errors won't
   * restore access." (https://developers.openai.com/api/docs/guides/error-codes, read 2026-10-27).
   */
  private static final Set<String> BILLING_CODES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(INSUFFICIENT_QUOTA,
      "credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded",
      "organization_usage_limit_exceeded")));
  /** Request parameters other than {@code input}: an error naming one rejects the configuration, not an input. */
  private static final Set<String> REQUEST_PARAMS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("model", "dimensions", "encoding_format", "user")));

  private ErrorClassifier() {
  }

  static ProviderResponseKind classify(OpenAiProvider provider, int status, ErrorBody body) {
    if (status >= 200 && status < 300) {
      return ProviderResponseKind.OK;
    }
    if (status == 429) {
      return rateLimited(body);
    }
    // 409 as the official SDK retries it ("408 Request Timeout, 409 Conflict, 429 Rate Limit, and >=500 Internal errors are all
    // retried by default", openai-python _base_client._should_retry)
    if (status == 408 || status == 409 || status >= 500) {
      return ProviderResponseKind.RETRYABLE;
    }
    if (status == 424 && provider == OpenAiProvider.HF_TEI) {
      // TEI documents 424 for backend inference failures; a non-embedding model is rejected by GET /info at init
      return ProviderResponseKind.RETRYABLE;
    }
    if (status == 400 || status == 422) {
      return rejected(body);
    }
    if (status == 413) {
      return ProviderResponseKind.INPUT;
    }
    return ProviderResponseKind.CONFIG;
  }

  /** A 429: an exhausted quota or billing limit is QUOTA, anything else a rate limit to retry. */
  private static ProviderResponseKind rateLimited(ErrorBody body) {
    return BILLING_CODES.contains(body.code()) || INSUFFICIENT_QUOTA.equals(body.type())
        ? ProviderResponseKind.QUOTA : ProviderResponseKind.RETRYABLE;
  }

  /** A 400 or 422: naming a request parameter other than input rejects the configuration, else the input. */
  private static ProviderResponseKind rejected(ErrorBody body) {
    return body.param() != null && REQUEST_PARAMS.contains(body.param()) ? ProviderResponseKind.CONFIG : ProviderResponseKind.INPUT;
  }

  /**
   * The fields of an error response: OpenAI/Azure {@code {"error":{"message","type","param","code"}}}, TEI native
   * {@code {"error":"…","error_type":"…"}}, TEI OpenAI-compatible {@code {"message","code","error_type"}}, anything else
   * as raw text. Missing fields are null.
   */
  static final class ErrorBody {

    /** Longest message kept from a provider error. */
    static final int MAX_MESSAGE_CHARS = 1024;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String fullMessage;
    private final String message;
    private final String param;
    private final String code;
    private final String type;

    private ErrorBody(String message, String param, String code, String type) {
      this.fullMessage = message;
      this.message = message == null || message.length() <= MAX_MESSAGE_CHARS ? message : message.substring(0, MAX_MESSAGE_CHARS);
      this.param = param;
      this.code = code;
      this.type = type;
    }

    static ErrorBody parse(byte[] body) {
      return parse(body, UnaryOperator.identity());
    }

    /**
     * @param redactor applied to the provider's message BEFORE it is cut to {@link #MAX_MESSAGE_CHARS}, so a secret
     *                 crossing the cut is still matched whole
     */
    static ErrorBody parse(byte[] body, UnaryOperator<String> redactor) {
      ErrorBody raw = parseRaw(body);
      return new ErrorBody(raw.fullMessage == null ? null : redactor.apply(raw.fullMessage), raw.param, raw.code, raw.type);
    }

    private static ErrorBody parseRaw(byte[] body) {
      if (body == null || body.length == 0) {
        return new ErrorBody(null, null, null, null);
      }
      String text = new String(body, StandardCharsets.UTF_8);
      try {
        JsonNode root = MAPPER.readTree(text);
        JsonNode error = root == null ? null : root.get("error");
        if (error != null && error.isObject()) {
          return new ErrorBody(text(error, "message"), text(error, "param"), text(error, "code"), text(error, "type"));
        }
        if (error != null && error.isTextual()) {
          return new ErrorBody(error.asText(), null, null, text(root, "error_type"));
        }
        JsonNode message = root == null ? null : root.get("message");
        if (message != null && message.isTextual()) {
          // TEI's OpenAI-compatible route: {message, code, error_type} (its own example spells the type field "type")
          String type = text(root, "error_type");
          return new ErrorBody(message.asText(), null, null, type != null ? type : text(root, "type"));
        }
      } catch (IOException e) {
        // not JSON: keep the raw text as the message
      }
      return new ErrorBody(text, null, null, null);
    }

    private static String text(JsonNode node, String field) {
      JsonNode value = node.get(field);
      return value == null || value.isNull() ? null : value.asText();
    }

    String message() {
      return message;
    }

    String param() {
      return param;
    }

    String code() {
      return code;
    }

    String type() {
      return type;
    }
  }
}
