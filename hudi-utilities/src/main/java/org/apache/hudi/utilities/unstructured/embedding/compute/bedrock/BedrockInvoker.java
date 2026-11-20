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

import java.io.IOException;

/**
 * Sends one Bedrock InvokeModel request. The production implementation is {@link SdkBedrockInvoker}; tests point it at a local
 * stub endpoint with {@code .endpoint}.
 */
public interface BedrockInvoker {

  /**
   * @return the provider's answer, an error answer included; never null
   * @throws IOException a transient failure (connection, timeout, a transient failure resolving credentials): the request may
   *     be retried
   * @throws InterruptedException the calling thread was interrupted
   */
  BedrockReply invoke(String modelId, byte[] body) throws IOException, InterruptedException;

  /** One InvokeModel answer: the body of a success, or the status, error code and message of a failure. */
  final class BedrockReply {
    /** Status of an answer that never reached Bedrock because the request could not be signed. */
    public static final int NO_CREDENTIALS = -1;
    private final int status;
    private final byte[] body;
    private final String errorCode;
    private final String message;
    private final String retryAfter;
    private final long inputTokens;

    private BedrockReply(int status, byte[] body, String errorCode, String message, String retryAfter, long inputTokens) {
      this.status = status;
      this.body = body;
      this.errorCode = errorCode;
      this.message = message;
      this.retryAfter = retryAfter;
      this.inputTokens = inputTokens;
    }

    /** @param inputTokens Bedrock's input token count of the request, -1 when not reported */
    public static BedrockReply ok(byte[] body, long inputTokens) {
      return new BedrockReply(200, body, null, null, null, inputTokens);
    }

    /** @param retryAfter the {@code Retry-After} header, or null */
    public static BedrockReply error(int status, String errorCode, String message, String retryAfter) {
      return new BedrockReply(status, null, errorCode, message, retryAfter, -1L);
    }

    public int status() {
      return status;
    }

    public byte[] body() {
      return body;
    }

    public String errorCode() {
      return errorCode;
    }

    public String message() {
      return message;
    }

    public String retryAfter() {
      return retryAfter;
    }

    /** The input tokens Bedrock counted for a success, -1 when not reported. */
    public long inputTokens() {
      return inputTokens;
    }
  }
}
