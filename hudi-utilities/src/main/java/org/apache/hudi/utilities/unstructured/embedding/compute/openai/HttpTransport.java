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

import java.io.IOException;
import java.util.Map;

/**
 * The HTTP calls of the OpenAI-compatible client: one blocking request at a time, the whole body in memory (a request is
 * bounded by the caller's micro batch, a response by that batch's vectors).
 */
interface HttpTransport {

  /**
   * @throws IOException on a transport failure, including a timeout ({@link java.net.http.HttpTimeoutException})
   */
  HttpReply post(String url, Map<String, String> headers, byte[] body, int timeoutMs) throws IOException, InterruptedException;

  /**
   * @throws IOException on a transport failure, including a timeout
   */
  HttpReply get(String url, Map<String, String> headers, int timeoutMs) throws IOException, InterruptedException;
}
