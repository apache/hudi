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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A received HTTP response: status, headers (names compared case-insensitively) and the body bytes.
 */
final class HttpReply {

  private final int status;
  private final Map<String, List<String>> headers;
  private final byte[] body;

  HttpReply(int status, Map<String, List<String>> headers, byte[] body) {
    this.status = status;
    this.headers = headers == null ? Collections.emptyMap() : headers;
    this.body = body == null ? new byte[0] : body;
  }

  int status() {
    return status;
  }

  /** The first value of header {@code name}, matched case-insensitively, or null. */
  String header(String name) {
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name) && !entry.getValue().isEmpty()) {
        return entry.getValue().get(0);
      }
    }
    return null;
  }

  byte[] body() {
    return body;
  }
}
