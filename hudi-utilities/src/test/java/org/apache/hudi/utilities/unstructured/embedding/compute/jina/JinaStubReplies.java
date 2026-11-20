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

package org.apache.hudi.utilities.unstructured.embedding.compute.jina;

import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Recorded;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Reply;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Function;

/**
 * Jina {@code /v1/embeddings} answers in the documented shape (EmbeddingResponse of its OpenAPI document 2026.10.13.1624), built
 * from the request, entries in REVERSED index order: a text's vector is {@link ScriptedHttpStub#vectorFor} of the text.
 */
public final class JinaStubReplies {

  private JinaStubReplies() {
  }

  public static Function<Recorded, Reply> embeddings(int dimension) {
    return request -> {
      JsonNode input = request.json().get("input");
      StringBuilder data = new StringBuilder("[");
      for (int i = input.size() - 1; i >= 0; i--) {
        JsonNode item = input.get(i);
        String seed = item.get("text").asText();
        data.append(i == input.size() - 1 ? "" : ",").append("{\"object\":\"embedding\",\"index\":").append(i)
            .append(",\"embedding\":").append(ScriptedHttpStub.vectorJson(seed, dimension)).append('}');
      }
      return Reply.json(200, "{\"model\":\"" + request.json().get("model").asText() + "\",\"object\":\"list\",\"usage\":"
          + "{\"total_tokens\":" + (5 * input.size()) + ",\"prompt_tokens\":" + (5 * input.size()) + "},\"data\":" + data + "]}");
    };
  }

  /** A Jina error: {@code {"detail","code"}}. */
  public static Reply error(int status, String code, String detail) {
    return Reply.json(status, "{\"detail\":\"" + detail + "\",\"code\":\"" + code + "\",\"request_id\":\"r1\"}");
  }
}
