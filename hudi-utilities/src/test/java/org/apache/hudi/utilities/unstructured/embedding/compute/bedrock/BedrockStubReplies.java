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

import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Recorded;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Reply;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.Function;

/**
 * Successful InvokeModel answers in each family's documented response shape, built from the request: a text's vector is
 * {@link ScriptedHttpStub#vectorFor} of the text.
 */
public final class BedrockStubReplies {

  /** Cohere answers carry Bedrock's input token count header; the stub reports 11 per request. */
  static final String INPUT_TOKEN_COUNT_HEADER = "X-Amzn-Bedrock-Input-Token-Count";

  private BedrockStubReplies() {
  }

  /** Answers any family's request with vectors of {@code dimension} floats. */
  public static Function<Recorded, Reply> embeddings(int dimension) {
    return request -> {
      JsonNode body = request.json();
      if (body.has("inputText")) {
        return Reply.json(200, "{\"embedding\":" + ScriptedHttpStub.vectorJson(body.get("inputText").asText(), dimension)
            + ",\"inputTextTokenCount\":3}");
      }
      StringBuilder floats = new StringBuilder("[");
      JsonNode inputs = body.get("texts");
      for (int i = 0; i < inputs.size(); i++) {
        String seed = inputs.get(i).asText();
        floats.append(i == 0 ? "" : ",").append(ScriptedHttpStub.vectorJson(seed, dimension));
      }
      return Reply.json(200, "{\"id\":\"stub\",\"embeddings\":{\"float\":" + floats + "]},\"texts\":[],"
          + "\"response_type\":\"embeddings_by_type\"}").withHeader(INPUT_TOKEN_COUNT_HEADER, "11");
    };
  }
}
