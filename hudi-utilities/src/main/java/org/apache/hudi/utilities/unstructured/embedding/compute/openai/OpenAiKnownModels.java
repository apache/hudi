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

import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The OpenAI embedding models whose limits are documented (output dimensions and
 * max input tokens); any other model id needs its limits configured. The per-input limit is 8191 tokens, the
 * stricter of the docs' 8192 and tiktoken/jtokkit's 8191.
 */
final class OpenAiKnownModels {

  /** Per-input token limit of the three documented models. */
  static final int MAX_INPUT_TOKENS = 8191;
  /** Encoding of the three documented models. */
  static final String ENCODING = "cl100k_base";

  private static final Map<String, KnownModel> MODELS = new HashMap<>();

  static {
    MODELS.put("text-embedding-3-small", new KnownModel(1536, true));
    MODELS.put("text-embedding-3-large", new KnownModel(3072, true));
    MODELS.put("text-embedding-ada-002", new KnownModel(1536, false));
  }

  private OpenAiKnownModels() {
  }

  static Option<KnownModel> lookup(String model) {
    return Option.ofNullable(model == null ? null : MODELS.get(model));
  }

  /** One documented model. */
  static final class KnownModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int nativeDimension;
    private final boolean dimensionsSettable;

    KnownModel(int nativeDimension, boolean dimensionsSettable) {
      this.nativeDimension = nativeDimension;
      this.dimensionsSettable = dimensionsSettable;
    }

    /** Vector length the model returns without a {@code dimensions} parameter. */
    int nativeDimension() {
      return nativeDimension;
    }

    /** Whether the model accepts {@code dimensions} (text-embedding-3 and later), from 1 up to the native length. */
    boolean dimensionsSettable() {
      return dimensionsSettable;
    }

    int maxInputTokens() {
      return MAX_INPUT_TOKENS;
    }

    String encoding() {
      return ENCODING;
    }
  }
}
