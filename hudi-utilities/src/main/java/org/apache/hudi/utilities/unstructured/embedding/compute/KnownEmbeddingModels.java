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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.util.Option;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * What each known embedding model can embed. A model's declared modalities
 * ({@code hoodie.unstructured.embedding.model.<alias>.modalities}) must not go beyond its entry here; a model name with no entry
 * (an ARN of a provisioned model, a model behind {@code hf-tei}) is trusted as declared, and a wrong declaration then fails the
 * round when the provider rejects the inputs ({@code EMBED-COMPUTE-REJECTED-AS-A-WHOLE}).
 * <ul>
 *   <li>{@code amazon.titan-embed-text-v2}: text;</li>
 *   <li>{@code cohere.embed-v4}: text, image;</li>
 *   <li>{@code amazon.titan-embed-image-v1} (Titan Multimodal Embeddings G1): text, image;</li>
 *   <li>{@code multimodalembedding@001} (Vertex AI): text, image;</li>
 *   <li>the OpenAI {@code text-embedding-3-*} models: text.</li>
 * </ul>
 * A Bedrock id matches its entry with or without its {@code :<version>} suffix, a cross-region inference profile prefix
 * ({@code us.}) or an ARN path ({@code arn:aws:bedrock:…/}).
 */
public final class KnownEmbeddingModels {

  private static final List<Entry> ENTRIES = Collections.unmodifiableList(Arrays.asList(
      Entry.bedrock("amazon.titan-embed-text-v2", EmbeddingClient.TEXT_ONLY),
      Entry.bedrock("cohere.embed-v4", EmbeddingClient.TEXT_AND_IMAGE),
      Entry.bedrock("amazon.titan-embed-image-v1", EmbeddingClient.TEXT_AND_IMAGE),
      Entry.exact("multimodalembedding@001", EmbeddingClient.TEXT_AND_IMAGE),
      Entry.prefix("text-embedding-3-", EmbeddingClient.TEXT_ONLY)));

  private KnownEmbeddingModels() {
  }

  /** The modalities known model {@code modelName} can embed; empty for a model with no entry, whose declaration is trusted. */
  public static Option<Set<String>> modalities(String modelName) {
    if (modelName == null) {
      return Option.empty();
    }
    String id = modelName.trim().toLowerCase(Locale.ROOT);
    for (Entry entry : ENTRIES) {
      if (entry.matches(id)) {
        return Option.of(entry.modalities);
      }
    }
    return Option.empty();
  }

  /** One known model, or one family of models named alike, and what it can embed. */
  private static final class Entry {
    private enum Match { BEDROCK, EXACT, PREFIX }

    private final String id;
    private final Match match;
    private final Set<String> modalities;

    private Entry(String id, Match match, Set<String> modalities) {
      this.id = id;
      this.match = match;
      this.modalities = modalities;
    }

    private static Entry bedrock(String baseModelId, Set<String> modalities) {
      return new Entry(baseModelId, Match.BEDROCK, modalities);
    }

    private static Entry exact(String modelName, Set<String> modalities) {
      return new Entry(modelName, Match.EXACT, modalities);
    }

    private static Entry prefix(String namePrefix, Set<String> modalities) {
      return new Entry(namePrefix, Match.PREFIX, modalities);
    }

    /** Whether lower-case model name {@code name} names this entry. */
    private boolean matches(String name) {
      switch (match) {
        case EXACT:
          return name.equals(id);
        case PREFIX:
          return name.startsWith(id);
        case BEDROCK:
          // the id after an ARN's last '/' and before its ':' version, alone or after a cross-region profile prefix
          String base = name;
          int slash = name.indexOf('/');
          if (slash >= 0) {
            String[] arn = name.substring(0, slash).split(":", -1);
            if (arn.length != 6 || !"arn".equals(arn[0])
                || !("aws".equals(arn[1]) || arn[1].startsWith("aws-")) || !"bedrock".equals(arn[2])
                || !("foundation-model".equals(arn[5]) || "inference-profile".equals(arn[5]))) {
              return false;
            }
            base = name.substring(slash + 1);
          }
          int version = base.indexOf(':');
          base = version < 0 ? base : base.substring(0, version);
          return base.equals(id) || base.endsWith("." + id);
        default:
          throw new UnsupportedOperationException("Unhandled match " + match);
      }
    }
  }
}
