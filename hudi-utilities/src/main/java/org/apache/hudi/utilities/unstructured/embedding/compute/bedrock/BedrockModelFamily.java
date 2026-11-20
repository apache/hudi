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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The Amazon Bedrock embedding model families the compute step embeds with. Bedrock has no common request format: each family
 * has its own InvokeModel request and response shape, output dimensions, per-request limits and modalities, and its own client
 * class, so {@code .client.class} names the family even when the model id is an ARN that does not. Another family is one more
 * entry here (and its client class), never a new provider or setting.
 * <ul>
 *   <li>{@link #TITAN_EMBED_TEXT_V2}: {@code amazon.titan-embed-text-v2}, text only; {@code {"inputText","dimensions"}} →
 *       {@code embedding}; 256, 512 or 1024 dimensions; one input per request
 *       (https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-text.html);</li>
 *   <li>{@link #COHERE_EMBED_V4}: {@code cohere.embed-v4}, text and images;
 *       {@code {"input_type":"search_document","truncate":"NONE","embedding_types":["float"],"output_dimension","texts"}} for
 *       text, the same with {@code "images":["data:<mime>;base64,…"]} in place of {@code texts} for an image (the two are never
 *       sent together) → {@code embeddings.float}; 256, 512, 1024 or 1536; up to 96 texts or 96 images per request, within
 *       128k tokens and about 20 MB of payload; an image of PNG, JPEG, WebP or GIF up to 5 MB
 *       (https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed-v4.html);</li>
 *   <li>{@link #TITAN_EMBED_IMAGE_V1}: {@code amazon.titan-embed-image-v1} (Titan Multimodal Embeddings G1), text and images;
 *       {@code {"inputText"}} or {@code {"inputImage":<base64>}} with {@code {"embeddingConfig":{"outputEmbeddingLength"}}} →
 *       {@code embedding}; 256, 384 or 1024; one input per request; an image of PNG or JPEG up to 25 MB and 2048 x 2048 pixels
 *       (https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-mm.html).</li>
 * </ul>
 * Images are packed into requests like texts: up to the family's inputs per request ({@code .request.max.inputs} lowers it),
 * within its payload and token limits; a call holds the prepared images of one request, plus at most the next image.
 */
public enum BedrockModelFamily {
  TITAN_EMBED_TEXT_V2("amazon.titan-embed-text-v2", Arrays.asList(256, 512, 1024), 1, EmbeddingClient.TEXT_ONLY,
      0L, 0, Collections.emptySet()),
  COHERE_EMBED_V4("cohere.embed-v4", Arrays.asList(256, 512, 1024, 1536), 96, EmbeddingClient.TEXT_AND_IMAGE,
      Limits.COHERE_MAX_IMAGE_BYTES, 0, Limits.mimeTypes(ImageContentTypes.PNG, ImageContentTypes.JPEG, Limits.WEBP,
          ImageContentTypes.GIF)),
  TITAN_EMBED_IMAGE_V1("amazon.titan-embed-image-v1", Arrays.asList(256, 384, 1024), 1, EmbeddingClient.TEXT_AND_IMAGE,
      Limits.TITAN_MULTIMODAL_MAX_IMAGE_BYTES, Limits.TITAN_MULTIMODAL_MAX_IMAGE_SIDE,
      Limits.mimeTypes(ImageContentTypes.PNG, ImageContentTypes.JPEG));

  private final String baseModelId;
  private final List<Integer> dimensions;
  private final int maxInputsPerRequest;
  private final Set<String> modalities;
  private final long maxImageBytes;
  private final int maxImageSide;
  private final Set<String> imageMimeTypes;

  BedrockModelFamily(String baseModelId, List<Integer> dimensions, int maxInputsPerRequest, Set<String> modalities,
                     long maxImageBytes, int maxImageSide, Set<String> imageMimeTypes) {
    this.baseModelId = baseModelId;
    this.dimensions = Collections.unmodifiableList(dimensions);
    this.maxInputsPerRequest = maxInputsPerRequest;
    this.modalities = modalities;
    this.maxImageBytes = maxImageBytes;
    this.maxImageSide = maxImageSide;
    this.imageMimeTypes = imageMimeTypes;
  }

  /** The foundation model id without its version suffix, e.g. {@code cohere.embed-v4}. */
  public String baseModelId() {
    return baseModelId;
  }

  /** The output dimensions the family accepts. */
  public List<Integer> dimensions() {
    return dimensions;
  }

  /** Most inputs (texts, or images) one request carries. */
  public int maxInputsPerRequest() {
    return maxInputsPerRequest;
  }

  /** The modalities the family has a request format for. */
  public Set<String> modalities() {
    return modalities;
  }

  /** The largest encoded image one request carries, in bytes before base64; 0 for a family that embeds no image. */
  public long maxImageBytes() {
    return maxImageBytes;
  }

  /** The longest image side the family accepts, in pixels; 0 when the family sets no limit (it resamples). */
  public int maxImageSide() {
    return maxImageSide;
  }

  /**
   * The text tokens one input may hold, in the model's own tokenizer; 0 when the family's text limit is far above any prep chunk
   * size (Titan Text v2 takes 8,192 tokens; Cohere v4 rejects an over-long text, truncate NONE).
   */
  public int maxTextTokens() {
    return this == TITAN_EMBED_IMAGE_V1 ? Limits.TITAN_MULTIMODAL_MAX_TEXT_TOKENS : 0;
  }

  /** The image types the family accepts; empty for a family that embeds no image. */
  public Set<String> imageMimeTypes() {
    return imageMimeTypes;
  }

  /**
   * The family a model id names: a foundation model id ({@code cohere.embed-v4:0}), a cross-region inference profile id
   * ({@code us.cohere.embed-v4:0}) or either one's ARN. Empty for an id that names no supported family, such as an application
   * inference profile or provisioned-throughput ARN, whose client is chosen with {@code .client.class}.
   */
  public static Option<BedrockModelFamily> fromModelId(String modelId) {
    if (modelId == null) {
      return Option.empty();
    }
    String id = modelId.trim();
    id = id.substring(id.lastIndexOf('/') + 1);
    int version = id.indexOf(':');
    String base = version < 0 ? id : id.substring(0, version);
    for (BedrockModelFamily family : values()) {
      if (base.equals(family.baseModelId) || base.endsWith("." + family.baseModelId)) {
        return Option.of(family);
      }
    }
    return Option.empty();
  }

  /** The documented per-image limits of the families (see the class comment for the sources). */
  private static final class Limits {
    /** Cohere Embed v4: an image "can be up to 5 MB in size"; read as 5,000,000 bytes of the encoded image, the smaller reading. */
    private static final long COHERE_MAX_IMAGE_BYTES = 5_000_000L;
    /** Titan Multimodal Embeddings G1: "Max input image size – 25 MB", read as 25,000,000 bytes. */
    private static final long TITAN_MULTIMODAL_MAX_IMAGE_BYTES = 25_000_000L;
    /** Titan Multimodal Embeddings G1: "Max input image resolution – 2048 x 2048 pixels". */
    private static final int TITAN_MULTIMODAL_MAX_IMAGE_SIDE = 2048;
    /** Titan Multimodal Embeddings G1: "Max input text tokens – 256" (older AWS material said 128). */
    private static final int TITAN_MULTIMODAL_MAX_TEXT_TOKENS = 256;
    /** WebP, which Cohere Embed v4 accepts; the compute flow prepares JPEG or PNG only. */
    private static final String WEBP = "image/webp";

    private static Set<String> mimeTypes(String... types) {
      return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(types)));
    }
  }
}
