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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Static configuration of a Bedrock embedding client, per model alias (prefix {@code hoodie.unstructured.embedding.model.<alias>}):
 * <ul>
 *   <li>{@code .name}: the model id sent as InvokeModel's {@code modelId}: a foundation model id, an inference profile id or an
 *       ARN. When it names a family ({@link BedrockModelFamily#fromModelId}) that family must be the client's;</li>
 *   <li>{@value #REGION_SUFFIX}: the AWS region; when unset the SDK's default region chain decides ({@code AWS_REGION},
 *       the {@code aws.region} system property, the profile, then instance metadata);</li>
 *   <li>{@value #ENDPOINT_SUFFIX}: an endpoint override, e.g. a VPC interface endpoint; default the region's
 *       {@code bedrock-runtime} endpoint;</li>
 *   <li>the request, retry and outage keys of {@link ProviderClientSettings}; {@code .request.max.inputs} bounds the texts of one
 *       Cohere request (default and maximum 96).</li>
 * </ul>
 * There is no credential key: requests are signed with the credentials of the AWS SDK default chain, which on a Kubernetes
 * executor with an IAM role for its service account is the pod's web identity (IRSA). The per-model request rate limit
 * ({@code .rate.limit.requests.per.second}, {@code .rate.limit.burst}) is shared by every client: see
 * {@link org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter}. The model's {@code .dimensions} must be one
 * the family accepts; for a model that declares {@code image}, the compute image long side must not exceed the family's
 * per-side limit ({@link BedrockModelFamily#maxImageSide()}).
 */
@ConfigClassProperty(name = "Bedrock Embedding Client Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Per-model AWS region and endpoint settings for Bedrock embedding requests.")
public final class BedrockClientConfig implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final String REGION_SUFFIX = ".bedrock.region";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.bedrock.region family. */
  public static final ConfigProperty<String> REGION_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.bedrock.region")
      .noDefaultValue("<unset>")
      .sinceVersion("1.3.0")
      .withDocumentation("Region. Replace <alias> with the declared model alias. effective AWS SDK region chain. Default: <unset>.");
  public static final String ENDPOINT_SUFFIX = ".endpoint";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.endpoint family. */
  public static final ConfigProperty<String> ENDPOINT_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.endpoint")
      .noDefaultValue("<unset>")
      .sinceVersion("1.3.0")
      .withDocumentation("Endpoint. Replace <alias> with the declared model alias. effective AWS SDK regional endpoint. Default: "
          + "<unset>.");

  private final String alias;
  private final BedrockModelFamily family;
  private final String modelId;
  private final String region;
  private final String endpoint;
  private final ProviderClientSettings settings;

  private BedrockClientConfig(EmbeddingModelSpec spec, TypedProperties props, BedrockModelFamily family) {
    this.alias = spec.getName();
    this.family = family;
    this.modelId = spec.getModelName().trim();
    Option<BedrockModelFamily> named = BedrockModelFamily.fromModelId(modelId);
    if (named.isPresent() && named.get() != family) {
      throw new IllegalArgumentException(key(EmbeddingTableConfig.MODEL_NAME_SUFFIX) + "=" + modelId + " is a "
          + named.get() + " model, but the client serves " + family);
    }
    if (!family.dimensions().contains(spec.getDimension())) {
      throw new IllegalArgumentException(key(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX) + "=" + spec.getDimension()
          + " is not an output dimension " + family + " supports: " + family.dimensions());
    }
    if (spec.declares(EmbeddingTableSchema.MODALITY_IMAGE) && family.maxImageSide() > 0) {
      int longSide = EmbeddingTableConfig.from(props).imagePreprocessing().longSide();
      if (longSide > family.maxImageSide()) {
        throw new IllegalArgumentException(EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key() + "=" + longSide + " resizes images "
            + "beyond the " + family.maxImageSide() + " pixels per side " + family + " accepts, and model " + alias + " declares "
            + EmbeddingTableSchema.MODALITY_IMAGE + " (" + key(EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX) + "); set it to at most "
            + family.maxImageSide());
      }
    }
    if (spec.declares(EmbeddingTableSchema.MODALITY_TEXT) && family.maxTextTokens() > 0) {
      int chunkTokens = EmbeddingTableConfig.from(props).chunkMaxTokens();
      if (chunkTokens > family.maxTextTokens()) {
        throw new IllegalArgumentException(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key() + "=" + chunkTokens + " cuts text chunks "
            + "beyond the " + family.maxTextTokens() + " text tokens " + family + " accepts, and model " + alias + " declares "
            + EmbeddingTableSchema.MODALITY_TEXT + " (" + key(EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX) + "); set it to at most "
            + family.maxTextTokens());
      }
    }
    this.region = trimmed(props, REGION_SUFFIX);
    this.endpoint = trimmed(props, ENDPOINT_SUFFIX);
    if (endpoint != null) {
      checkEndpoint();
    }
    this.settings = ProviderClientSettings.from(spec, props, family.maxInputsPerRequest(), family.maxInputsPerRequest());
  }

  /**
   * @throws IllegalArgumentException naming the offending key: a model id of another family, a dimension the family does not
   *     accept, an image long side beyond the family's limit for a model that declares {@code image}, an endpoint that is not
   *     an absolute http(s) URL without user info, query or fragment, or a bad request key
   */
  public static BedrockClientConfig from(EmbeddingModelSpec spec, TypedProperties props, BedrockModelFamily family) {
    return new BedrockClientConfig(spec, props, family);
  }

  private void checkEndpoint() {
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(key(ENDPOINT_SUFFIX) + " is not a valid URL (" + e.getReason() + ")");
    }
    boolean http = "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
    if (!http || uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      throw new IllegalArgumentException(key(ENDPOINT_SUFFIX)
          + " must be an absolute http(s) URL without user info, a query or a fragment");
    }
  }

  private String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + alias + suffix;
  }

  private String trimmed(TypedProperties props, String suffix) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key(suffix)).noDefaultValue(), null);
    return StringUtils.isNullOrEmpty(value) || value.trim().isEmpty() ? null : value.trim();
  }

  public String alias() {
    return alias;
  }

  public BedrockModelFamily family() {
    return family;
  }

  /** InvokeModel's {@code modelId}. */
  public String modelId() {
    return modelId;
  }

  /** The configured region, or null for the SDK's default region chain. */
  public String region() {
    return region;
  }

  /** The endpoint override, or null for the region's endpoint. */
  public String endpoint() {
    return endpoint;
  }

  public ProviderClientSettings settings() {
    return settings;
  }

  /** The key naming the region, for messages. */
  public String regionKey() {
    return key(REGION_SUFFIX);
  }

  @Override
  public String toString() {
    return "BedrockClientConfig{alias=" + alias + ", family=" + family + ", model=" + modelId + ", region="
        + (region == null ? "(default chain)" : region) + ", endpoint=" + (endpoint == null ? "(regional)" : endpoint) + ", "
        + settings + ", credentials=(default chain)}";
  }
}
