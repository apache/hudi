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

package org.apache.hudi.utilities.unstructured.embedding.compute.vertex;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.CredentialSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Static configuration of {@link VertexMultimodalEmbeddingClient}, per model alias (prefix
 * {@code hoodie.unstructured.embedding.model.<alias>}):
 * <ul>
 *   <li>{@value #ENDPOINT_SUFFIX}: the full {@code :predict} URL; when unset it is built from
 *       {@value #PROJECT_SUFFIX} (required then) and {@value #LOCATION_SUFFIX} (default {@value #DEFAULT_LOCATION}) as
 *       {@code https://<location>-aiplatform.googleapis.com/v1/projects/<project>/locations/<location>/publishers/google/models/<model name>:predict};</li>
 *   <li>{@value #CREDENTIAL_SOURCE_SUFFIX}: where the OAuth access token comes from ({@link VertexAccessTokens}):
 *       {@value #CREDENTIAL_SOURCE_TOKEN} (default), an access token read from {@value #CREDENTIAL_FILE_SUFFIX}, else the
 *       environment variable {@value #CREDENTIAL_ENV_SUFFIX} (default {@value #DEFAULT_CREDENTIAL_ENV}), read again when the
 *       endpoint answers HTTP 401; or {@value #CREDENTIAL_SOURCE_GOOGLE}, a Google credential that refreshes its own tokens:
 *       the service-account key file at {@value #CREDENTIAL_FILE_SUFFIX} when set, else as Application Default Credentials
 *       find it (GOOGLE_APPLICATION_CREDENTIALS, else the metadata server: GKE workload identity, a Compute Engine service
 *       account); {@value #CREDENTIAL_ENV_SUFFIX} does not apply to it. Never a Hudi property.
 *       Credential sources do not change the configured model;</li>
 *   <li>the request, retry and outage keys of {@link ProviderClientSettings} ({@code .request.timeout.ms},
 *       {@code .request.deadline.ms}, {@code .retry.*}, {@code .outage.consecutive.failures}), as for every provider; one input per
 *       request, so {@code .request.max.inputs} is at most 1.</li>
 * </ul>
 * The per-model request rate limit ({@code .rate.limit.requests.per.second}, {@code .rate.limit.burst}) is shared by every HTTP
 * client: see {@link org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter}.
 * The model's {@code .dimensions} must be one the API accepts: 128, 256, 512 or 1408
 * (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/multimodal-embeddings-api).
 */
@ConfigClassProperty(name = "Vertex Embedding Client Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Per-model endpoint, project, location and credentials for Vertex embedding requests.")
public final class VertexClientConfig {

  private static final String SINCE_VERSION = "1.3.0";

  public static final String ENDPOINT_SUFFIX = ".endpoint";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.endpoint family. */
  public static final ConfigProperty<String> ENDPOINT_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.endpoint")
      .noDefaultValue("<derived from project/location/model>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Endpoint. Replace <alias> with the declared model alias. otherwise explicit URL; project required when URL "
          + "absent. Default: <derived from project/location/model>.");
  public static final String PROJECT_SUFFIX = ".vertex.project";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.vertex.project family. */
  public static final ConfigProperty<String> PROJECT_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.vertex.project")
      .noDefaultValue("<unset>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Project. Replace <alias> with the declared model alias. required when endpoint absent. Default: <unset>.");
  public static final String LOCATION_SUFFIX = ".vertex.location";

  public static final String DEFAULT_LOCATION = "us-central1";
  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.vertex.location family. */
  public static final ConfigProperty<String> LOCATION_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.vertex.location")
      .defaultValue(DEFAULT_LOCATION)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Location. Replace <alias> with the declared model alias. used only when endpoint derived.");
  public static final String CREDENTIAL_FILE_SUFFIX = ".credential.file";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.file family. */
  public static final ConfigProperty<String> CREDENTIAL_FILE_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.file")
      .noDefaultValue("<unset>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Credential file. Replace <alias> with the declared model alias. Default: <unset>.");
  public static final String CREDENTIAL_ENV_SUFFIX = ".credential.env";

  public static final String DEFAULT_CREDENTIAL_ENV = "GOOGLE_OAUTH_ACCESS_TOKEN";
  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.env family. */
  public static final ConfigProperty<String> CREDENTIAL_ENV_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.env")
      .defaultValue(DEFAULT_CREDENTIAL_ENV)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Credential env. Replace <alias> with the declared model alias. token source only; forbidden for google "
          + "source.");
  public static final String CREDENTIAL_SOURCE_SUFFIX = ".credential.source";

  public static final String CREDENTIAL_SOURCE_TOKEN = "token";
  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.source family. */
  public static final ConfigProperty<String> CREDENTIAL_SOURCE_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.source")
      .defaultValue(CREDENTIAL_SOURCE_TOKEN)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Credential source. Replace <alias> with the declared model alias.");
  public static final String CREDENTIAL_SOURCE_GOOGLE = "google";
  public static final List<String> CREDENTIAL_SOURCES =
      Collections.unmodifiableList(Arrays.asList(CREDENTIAL_SOURCE_TOKEN, CREDENTIAL_SOURCE_GOOGLE));
  /** {@code parameters.dimension} values multimodalembedding@001 accepts. */
  public static final List<Integer> ALLOWED_DIMENSIONS = Collections.unmodifiableList(Arrays.asList(128, 256, 512, 1408));
  private static final int MAX_TEXT_TOKENS = 32;

  private final String endpoint;
  private final String credentialSource;
  private final String credentialFile;
  private final String credentialEnv;
  private final ProviderClientSettings settings;

  private VertexClientConfig(String endpoint, String credentialSource, String credentialFile, String credentialEnv,
                             ProviderClientSettings settings) {
    this.endpoint = endpoint;
    this.credentialSource = credentialSource;
    this.credentialFile = credentialFile;
    this.credentialEnv = credentialEnv;
    this.settings = settings;
  }

  /**
   * @throws IllegalArgumentException naming the offending key: a dimension the API does not accept, neither an endpoint nor
   *     a project, an endpoint that is not an absolute http(s) URL with a host or carries user info, a query or a
   *     fragment, a bad request or retry setting, an unknown credential source,
   *     {@value #CREDENTIAL_ENV_SUFFIX} set with {@value #CREDENTIAL_SOURCE_GOOGLE} or not an environment variable name
   */
  public static VertexClientConfig from(EmbeddingModelSpec spec, TypedProperties props) {
    String prefix = EmbeddingTableConfig.MODEL_PREFIX + spec.getName();
    if (!ALLOWED_DIMENSIONS.contains(spec.getDimension())) {
      throw new IllegalArgumentException("Config " + prefix + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX + " = "
          + spec.getDimension() + " is not a dimension the Vertex multimodal embedding API accepts: " + ALLOWED_DIMENSIONS);
    }
    String endpoint = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + ENDPOINT_SUFFIX).noDefaultValue(), null);
    if (StringUtils.isNullOrEmpty(endpoint)) {
      String project = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + PROJECT_SUFFIX).noDefaultValue(), null);
      if (StringUtils.isNullOrEmpty(project)) {
        throw new IllegalArgumentException("Config " + prefix + ENDPOINT_SUFFIX + " or " + prefix + PROJECT_SUFFIX
            + " must be set for the Vertex multimodal embedding client");
      }
      String location = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + LOCATION_SUFFIX).noDefaultValue(), DEFAULT_LOCATION).trim();
      endpoint = "https://" + location + "-aiplatform.googleapis.com/v1/projects/" + project.trim() + "/locations/"
          + location + "/publishers/google/models/" + spec.getModelName() + ":predict";
    }
    checkEndpoint(prefix, endpoint.trim());
    if (spec.declares(EmbeddingTableSchema.MODALITY_TEXT)
        && EmbeddingTableConfig.from(props).chunkMaxTokens() > MAX_TEXT_TOKENS) {
      throw new IllegalArgumentException("Vertex text inputs require " + EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()
          + " <= " + MAX_TEXT_TOKENS + "; the endpoint truncates longer text");
    }
    String modelPath = "/publishers/google/models/" + spec.getModelName() + ":predict";
    if (spec.getModelName().contains("/") || !URI.create(endpoint.trim()).getPath().endsWith(modelPath)) {
      throw new IllegalArgumentException("Config " + prefix + ENDPOINT_SUFFIX + " must name the configured model "
          + spec.getModelName() + " in a /publishers/google/models/<model>:predict path");
    }
    // one instance per :predict request (VertexMultimodalEmbeddingClient)
    ProviderClientSettings settings = ProviderClientSettings.from(spec, props, 1, 1);
    String source = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + CREDENTIAL_SOURCE_SUFFIX).noDefaultValue(), CREDENTIAL_SOURCE_TOKEN).trim();
    if (!CREDENTIAL_SOURCES.contains(source)) {
      throw new IllegalArgumentException("Config " + prefix + CREDENTIAL_SOURCE_SUFFIX + " = " + source + " is not one of "
          + CREDENTIAL_SOURCES);
    }
    String file = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + CREDENTIAL_FILE_SUFFIX).noDefaultValue(), null);
    String env = null;
    if (CREDENTIAL_SOURCE_GOOGLE.equals(source)) {
      if (props.containsKey(prefix + CREDENTIAL_ENV_SUFFIX)) {
        throw new IllegalArgumentException("Config " + prefix + CREDENTIAL_ENV_SUFFIX + " does not apply to "
            + prefix + CREDENTIAL_SOURCE_SUFFIX + " = " + CREDENTIAL_SOURCE_GOOGLE + ": set " + prefix + CREDENTIAL_FILE_SUFFIX
            + " to a service-account key file, or neither for Application Default Credentials");
      }
    } else {
      env = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(prefix + CREDENTIAL_ENV_SUFFIX).noDefaultValue(), DEFAULT_CREDENTIAL_ENV).trim();
      CredentialSource.checkEnvName(prefix + CREDENTIAL_ENV_SUFFIX, env);
    }
    return new VertexClientConfig(endpoint.trim(), source, StringUtils.isNullOrEmpty(file) ? null : file.trim(), env, settings);
  }

  /**
   * Refuses, as the OpenAI and Jina configs do, an endpoint that is not an absolute http(s) URL with a host, and one carrying
   * user info, a query or a fragment (Google APIs take an API key as {@code ?key=}) before it can be printed in a message. A
   * message names the key and which rule failed, never any part of the endpoint, which may hold a credential.
   */
  private static void checkEndpoint(String prefix, String endpoint) {
    String key = prefix + ENDPOINT_SUFFIX;
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Config " + key + " is not a valid URL (" + e.getReason() + ")");
    }
    if (uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
      throw new IllegalArgumentException("Config " + key + " must not carry user info, a query or a fragment; pass the credential "
          + "through " + prefix + CREDENTIAL_SOURCE_SUFFIX);
    }
    String scheme = uri.getScheme();
    if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Config " + key + " must be an absolute http(s) URL, got "
          + (scheme == null ? "no scheme" : "a scheme other than http or https"));
    }
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("Config " + key + " must be an absolute http(s) URL, got no host");
    }
  }

  public String endpoint() {
    return endpoint;
  }

  /** {@value #CREDENTIAL_SOURCE_TOKEN} or {@value #CREDENTIAL_SOURCE_GOOGLE}. */
  public String credentialSource() {
    return credentialSource;
  }

  /**
   * {@value #CREDENTIAL_SOURCE_TOKEN}: the token file, or null to read {@link #credentialEnv()};
   * {@value #CREDENTIAL_SOURCE_GOOGLE}: the service-account key file, or null for Application Default Credentials.
   */
  public String credentialFile() {
    return credentialFile;
  }

  /** The token's environment variable for {@value #CREDENTIAL_SOURCE_TOKEN}; null for {@value #CREDENTIAL_SOURCE_GOOGLE}. */
  public String credentialEnv() {
    return credentialEnv;
  }

  public int requestTimeoutMs() {
    return settings.requestTimeoutMs();
  }

  /** The request, retry and outage settings of {@link org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop}. */
  public ProviderClientSettings settings() {
    return settings;
  }

  /** Where the token comes from, for messages: never the token. */
  public String describeCredential() {
    if (CREDENTIAL_SOURCE_GOOGLE.equals(credentialSource)) {
      return credentialFile != null ? "google file " + credentialFile : "google application default credentials";
    }
    return credentialFile != null ? "file " + credentialFile : "env " + credentialEnv;
  }
}
