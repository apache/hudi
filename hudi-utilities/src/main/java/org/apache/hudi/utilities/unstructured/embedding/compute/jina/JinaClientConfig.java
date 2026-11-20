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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.CredentialSource;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Static configuration of a Jina embedding client, per model alias (prefix {@code hoodie.unstructured.embedding.model.<alias>}):
 * <ul>
 *   <li>{@code .name}: the Jina model id sent as {@code model} (e.g. {@code jina-embeddings-v3}, {@code jina-clip-v2});</li>
 *   <li>{@value #ENDPOINT_SUFFIX}: the API base URL, requests go to {@code <endpoint>/embeddings}; default
 *       {@value #DEFAULT_ENDPOINT};</li>
 *   <li>{@value #CREDENTIAL_FILE_SUFFIX} / {@value #CREDENTIAL_ENV_SUFFIX}: where the API key is read, a file (preferred) else
 *       the environment variable NAME, default {@value #DEFAULT_CREDENTIAL_ENV}; never a Hudi property. The key is sent as
 *       {@code Authorization: Bearer};</li>
 *   <li>the request, retry and outage keys of {@link ProviderClientSettings}; {@code .request.max.inputs} bounds the texts of
 *       one request (default {@value #DEFAULT_MAX_INPUTS}, at most {@value #MAX_INPUTS}).</li>
 * </ul>
 * The model's {@code .dimensions} is sent as {@code dimensions} for a model that takes it. For the models Jina's OpenAPI document
 * (2026.10.13.1624) describes it is checked here: jina-embeddings-v2-* and jina-clip-v1 take no {@code dimensions} and return
 * their native 768 (Jina's model cards); jina-clip-v2, jina-embeddings-v3 and jina-embeddings-v5-* take up to 1024, jina-embeddings-v4 up to 2048,
 * jina-code-embeddings-0.5b up to 896 and jina-code-embeddings-1.5b up to 1536. The ColBERT models answer one vector per
 * token, not one per input, and are refused. The per-model request rate limit is shared by every client: see
 * {@link org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter}.
 */
@ConfigClassProperty(name = "Jina Embedding Client Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Per-model endpoint and credential settings for Jina embedding requests.")
public final class JinaClientConfig implements Serializable {

  private static final String SINCE_VERSION = "1.3.0";

  private static final long serialVersionUID = 1L;
  public static final String ENDPOINT_SUFFIX = ".endpoint";

  public static final String DEFAULT_ENDPOINT = "https://api.jina.ai/v1";
  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.endpoint family. */
  public static final ConfigProperty<String> ENDPOINT_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.endpoint")
      .defaultValue(DEFAULT_ENDPOINT)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Endpoint. Replace <alias> with the declared model alias.");
  public static final String CREDENTIAL_FILE_SUFFIX = ".credential.file";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.file family. */
  public static final ConfigProperty<String> CREDENTIAL_FILE_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.file")
      .noDefaultValue("<unset>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Credential file. Replace <alias> with the declared model alias. Default: <unset>.");
  public static final String CREDENTIAL_ENV_SUFFIX = ".credential.env";

  public static final String DEFAULT_CREDENTIAL_ENV = "JINA_API_KEY";
  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.env family. */
  public static final ConfigProperty<String> CREDENTIAL_ENV_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.env")
      .defaultValue(DEFAULT_CREDENTIAL_ENV)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Credential env. Replace <alias> with the declared model alias.");
  public static final int DEFAULT_MAX_INPUTS = 64;
  public static final int MAX_INPUTS = 2048;
  /** Documented models: the most dimensions, and whether {@code dimensions} may be sent (else the model returns exactly that). */
  private static final Map<String, int[]> KNOWN_MODELS = new HashMap<>();

  /** Model id prefix of the ColBERT (multi-vector) models, which this client cannot store. */
  private static final String COLBERT_PREFIX = "jina-colbert-";

  static {
    for (String v2 : new String[] {"en", "zh", "de", "es", "code"}) {
      KNOWN_MODELS.put("jina-embeddings-v2-base-" + v2, new int[] {768, 0});
    }
    KNOWN_MODELS.put("jina-embeddings-v3", new int[] {1024, 1});
    for (String v5 : new String[] {"text-nano", "text-small"}) {
      KNOWN_MODELS.put("jina-embeddings-v5-" + v5, new int[] {1024, 1});
    }
    KNOWN_MODELS.put("jina-code-embeddings-0.5b", new int[] {896, 1});
    KNOWN_MODELS.put("jina-code-embeddings-1.5b", new int[] {1536, 1});
  }

  private final String alias;
  private final String modelName;
  private final String endpoint;
  private final Integer dimensionsParam;
  private final String credentialFile;
  private final String credentialEnv;
  private final ProviderClientSettings settings;

  private JinaClientConfig(EmbeddingModelSpec spec, TypedProperties props) {
    this.alias = spec.getName();
    this.modelName = spec.getModelName().trim();
    String configured = trimmed(props, ENDPOINT_SUFFIX);
    String url = configured == null ? DEFAULT_ENDPOINT : configured;
    while (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }
    this.endpoint = url;
    checkEndpoint();
    if (modelName.startsWith(COLBERT_PREFIX)) {
      throw new IllegalArgumentException(key(EmbeddingTableConfig.MODEL_NAME_SUFFIX) + "=" + modelName
          + " is a multi-vector (ColBERT) model; an embedding column holds one vector per input");
    }
    int[] known = KNOWN_MODELS.get(modelName);
    int dimension = spec.getDimension();
    if (known != null && (dimension > known[0] || (known[1] == 0 && dimension != known[0]))) {
      throw new IllegalArgumentException(key(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX) + "=" + dimension + " but "
          + modelName + " returns " + (known[1] == 0 ? "exactly " : "at most ") + known[0] + " dimensions");
    }
    this.dimensionsParam = known != null && known[1] == 0 ? null : dimension;
    this.credentialFile = trimmed(props, CREDENTIAL_FILE_SUFFIX);
    String env = trimmed(props, CREDENTIAL_ENV_SUFFIX);
    this.credentialEnv = env == null ? DEFAULT_CREDENTIAL_ENV : env;
    CredentialSource.checkEnvName(key(CREDENTIAL_ENV_SUFFIX), credentialEnv);
    this.settings = ProviderClientSettings.from(spec, props, DEFAULT_MAX_INPUTS, MAX_INPUTS);
  }

  /**
   * @throws IllegalArgumentException naming the offending key: an endpoint that is not an absolute http(s) URL without user
   *     info, query or fragment, a ColBERT model, a dimension the documented model cannot return, a credential env that is not a variable name,
   *     or a bad request key
   */
  public static JinaClientConfig from(EmbeddingModelSpec spec, TypedProperties props) {
    return new JinaClientConfig(spec, props);
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
      throw new IllegalArgumentException(key(ENDPOINT_SUFFIX) + " must be an absolute http(s) URL without user info, a query "
          + "or a fragment; pass the key through " + key(CREDENTIAL_FILE_SUFFIX) + " or " + key(CREDENTIAL_ENV_SUFFIX));
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

  public String modelName() {
    return modelName;
  }

  /** Base URL without a trailing slash. */
  public String endpoint() {
    return endpoint;
  }

  /** The {@code dimensions} request field, or null to send none. */
  public Integer dimensionsParam() {
    return dimensionsParam;
  }

  /** Path of the key file, or null to read {@link #credentialEnv()}. */
  public String credentialFile() {
    return credentialFile;
  }

  public String credentialEnv() {
    return credentialEnv;
  }

  public ProviderClientSettings settings() {
    return settings;
  }

  /** Where the key comes from, for messages: never the key. */
  public String describeCredential() {
    return credentialFile != null ? "file " + credentialFile : "env " + credentialEnv;
  }

  @Override
  public String toString() {
    return "JinaClientConfig{alias=" + alias + ", model=" + modelName + ", endpoint=" + endpoint + ", dimensions="
        + dimensionsParam + ", " + settings + ", credential=" + describeCredential() + "}";
  }
}
