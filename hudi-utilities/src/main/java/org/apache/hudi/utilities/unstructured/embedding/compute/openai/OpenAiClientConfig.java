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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.config.NumericConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiKnownModels.KnownModel;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The validated configuration of one model alias served by the OpenAI-compatible embedding client, read from
 * {@code hoodie.unstructured.embedding.model.<alias>.*}. No key here carries a secret: the credential is named by a
 * file path or an environment variable NAME ({@link CredentialSource}). The per-model request rate limit
 * ({@code .rate.limit.requests.per.second}, {@code .rate.limit.burst}) is not provider-specific: see
 * {@link org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter}.
 */
@ConfigClassProperty(name = "OpenAI Compatible Embedding Client Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Per-model endpoint, credentials and request limits for OpenAI-compatible embedding providers.")
public final class OpenAiClientConfig implements Serializable {

  private static final String SINCE_VERSION = "1.3.0";
  private static final String PROVIDER_DEPENDENT = "<provider-dependent>";

  private static final long serialVersionUID = 1L;

  /** Base URL; requests go to {@code <endpoint>/embeddings}. Default: OpenAI's; required for azure-openai and hf-tei. */
  public static final String ENDPOINT_SUFFIX = ".endpoint";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.endpoint family. */
  public static final ConfigProperty<String> ENDPOINT_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.endpoint")
      .noDefaultValue(PROVIDER_DEPENDENT)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Base URL; requests go to <endpoint>/embeddings. Default: OpenAI's; required for azure-openai and hf-tei. "
          + "Replace <alias> with the declared model alias. openai https://api.openai.com/v1; azure-openai/hf-tei "
          + "required. Default: <provider-dependent>.");
  /** The documented OpenAI model whose limits apply (an Azure deployment's model); default the model name. */
  public static final String BASE_MODEL_SUFFIX = ".base.model";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.base.model family. */
  public static final ConfigProperty<String> BASE_MODEL_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.base.model")
      .noDefaultValue("<model name>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("The documented OpenAI model whose limits apply (an Azure deployment's model); default the model name. "
          + "Replace <alias> with the declared model alias. alias when model.name absent. Default: <model name>.");
  /** Path of a file whose trimmed content is the API key; wins over {@link #CREDENTIAL_ENV_SUFFIX}. */
  public static final String CREDENTIAL_FILE_SUFFIX = ".credential.file";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.file family. */
  public static final ConfigProperty<String> CREDENTIAL_FILE_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.file")
      .noDefaultValue("<unset>")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Path of a file whose trimmed content is the API key; wins over "
          + "hoodie.unstructured.embedding.model.<alias>.credential.env. Replace <alias> with the declared model alias. "
          + "Default: <unset>.");
  /**
   * NAME of the env var holding the API key; default OPENAI_API_KEY / AZURE_OPENAI_API_KEY / none (hf-tei). Deliver it
   * as a secret reference ({@code spark.kubernetes.executor.secretKeyRef.<ENV>=<secret>:<key>}), never as
   * {@code spark.executorEnv.<ENV>=<value>}: Spark's default {@code spark.redaction.regex} does not match
   * {@code OPENAI_API_KEY}, so such a value shows in the Spark UI environment tab and the event log (extend the regex
   * with {@code |api[_.-]?key} if executorEnv cannot be avoided). A key file ({@link #CREDENTIAL_FILE_SUFFIX}) is preferred.
   */
  public static final String CREDENTIAL_ENV_SUFFIX = ".credential.env";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.credential.env family. */
  public static final ConfigProperty<String> CREDENTIAL_ENV_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.credential.env")
      .noDefaultValue(PROVIDER_DEPENDENT)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("NAME of the env var holding the API key; default OPENAI_API_KEY / AZURE_OPENAI_API_KEY / none (hf-tei). "
          + "Deliver it as a secret reference (spark.kubernetes.executor.secretKeyRef.<ENV>=<secret>:<key>), never as "
          + "spark.executorEnv.<ENV>=<value>: Spark's default spark.redaction.regex does not match OPENAI_API_KEY, so "
          + "such a value shows in the Spark UI environment tab and the event log (extend the regex with |api[_.-]?key "
          + "if executorEnv cannot be avoided). A key file (hoodie.unstructured.embedding.model.<alias>.credential.file) "
          + "is preferred. Replace <alias> with the declared model alias. OPENAI_API_KEY / AZURE_OPENAI_API_KEY / unset "
          + "hf-tei. Default: <provider-dependent>.");
  /** Most inputs per request; default (and maximum) the provider's: 2048, or TEI's max_client_batch_size. */
  public static final String REQUEST_MAX_INPUTS_SUFFIX = ".request.max.inputs";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.max.inputs family. */
  public static final ConfigProperty<String> REQUEST_MAX_INPUTS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.max.inputs")
      .noDefaultValue(PROVIDER_DEPENDENT)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Most inputs per request; default (and maximum) the provider's: 2048, or TEI's max_client_batch_size. "
          + "Replace <alias> with the declared model alias. openai/azure 2048; hf-tei -1 then GET /info; shared key "
          + "also Jina 64, Vertex 1, Bedrock Titan 1/Cohere 96. Default: <provider-dependent>.");
  /** Most summed tokens per request; default (and maximum) the provider's: 300000, or TEI's max_batch_tokens. */
  public static final String REQUEST_MAX_TOKENS_SUFFIX = ".request.max.tokens";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.max.tokens family. */
  public static final ConfigProperty<String> REQUEST_MAX_TOKENS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.max.tokens")
      .noDefaultValue(PROVIDER_DEPENDENT)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Most summed tokens per request; default (and maximum) the provider's: 300000, or TEI's max_batch_tokens. "
          + "Replace <alias> with the declared model alias. openai/azure 300000; hf-tei -1 then GET /info. Default: "
          + "<provider-dependent>.");
  /**
   * Per-input token limit checked locally; default the known model's 8191, which it may only lower; required otherwise.
   * An input above it is PERMANENT: it is a compute error-table event and the writer's incremental rounds do not select the row
   * again for that column, so setting it too low strands those rows in the table without a vector; raising it later re-selects
   * them only through a reset of the writer's cursor or a snapshot read.
   * Embedding them needs a new model column or a new embedding table.
   */
  public static final String REQUEST_MAX_INPUT_TOKENS_SUFFIX = ".request.max.input.tokens";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.max.input.tokens family. */
  public static final ConfigProperty<String> REQUEST_MAX_INPUT_TOKENS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.max.input.tokens")
      .noDefaultValue("<model-dependent>")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Per-input token limit checked locally; default the known model's 8191, which it may only lower; required "
          + "otherwise. An input above it is PERMANENT: it is a compute error-table event and the writer's incremental "
          + "rounds do not select the row again for that column, so setting it too low strands those rows in the table "
          + "without a vector; raising it later re-selects them only through a reset of the writer's cursor or a "
          + "snapshot read. Embedding them needs a new model column or a new embedding table. Replace <alias> with the "
          + "declared model alias. known OpenAI 8191; unknown required; hf-tei ignores (0). Default: <model-dependent>.");
  /** HTTP timeout of one request in ms; default 30000. */
  public static final String REQUEST_TIMEOUT_MS_SUFFIX = ".request.timeout.ms";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.timeout.ms family. */
  public static final ConfigProperty<Integer> REQUEST_TIMEOUT_MS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.timeout.ms")
      .defaultValue(30000)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("HTTP timeout of one request in ms; default 30000. Replace <alias> with the declared model alias.");
  /** Budget for starting requests in one embed() call in ms, retries and bisection included; default 300000. */
  public static final String REQUEST_DEADLINE_MS_SUFFIX = ".request.deadline.ms";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.deadline.ms family. */
  public static final ConfigProperty<Integer> REQUEST_DEADLINE_MS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.deadline.ms")
      .defaultValue(300000)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Budget for starting requests in one embed() call in ms, retries and bisection included; default 300000. "
          + "A request already in flight keeps its configured timeout. Replace <alias> with the declared model alias.");
  /** Most extra requests one embed() call spends isolating rejected inputs; default 32. */
  public static final String BISECT_MAX_REQUESTS_SUFFIX = ".request.bisect.max.requests";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.request.bisect.max.requests family. */
  public static final ConfigProperty<Integer> BISECT_MAX_REQUESTS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.request.bisect.max.requests")
      .defaultValue(32)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Most extra requests one embed() call spends isolating rejected inputs; default 32. Replace <alias> with "
          + "the declared model alias.");
  /** Attempts per request, the first included, before its inputs are RETRYABLE; default 5. */
  public static final String RETRY_MAX_ATTEMPTS_SUFFIX = ".retry.max.attempts";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.retry.max.attempts family. */
  public static final ConfigProperty<Integer> RETRY_MAX_ATTEMPTS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.retry.max.attempts")
      .defaultValue(5)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Attempts per request, the first included, before its inputs are RETRYABLE; default 5. Replace <alias> with "
          + "the declared model alias.");
  /** Upper bound of the first full-jitter backoff in ms; default 500. */
  public static final String RETRY_INITIAL_BACKOFF_MS_SUFFIX = ".retry.initial.backoff.ms";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.retry.initial.backoff.ms family. */
  public static final ConfigProperty<Integer> RETRY_INITIAL_BACKOFF_MS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.retry.initial.backoff.ms")
      .defaultValue(500)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Upper bound of the first full-jitter backoff in ms; default 500. Replace <alias> with the declared model "
          + "alias.");
  /** Cap of any backoff in ms; a provider hint above it ends the retries of the request; default 60000. */
  public static final String RETRY_MAX_BACKOFF_MS_SUFFIX = ".retry.max.backoff.ms";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.retry.max.backoff.ms family. */
  public static final ConfigProperty<Integer> RETRY_MAX_BACKOFF_MS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.retry.max.backoff.ms")
      .defaultValue(60000)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Cap of any backoff in ms; a provider hint above it ends the retries of the request; default 60000. Replace "
          + "<alias> with the declared model alias.");
  /** Consecutive requests left RETRYABLE after their retries before the client stops calling; default 3. */
  public static final String OUTAGE_CONSECUTIVE_FAILURES_SUFFIX = ".outage.consecutive.failures";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.outage.consecutive.failures family. */
  public static final ConfigProperty<Integer> OUTAGE_CONSECUTIVE_FAILURES_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.outage.consecutive.failures")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Consecutive requests left RETRYABLE after their retries before the client stops calling; default 3. "
          + "Replace <alias> with the declared model alias.");

  static final int DEFAULT_BISECT_MAX_REQUESTS = 32;

  private final String alias;
  private final OpenAiProvider provider;
  private final String endpoint;
  private final String modelName;
  private final String baseModel;
  private final KnownModel knownModel;
  private final Integer dimensionsParam;
  private final int maxInputs;
  private final long maxTokens;
  private final int maxInputTokens;
  private final String encoding;
  /** The request, retry and outage settings every provider client reads alike; inputs and tokens per request are bounded here. */
  private final ProviderClientSettings settings;
  private final int bisectMaxRequests;
  private final String credentialFile;
  private final String credentialEnv;

  private OpenAiClientConfig(EmbeddingModelSpec spec, TypedProperties props) {
    this.alias = spec.getName();
    this.provider = OpenAiProvider.fromId(spec.getProvider()).orElseThrow(() -> new IllegalArgumentException(
        key(EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX) + "=" + spec.getProvider() + " is not an OpenAI-compatible provider (openai, azure-openai, hf-tei)"));
    String configuredEndpoint = string(props, ENDPOINT_SUFFIX, provider.defaultEndpoint());
    if (configuredEndpoint == null) {
      throw new IllegalArgumentException(key(ENDPOINT_SUFFIX) + " is required for provider " + provider.id());
    }
    this.endpoint = stripTrailingSlashes(configuredEndpoint);
    checkEndpoint();
    this.modelName = spec.getModelName();
    this.baseModel = string(props, BASE_MODEL_SUFFIX, modelName);
    boolean openAiModels = provider != OpenAiProvider.HF_TEI;
    this.knownModel = openAiModels ? OpenAiKnownModels.lookup(baseModel).orElse(null) : null;
    this.dimensionsParam = dimensionsParam(spec.getDimension());
    this.maxInputs = providerBounded(props, REQUEST_MAX_INPUTS_SUFFIX, provider.maxInputsPerRequest());
    this.maxTokens = providerBounded(props, REQUEST_MAX_TOKENS_SUFFIX, provider.maxTokensPerRequest());
    this.maxInputTokens = openAiModels ? maxInputTokens(props) : 0;
    this.encoding = openAiModels ? OpenAiKnownModels.ENCODING : null;
    this.settings = ProviderClientSettings.withoutRequestBound(spec, props);
    this.bisectMaxRequests = positiveInt(props, BISECT_MAX_REQUESTS_SUFFIX, DEFAULT_BISECT_MAX_REQUESTS);
    this.credentialFile = string(props, CREDENTIAL_FILE_SUFFIX, null);
    this.credentialEnv = string(props, CREDENTIAL_ENV_SUFFIX, provider.defaultCredentialEnv());
    if (credentialEnv != null) {
      CredentialSource.checkEnvName(key(CREDENTIAL_ENV_SUFFIX), credentialEnv);
    }
  }

  /**
   * Reads and validates the configuration of {@code spec}.
   *
   * @throws IllegalArgumentException naming the offending key: a provider this client does not serve, a missing
   *     endpoint, a dimension the model cannot return, an unknown model without {@link #REQUEST_MAX_INPUT_TOKENS_SUFFIX},
   *     or a limit that is not positive or exceeds the provider's
   */
  public static OpenAiClientConfig from(EmbeddingModelSpec spec, TypedProperties props) {
    return new OpenAiClientConfig(spec, props);
  }

  /** The value sent as {@code dimensions}: null for a model's native length, always the dimension for an unknown model. */
  private Integer dimensionsParam(int dimension) {
    if (provider == OpenAiProvider.HF_TEI) {
      return null;
    }
    if (knownModel == null) {
      return dimension;
    }
    if (dimension == knownModel.nativeDimension()) {
      return null;
    }
    if (!knownModel.dimensionsSettable() || dimension > knownModel.nativeDimension()) {
      throw new IllegalArgumentException(key(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX) + "=" + dimension + " but "
          + baseModel + " returns " + (knownModel.dimensionsSettable() ? "at most " : "exactly ")
          + knownModel.nativeDimension() + " dimensions");
    }
    return dimension;
  }

  /**
   * An absolute http(s) URL with no user info, query or fragment, so a typo fails on the driver for every provider
   * instead of on the executors, a query cannot swallow the appended {@code /embeddings}, and a credential pasted into
   * the URL is refused before it can be printed. Messages name the key, never the value.
   */
  private void checkEndpoint() {
    String key = key(ENDPOINT_SUFFIX);
    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(key + " is not a valid URL (" + e.getReason() + ")");
    }
    boolean http = "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
    if (!http || uri.getHost() == null) {
      throw new IllegalArgumentException(key + " must be an absolute http(s) URL");
    }
    if (uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
      throw new IllegalArgumentException(key + " must not carry user info, a query or a fragment; pass credentials "
          + "through " + key(CREDENTIAL_FILE_SUFFIX) + " or " + key(CREDENTIAL_ENV_SUFFIX));
    }
  }

  private int maxInputTokens(TypedProperties props) {
    String key = key(REQUEST_MAX_INPUT_TOKENS_SUFFIX);
    if (knownModel == null) {
      if (!props.containsKey(key)) {
        throw new IllegalArgumentException(key + " is required for model " + baseModel
            + ", which has no documented per-input limit (known: text-embedding-3-small, text-embedding-3-large, "
            + "text-embedding-ada-002; set " + key(BASE_MODEL_SUFFIX) + " for an Azure deployment of one)");
      }
      return positiveInt(props, REQUEST_MAX_INPUT_TOKENS_SUFFIX, 0);
    }
    int limit = positiveInt(props, REQUEST_MAX_INPUT_TOKENS_SUFFIX, knownModel.maxInputTokens());
    if (limit > knownModel.maxInputTokens()) {
      throw new IllegalArgumentException(key + "=" + limit + " exceeds the " + knownModel.maxInputTokens()
          + "-token limit of " + baseModel);
    }
    return limit;
  }

  /** A positive limit no larger than the provider's ({@code providerMax} -1 = the server reports it, no bound). */
  private long providerBounded(TypedProperties props, String suffix, long providerMax) {
    String key = key(suffix);
    if (!props.containsKey(key)) {
      return providerMax;
    }
    long value = positiveLong(props, suffix);
    if (providerMax > 0 && value > providerMax) {
      throw new IllegalArgumentException(key + "=" + value + " exceeds the " + provider.id() + " maximum " + providerMax);
    }
    return value;
  }

  private int providerBounded(TypedProperties props, String suffix, int providerMax) {
    return (int) Math.min(Integer.MAX_VALUE, providerBounded(props, suffix, (long) providerMax));
  }

  private int positiveInt(TypedProperties props, String suffix, int defaultValue) {
    return ProviderClientSettings.positiveInt(props, key(suffix), defaultValue);
  }

  private long positiveLong(TypedProperties props, String suffix) {
    return NumericConfig.positiveLong(props, key(suffix));
  }

  private String string(TypedProperties props, String suffix, String defaultValue) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key(suffix)).noDefaultValue(), null);
    return StringUtils.isNullOrEmpty(value) || value.trim().isEmpty() ? defaultValue : value.trim();
  }

  private String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + alias + suffix;
  }

  private static String stripTrailingSlashes(String url) {
    String stripped = url;
    while (stripped.endsWith("/")) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }

  public String alias() {
    return alias;
  }

  public OpenAiProvider provider() {
    return provider;
  }

  /** Base URL without a trailing slash. */
  public String endpoint() {
    return endpoint;
  }

  /** The {@code model} sent: the OpenAI model id, Azure deployment name or TEI model id. */
  public String modelName() {
    return modelName;
  }

  /** The model whose documented limits apply. */
  public String baseModel() {
    return baseModel;
  }

  Option<KnownModel> knownModel() {
    return Option.ofNullable(knownModel);
  }

  /** The {@code dimensions} request field, or null to send none. */
  public Integer dimensionsParam() {
    return dimensionsParam;
  }

  /** Most inputs per request; -1 until TEI's {@code /info} supplies it. */
  public int maxInputs() {
    return maxInputs;
  }

  /** Most summed tokens per request; -1 until TEI's {@code /info} supplies it. */
  public long maxTokens() {
    return maxTokens;
  }

  /** Per-input token limit checked locally; 0 = no local check (TEI: no matching tokenizer). */
  public int maxInputTokens() {
    return maxInputTokens;
  }

  /** jtokkit encoding counting tokens locally; null when the client counts UTF-8 bytes instead (TEI). */
  public String encoding() {
    return encoding;
  }

  public int requestTimeoutMs() {
    return settings.requestTimeoutMs();
  }

  public int deadlineMs() {
    return settings.deadlineMs();
  }

  public int bisectMaxRequests() {
    return bisectMaxRequests;
  }

  public int retryMaxAttempts() {
    return settings.retryMaxAttempts();
  }

  public int initialBackoffMs() {
    return settings.initialBackoffMs();
  }

  public int maxBackoffMs() {
    return settings.maxBackoffMs();
  }

  public int outageConsecutiveFailures() {
    return settings.outageConsecutiveFailures();
  }

  /** Path of the key file, or null. */
  public String credentialFile() {
    return credentialFile;
  }

  /** Name of the env var holding the key, or null. */
  public String credentialEnv() {
    return credentialEnv;
  }

  /** Every setting except the credential's value, which this class never holds; the credential appears as its source. */
  @Override
  public String toString() {
    return "OpenAiClientConfig{alias=" + alias + ", provider=" + provider.id() + ", endpoint=" + endpoint + ", model="
        + modelName + ", baseModel=" + baseModel + ", dimensions=" + dimensionsParam + ", maxInputs=" + maxInputs
        + ", maxTokens=" + maxTokens + ", maxInputTokens=" + maxInputTokens + ", timeoutMs=" + requestTimeoutMs()
        + ", deadlineMs=" + deadlineMs() + ", bisectMaxRequests=" + bisectMaxRequests + ", retryMaxAttempts="
        + retryMaxAttempts() + ", backoffMs=" + initialBackoffMs() + ".." + maxBackoffMs() + ", outageConsecutiveFailures="
        + outageConsecutiveFailures() + ", credential=" + CredentialSource.of(this).describe() + "}";
  }
}
