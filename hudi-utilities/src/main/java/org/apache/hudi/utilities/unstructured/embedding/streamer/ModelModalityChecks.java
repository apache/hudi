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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.KnownEmbeddingModels;
import org.apache.hudi.utilities.unstructured.embedding.prep.FilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The start checks of the active model's declared modalities ({@code hoodie.unstructured.embedding.model.<alias>.modalities}),
 * on the driver before any Spark work, under {@code prep_and_compute}:
 * <ol>
 *   <li>{@link #checkDeclaration}: the key is set; every declared modality is one the model's client has a request format for
 *       ({@link EmbeddingClient#supportedModalities}); and none goes beyond what a known model can embed
 *       ({@link KnownEmbeddingModels}). A model name with no entry is trusted as declared.</li>
 *   <li>{@link #checkContent}: the modalities the job prepares (the preprocessors of {@code prep.file.extensions}, or of every
 *       mapped extension when it is unset, under the job's settings, e.g. {@code prep.audio.enabled}) against the declared ones.
 *       Content of an undeclared modality is refused while the error table is off, since the first batch holding such a row
 *       would fail its round on every sync; with the error table on the job starts and logs one WARN, and each such row gets an
 *       {@code EMBED-COMPUTE-<MODALITY>-NOT-SUPPORTED} error record.</li>
 * </ol>
 */
final class ModelModalityChecks {

  private static final Logger LOG = LoggerFactory.getLogger(ModelModalityChecks.class);
  /** Content mismatches already logged: the check runs every round, the WARN once per model and mismatch per JVM. */
  private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();
  /** Why a provider takes no image, for the providers whose API has no image input at all. */
  private static final Map<String, String> NO_IMAGE_REASONS;

  static {
    Map<String, String> reasons = new HashMap<>();
    String openAi = "the OpenAI embeddings API takes no image input";
    reasons.put("openai", openAi);
    reasons.put("azure-openai", openAi);
    reasons.put("hf-tei", "image input for OpenAI-compatible servers is not supported");
    NO_IMAGE_REASONS = Collections.unmodifiableMap(reasons);
  }

  private ModelModalityChecks() {
  }

  /**
   * The key is set, and every declared modality is one the client of {@code model} can send and a known model can embed.
   *
   * @throws IllegalArgumentException naming the key: unset, a modality the client has no request format for (naming the
   *     provider and the model), or a modality beyond a known model's
   */
  static void checkDeclaration(EmbeddingModelSpec model) {
    String key = key(model);
    Set<String> declared = model.getModalities();
    if (declared.isEmpty()) {
      throw new IllegalArgumentException("Missing required config " + key + ": declare the modalities model " + model.getName()
          + " embeds: " + EmbeddingTableSchema.MODALITY_TEXT + ", " + EmbeddingTableSchema.MODALITY_IMAGE + " or "
          + EmbeddingTableSchema.MODALITY_TEXT + "," + EmbeddingTableSchema.MODALITY_IMAGE + ". Nothing was written");
    }
    String className = EmbeddingClientFactory.clientClassName(model);
    Set<String> supported = ((EmbeddingClient) ReflectionUtils.loadClass(className)).supportedModalities(model.getModelName());
    Set<String> unsupported = without(declared, supported);
    if (!unsupported.isEmpty()) {
      String reason = unsupported.contains(EmbeddingTableSchema.MODALITY_IMAGE)
          ? NO_IMAGE_REASONS.get(model.getProvider()) : null;
      throw new IllegalArgumentException("Config " + key + "=" + String.join(",", declared) + " declares " + unsupported
          + ", which provider " + model.getProvider() + " has no request format for with model " + model.getModelName()
          + (reason == null ? "" : " (" + reason + ")") + "; its client " + className + " embeds " + supported
          + ". Nothing was written");
    }
    Option<Set<String>> known = KnownEmbeddingModels.modalities(model.getModelName());
    if (known.isPresent() && !known.get().containsAll(declared)) {
      throw new IllegalArgumentException("Config " + key + "=" + String.join(",", declared) + " declares "
          + without(declared, known.get()) + ", but model " + model.getModelName() + " (provider " + model.getProvider()
          + ") embeds only " + known.get() + ". Nothing was written");
    }
  }

  /**
   * The modalities the job prepares against those {@code model} declares: an undeclared one is refused while the error table is
   * off and logged (WARN) once per JVM, model and mismatch while it is on; the check runs every round.
   *
   * @return the undeclared modalities the job prepares, each with the extensions that produce it (empty when none)
   * @throws IllegalArgumentException with the error table off, naming the extensions key, the modalities key and the error table
   *     key
   */
  static Map<String, Set<String>> checkContent(EmbeddingTableConfig config, PreprocessorRegistry registry, EmbeddingModelSpec model) {
    Map<String, Set<String>> undeclared = new TreeMap<>(preparedModalities(config, registry));
    undeclared.keySet().removeAll(model.getModalities());
    if (undeclared.isEmpty()) {
      return undeclared;
    }
    String mismatch = "Config " + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key() + "="
        + (config.prepFileExtensions().isPresent() ? String.join(",", config.prepFileExtensions().get()) : "(unset: every mapped extension)")
        + " prepares " + describe(undeclared) + ", which " + key(model) + "=" + String.join(",", model.getModalities())
        + " does not declare";
    if (!PrepRound.errorTableEnabled(config.getProps())) {
      throw new IllegalArgumentException(mismatch + ", and the error table is off (" + HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key()
          + "): the first batch holding such a row would fail its round on every sync. List only extensions of declared "
          + "modalities in " + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key() + ", declare the modalities in " + key(model)
          + ", or turn the error table on. Nothing was written");
    }
    if (WARNED.add(model.getName() + " " + mismatch)) {
      LOG.warn("EMBED-MODALITIES-CONTENT-MISMATCH model={}: {}; each such row gets a NULL vector and an "
          + "EMBED-COMPUTE-<MODALITY>-NOT-SUPPORTED error record", model.getName(), mismatch);
    }
    return undeclared;
  }

  /** Each modality the job prepares, with the extensions whose preprocessor produces it under the job's settings. */
  static Map<String, Set<String>> preparedModalities(EmbeddingTableConfig config, PreprocessorRegistry registry) {
    Option<List<String>> allowed = config.prepFileExtensions();
    Collection<String> extensions = allowed.isPresent() ? allowed.get() : registry.extensions();
    TypedProperties props = config.getProps();
    Map<String, Set<String>> prepared = new TreeMap<>();
    for (String extension : extensions) {
      Option<FilePreprocessor> preprocessor = registry.forExtension(extension);
      if (!preprocessor.isPresent()) {
        continue;
      }
      for (String modality : preprocessor.get().modalities(props)) {
        prepared.computeIfAbsent(modality, m -> new TreeSet<>()).add(extension);
      }
    }
    return prepared;
  }

  private static String describe(Map<String, Set<String>> byModality) {
    StringBuilder text = new StringBuilder();
    for (Map.Entry<String, Set<String>> entry : byModality.entrySet()) {
      text.append(text.length() == 0 ? "" : " and ").append(entry.getKey()).append(" rows (from ")
          .append(String.join(", ", entry.getValue())).append(")");
    }
    return text.toString();
  }

  private static Set<String> without(Set<String> set, Set<String> removed) {
    Set<String> rest = new TreeSet<>(set);
    rest.removeAll(removed);
    return rest;
  }

  private static String key(EmbeddingModelSpec model) {
    return EmbeddingTableConfig.MODEL_PREFIX + model.getName() + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX;
  }
}
