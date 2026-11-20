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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientNames;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Accessors and diagnostics of the embedding configuration. */
public class TestEmbeddingConfigProperties {
  @Test
  void validModelDimensionsControl() {
    TypedProperties props = modelProperties("8");
    assertEquals(8, EmbeddingTableConfig.from(props).models().get(0).getDimension());
  }

  @Test
  void invalidModelDimensionsNamesKeyAndValue() {
    String key = "hoodie.unstructured.embedding.model.m1.dimensions";
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(modelProperties("not-a-number")).models());
    assertTrue(error.getMessage().contains(key), error.getMessage());
    assertTrue(error.getMessage().contains("not-a-number"), error.getMessage());
  }

  @Test
  void chunkMaxTokensDefaultsAndMustBePositive() {
    assertEquals(256, EmbeddingTableConfig.from(new TypedProperties()).chunkMaxTokens());
    TypedProperties zero = props(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "0");
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(zero).chunkMaxTokens());
    assertTrue(error.getMessage().contains(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()), error.getMessage());
  }

  /** A properties file keeps trailing spaces; a numeric value is read without them. */
  @Test
  void numericValuesIgnoreSurroundingSpaces() {
    TypedProperties props = props(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100 ");
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), " 250");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    assertEquals(100, config.chunkMaxTokens());
    assertEquals(250L, config.computeRetryBackoffMs());
  }

  @Test
  void chunkOverlapTokensDefaultsToHalfTheLimitUpTo128AndIsBounded() {
    assertEquals(128, EmbeddingTableConfig.from(new TypedProperties()).chunkOverlapTokens());
    assertEquals(50, EmbeddingTableConfig.from(props(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100")).chunkOverlapTokens());
    TypedProperties set = props(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100");
    set.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "50");
    assertEquals(50, EmbeddingTableConfig.from(set).chunkOverlapTokens());
    for (String overlap : new String[] {"51", "-1"}) {
      TypedProperties bad = props(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100");
      bad.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), overlap);
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
          () -> EmbeddingTableConfig.from(bad).chunkOverlapTokens(), overlap);
      assertTrue(error.getMessage().contains(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key()), error.getMessage());
    }
  }

  @Test
  void modeIsRequiredFoldsCaseAndRefusesOtherValues() {
    IllegalArgumentException unset = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(new TypedProperties()).mode());
    assertTrue(unset.getMessage().contains("unset"), unset.getMessage());
    IllegalArgumentException other = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.MODE.key(), "compute")).mode());
    assertTrue(other.getMessage().contains("'compute'"), other.getMessage());
    EmbeddingTableConfig prepOnly = EmbeddingTableConfig.from(props(EmbeddingTableConfig.MODE.key(), " PREP_ONLY "));
    assertEquals(EmbeddingTableConfig.MODE_PREP_ONLY, prepOnly.mode());
    assertFalse(prepOnly.computes());
    assertTrue(EmbeddingTableConfig.from(props(EmbeddingTableConfig.MODE.key(), "Prep_And_Compute")).computes());
  }

  @Test
  void prepFileExtensionsAreTrimmedLowerCasedSortedAndDeduplicated() {
    assertFalse(EmbeddingTableConfig.from(new TypedProperties()).prepFileExtensions().isPresent());
    assertFalse(EmbeddingTableConfig.from(props(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "  ")).prepFileExtensions().isPresent());
    assertEquals(Arrays.asList("pdf", "txt"),
        EmbeddingTableConfig.from(props(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "PDF, txt,pdf")).prepFileExtensions().get());
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), " , ")).prepFileExtensions());
    assertTrue(error.getMessage().contains("lists no file extension"), error.getMessage());
  }

  @Test
  void prepSourceRetryRefusesNoAttemptAndANegativeBackoff() {
    EmbeddingTableConfig.from(new TypedProperties()).prepSourceRetry();
    IllegalArgumentException attempts = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.PREP_SOURCE_RETRY_MAX_ATTEMPTS.key(), "0")).prepSourceRetry());
    assertTrue(attempts.getMessage().contains(EmbeddingTableConfig.PREP_SOURCE_RETRY_MAX_ATTEMPTS.key()), attempts.getMessage());
    IllegalArgumentException backoff = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.PREP_SOURCE_RETRY_BACKOFF_MS.key(), "-1")).prepSourceRetry());
    assertTrue(backoff.getMessage().contains(EmbeddingTableConfig.PREP_SOURCE_RETRY_BACKOFF_MS.key()), backoff.getMessage());
  }

  @Test
  void computeParallelismAndBalancePartitionsDefaultWhenUnsetAndMustBePositive() {
    EmbeddingTableConfig unset = EmbeddingTableConfig.from(new TypedProperties());
    assertEquals(7, unset.computeParallelism(7));
    assertEquals(7, unset.balanceNumPartitions(7));
    assertEquals(4, EmbeddingTableConfig.from(props(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "4")).computeParallelism(7));
    assertEquals(4, EmbeddingTableConfig.from(props(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "4")).balanceNumPartitions(7));
    assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "0")).computeParallelism(7));
    assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "0")).balanceNumPartitions(7));
  }

  @Test
  void modelWithoutProviderOrDimensionsNamesTheMissingKey() {
    TypedProperties noProvider = modelProperties("8");
    noProvider.remove("hoodie.unstructured.embedding.model.m1.provider");
    HoodieException provider = assertThrows(HoodieException.class, () -> EmbeddingTableConfig.from(noProvider).models());
    assertTrue(provider.getMessage().contains("hoodie.unstructured.embedding.model.m1.provider"), provider.getMessage());

    TypedProperties noDimensions = modelProperties("8");
    noDimensions.remove("hoodie.unstructured.embedding.model.m1.dimensions");
    HoodieException dimensions = assertThrows(HoodieException.class, () -> EmbeddingTableConfig.from(noDimensions).models());
    assertTrue(dimensions.getMessage().contains("hoodie.unstructured.embedding.model.m1.dimensions"), dimensions.getMessage());
  }

  /** A properties file keeps trailing spaces; the provider, column and name of a model are read without them. */
  @Test
  void modelValuesIgnoreSurroundingSpaces() {
    TypedProperties props = modelProperties("8");
    props.setProperty("hoodie.unstructured.embedding.model.m1.provider", "dummy ");
    props.setProperty("hoodie.unstructured.embedding.model.m1.column", " emb_x ");
    props.setProperty("hoodie.unstructured.embedding.model.m1.name", " model-x ");
    EmbeddingModelSpec spec = EmbeddingTableConfig.from(props).model("m1");
    assertEquals(DummyEmbeddingClient.PROVIDER, spec.getProvider());
    assertEquals("emb_x", spec.getColumn());
    assertEquals("model-x", spec.getModelName());
    assertEquals(DummyEmbeddingClient.class.getName(), EmbeddingClientNames.clientClassName(spec));
  }

  /** A model's client class is read without surrounding spaces as well. */
  @Test
  void modelClientClassIgnoresSurroundingSpaces() {
    TypedProperties props = modelProperties("8");
    props.setProperty("hoodie.unstructured.embedding.model.m1.client.class", " " + DummyEmbeddingClient.class.getName() + " ");
    assertEquals(DummyEmbeddingClient.class.getName(), EmbeddingTableConfig.from(props).model("m1").getClientClass());
  }

  @Test
  void anAliasListedTwiceIsNamedAsSuch() {
    TypedProperties props = modelProperties("8");
    props.setProperty("hoodie.unstructured.embedding.models", "m1, m1");
    HoodieException error = assertThrows(HoodieException.class, () -> EmbeddingTableConfig.from(props).models());
    assertTrue(error.getMessage().contains("m1 is listed twice in hoodie.unstructured.embedding.models"), error.getMessage());
  }

  /** Unset, every declared model is active; set, the declared models it names, in declaration order. */
  @Test
  void activeModelsDefaultToEveryDeclaredModel() {
    TypedProperties props = modelProperties("8");
    props.setProperty("hoodie.unstructured.embedding.models", "m1,m2");
    props.setProperty("hoodie.unstructured.embedding.model.m2.provider", "dummy");
    props.setProperty("hoodie.unstructured.embedding.model.m2.dimensions", "4");
    assertEquals(Arrays.asList("m1", "m2"), names(EmbeddingTableConfig.from(props)));
    props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), "m2");
    assertEquals(Collections.singletonList("m2"), names(EmbeddingTableConfig.from(props)));
  }

  @Test
  void concreteAliasSettingsDoNotReplaceAnotherAliasesDefaults() {
    TypedProperties props = modelProperties("8");
    props.setProperty("hoodie.unstructured.embedding.models", "m1,m2");
    props.setProperty("hoodie.unstructured.embedding.model.m1.column", "custom_vector");
    props.setProperty("hoodie.unstructured.embedding.model.m1.name", "custom_model");
    props.setProperty("hoodie.unstructured.embedding.model.m2.provider", "dummy");
    props.setProperty("hoodie.unstructured.embedding.model.m2.dimensions", "16");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    assertEquals("custom_vector", config.model("m1").getColumn());
    assertEquals("custom_model", config.model("m1").getModelName());
    assertEquals("emb_m2", config.model("m2").getColumn());
    assertEquals("m2", config.model("m2").getModelName());
    assertEquals(16, config.model("m2").getDimension());
  }

  private static List<String> names(EmbeddingTableConfig config) {
    return config.activeModels().stream().map(EmbeddingModelSpec::getName).collect(Collectors.toList());
  }

  private static TypedProperties props(String key, String value) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key, value);
    return props;
  }

  private static TypedProperties modelProperties(String dimensions) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.unstructured.embedding.models", "m1");
    props.setProperty("hoodie.unstructured.embedding.model.m1.provider", "dummy");
    props.setProperty("hoodie.unstructured.embedding.model.m1.dimensions", dimensions);
    return props;
  }
}
