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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The round's start-up checks of the mode: the mode is required and is one of two values; a {@code prep_only} job sets no model
 * and no compute setting, and its table has no VECTOR column; under {@code prep_and_compute} exactly one model is active, whose
 * column and dimensions name and size the VECTOR column, and its model has a client; a round whose mode does not match the existing
 * table, or whose model's dimensions differ from the existing table's column, is refused.
 */
public class TestEmbeddingModeConfig {

  private static final String MODEL = EmbeddingTableConfig.MODEL_PREFIX + "m1";

  private static TypedProperties props(String mode) {
    TypedProperties props = new TypedProperties();
    if (mode != null) {
      props.setProperty(EmbeddingTableConfig.MODE.key(), mode);
    }
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "8");
    props.setProperty(MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    return props;
  }

  @Test
  public void theModeIsRequired() {
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props(null)).mode());
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.MODE.key()) && refused.getMessage().contains("unset"),
        refused.getMessage());
  }

  @Test
  public void theModeIsOneOfTwoValues() {
    assertEquals(EmbeddingTableConfig.MODE_PREP_ONLY, EmbeddingTableConfig.from(props(" PREP_ONLY ")).mode());
    assertFalse(EmbeddingTableConfig.from(props("prep_only")).computes());
    assertTrue(EmbeddingTableConfig.from(props("prep_and_compute")).computes());
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props("compute_only")).mode());
    assertTrue(refused.getMessage().contains("compute_only"), refused.getMessage());
  }

  /** A {@code prep_only} job: the mode only, no model declared. */
  private static TypedProperties prepOnly() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    return props;
  }

  @ParameterizedTest
  @ValueSource(strings = {"hoodie.unstructured.embedding.compute.batch.size", "hoodie.unstructured.embedding.compute.max.attempts",
      "hoodie.unstructured.embedding.compute.retry.backoff.ms", "hoodie.unstructured.embedding.compute.parallelism",
      "hoodie.unstructured.embedding.compute.image.long.side", "hoodie.unstructured.embedding.compute.image.decoder.class",
      "hoodie.unstructured.embedding.models", "hoodie.unstructured.embedding.active.models",
      "hoodie.unstructured.embedding.model.m1.column", "hoodie.unstructured.embedding.model.m1.dimensions",
      "hoodie.unstructured.embedding.model.m1.provider", "hoodie.unstructured.embedding.model.m1.name",
      "hoodie.unstructured.embedding.model.m1.client.class"})
  public void prepOnlyJobRefusesModelAndComputeSettings(String key) {
    TypedProperties props = prepOnly();
    props.setProperty(key, "1");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.checkNoModelOrComputeSettings(EmbeddingTableConfig.from(props)));
    assertTrue(refused.getMessage().contains("[" + key + "]"), refused.getMessage());
    assertTrue(refused.getMessage().contains("Nothing was written"), refused.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"hoodie.unstructured.embedding.models", "hoodie.unstructured.embedding.active.models"})
  public void prepOnlyJobRefusesAModelKeySetEvenToItsEmptyDefault(String key) {
    // a set key is refused whatever its value: a prep_only job carries no model setting at all
    TypedProperties props = prepOnly();
    props.setProperty(key, "");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.checkNoModelOrComputeSettings(EmbeddingTableConfig.from(props)));
    assertTrue(refused.getMessage().contains("[" + key + "]"), refused.getMessage());
  }

  @Test
  public void theKeyRefusalOffersPrepAndCompute() {
    TypedProperties props = prepOnly();
    props.setProperty(EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "8");
    // the same refusal whether the table exists or not: an existing table takes prep_and_compute rounds too
    String refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.checkNoModelOrComputeSettings(EmbeddingTableConfig.from(props))).getMessage();
    assertTrue(refused.contains(", or set " + EmbeddingTableConfig.MODE.key() + "=" + EmbeddingTableConfig.MODE_PREP_AND_COMPUTE),
        refused);
  }

  @Test
  public void prepOnlyJobRefusesTheModelsClientSettings() {
    for (String suffix : new String[] {ProviderClientSettings.REQUEST_DEADLINE_MS_SUFFIX, ProviderClientSettings.RETRY_MAX_ATTEMPTS_SUFFIX,
        RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX}) {
      TypedProperties props = prepOnly();
      props.setProperty(MODEL + suffix, "5");
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
          () -> EmbeddingTableTransformer.checkNoModelOrComputeSettings(EmbeddingTableConfig.from(props)), suffix);
      assertTrue(refused.getMessage().contains(MODEL + suffix), refused.getMessage());
    }
  }

  @Test
  public void prepOnlyJobNamesEveryRefusedKeyAtOnce() {
    TypedProperties props = props(EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "8");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.checkNoModelOrComputeSettings(EmbeddingTableConfig.from(props)));
    // sorted, as the message lists them
    assertTrue(refused.getMessage().contains(Arrays.asList(EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(),
        MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX,
        EmbeddingTableConfig.MODELS.key()).toString()), refused.getMessage());
  }

  @Test
  public void prepOnlyJobNeedsNoModelAndItsTableHasNoVectorColumn() {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(prepOnly());
    EmbeddingTableTransformer.checkNoModelOrComputeSettings(config);
    StructType schema = EmbeddingTableSchema.build(config);
    assertEquals(EmbeddingTableSchema.BASE_COLUMNS, Arrays.asList(schema.fieldNames()));
  }

  @Test
  public void exactlyOneModelIsActive() {
    TypedProperties props = props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1,m2");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m2" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m2" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingTableTransformer.activeModel(EmbeddingTableConfig.from(props)));
    props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), "m2");
    assertEquals("m2", EmbeddingTableTransformer.activeModel(EmbeddingTableConfig.from(props)).getName());
  }

  @Test
  public void anActiveModelThatIsNotDeclaredIsRefused() {
    // one valid alias next to a typo must not pass: the typo would be ignored and the job would run on the valid one alone
    TypedProperties props = props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), "m1,typo");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.activeModel(EmbeddingTableConfig.from(props)));
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.ACTIVE_MODELS.key() + "=m1,typo"), refused.getMessage());
    assertTrue(refused.getMessage().contains("[typo]") && refused.getMessage().contains("[m1]"), refused.getMessage());
  }

  @Test
  public void prepAndComputeJobNeedsAModelWithAClient() {
    TypedProperties props = props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "no-such-provider");
    assertThrows(UnsupportedOperationException.class,
        () -> EmbeddingClientFactory.clientClassName(EmbeddingTableTransformer.activeModel(EmbeddingTableConfig.from(props))));
    TypedProperties noProvider = props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    noProvider.remove(MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX);
    assertThrows(RuntimeException.class, () -> EmbeddingTableTransformer.activeModel(EmbeddingTableConfig.from(noProvider)));
  }

  @Test
  public void prepOnlyTableHasTheFixedAndThePathPartitionColumnsOnly() {
    TypedProperties props = prepOnly();
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "team,region");
    StructType schema = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props));
    List<String> expected = new ArrayList<>(EmbeddingTableSchema.BASE_COLUMNS);
    expected.addAll(Arrays.asList("team", "region"));
    assertEquals(expected, Arrays.asList(schema.fieldNames()));
  }

  @Test
  public void prepAndComputeRoundOnATableWithAnotherDimensionIsRefusedAtStart() {
    StructType tableAt8 = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE)));
    TypedProperties at16 = props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    at16.setProperty(MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "16");
    at16.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tables/emb");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(at16);
    // no SparkSession: the refusal comes before any Spark work, so no task runs and no model is called
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableTransformer.checkRound(config, null, Option.of(tableAt8)));
    String message = refused.getMessage();
    assertTrue(message.contains("VECTOR(16)") && message.contains("VECTOR(8)") && message.contains("/tables/emb"), message);
    assertTrue(message.contains(MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX + "=16"), message);
    assertTrue(message.contains("new table") && message.contains("Nothing was written"), message);
    // the same dimensions pass
    EmbeddingTableTransformer.checkModelColumnMatchesTable(EmbeddingTableTransformer.activeModel(
        EmbeddingTableConfig.from(props(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE))), "/tables/emb", tableAt8);
  }
}
