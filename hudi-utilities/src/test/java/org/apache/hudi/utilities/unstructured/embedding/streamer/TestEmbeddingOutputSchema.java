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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Schema evolution appends the nullable image BLOB while preserving table and customer columns. */
class TestEmbeddingOutputSchema {

  private static EmbeddingTableConfig config(boolean computes) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), computes ? EmbeddingTableConfig.MODE_PREP_AND_COMPUTE : EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "file:///tmp/embedding_table");
    if (computes) {
      props.setProperty(EmbeddingTableConfig.MODELS.key(), "m");
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "2");
    }
    return EmbeddingTableConfig.from(props);
  }

  /** A legacy table gains one nullable image column after all existing fields. */
  @Test
  void existingColumnsArePreservedWhenNullableImageIsAdded() {
    EmbeddingTableConfig config = config(false);
    StructType table = new StructType(Arrays.stream(EmbeddingTableSchema.build(config).fields())
        .filter(field -> !field.name().equals(EmbeddingTableSchema.PREPARED_IMAGE)).toArray(StructField[]::new));
    StructType round = EmbeddingTableSchema.outputSchema(config, Option.of(table), Option.empty());
    assertEquals(table.size() + 1, round.size());
    assertArrayEquals(table.fields(), Arrays.copyOf(round.fields(), table.size()));
    StructField image = round.fields()[table.size()];
    assertEquals(EmbeddingTableSchema.PREPARED_IMAGE, image.name());
    assertEquals(BlobType.dataType(), image.dataType());
    assertTrue(image.nullable());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void legacyTableGainsNullableImageBlobBeforeAnyNewModelColumn(boolean computes) {
    EmbeddingTableConfig config = config(computes);
    StructType legacy = new StructType(Arrays.stream(EmbeddingTableSchema.build(config(false)).fields())
        .filter(field -> !field.name().equals(EmbeddingTableSchema.PREPARED_IMAGE)).toArray(StructField[]::new))
        .add(DataTypes.createStructField("customer_note", DataTypes.StringType, true));
    Option<EmbeddingModelSpec> model = computes ? Option.of(config.model("m")) : Option.empty();
    StructType expected = legacy.add(EmbeddingTableSchema.preparedImageField());
    if (computes) {
      expected = expected.add(EmbeddingTableSchema.vectorField(model.get()));
    }

    StructType round = EmbeddingTableSchema.outputSchema(config, Option.of(legacy), model);

    assertEquals(expected, round);
    assertEquals(EmbeddingTableSchema.preparedImageField(), round.apply(EmbeddingTableSchema.PREPARED_IMAGE));
    assertArrayEquals(expected.fieldNames(), EmbeddingTableSchema.roundSchema(config, Option.of(legacy)).fieldNames());
  }

  @Test
  void currentTableKeepsItsColumnsAndBlobMetadataWithoutDuplicatingImage() {
    EmbeddingTableConfig config = config(false);
    StructType table = EmbeddingTableSchema.build(config)
        .add(DataTypes.createStructField("customer_note", DataTypes.StringType, true));

    assertEquals(table, EmbeddingTableSchema.outputSchema(config, Option.of(table), Option.empty()));
    assertEquals(table, EmbeddingTableSchema.outputSchema(config, Option.empty(), Option.empty())
        .add(DataTypes.createStructField("customer_note", DataTypes.StringType, true)));
  }

  /** A complete existing schema retains its columns and their order. */
  @Test
  void existingTableColumnsAreKeptAsTheyAre() {
    EmbeddingTableConfig config = config(false);
    StructType table = EmbeddingTableSchema.build(config).add("customer_column", DataTypes.StringType, true);
    StructType round = EmbeddingTableSchema.outputSchema(config, Option.of(table), Option.empty());
    assertArrayEquals(table.fields(), round.fields());
  }

  /** Missing identity columns are refused even when image migration is supported. */
  @Test
  void missingIdentityColumnIsRefused() {
    EmbeddingTableConfig config = config(false);
    StructType table = new StructType(Arrays.stream(EmbeddingTableSchema.build(config).fields())
        .filter(field -> !field.name().equals(EmbeddingTableSchema.SOURCE_PATH_SHA)).toArray(StructField[]::new));
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableSchema.outputSchema(config, Option.of(table), Option.empty()));
    assertTrue(refused.getMessage().contains(EmbeddingTableSchema.SOURCE_PATH_SHA), refused.getMessage());
  }
}
