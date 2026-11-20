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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientNames;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The embedding table's columns, record key and client lookup for a declared dummy model. */
public class TestEmbeddingTableSchema {

  @TempDir
  Path tempDir;

  @Test
  void vectorColumnIsANullableVectorOfNonNullFloatsNamedByTheModelWithNoComment() {
    EmbeddingModelSpec model = EmbeddingTableConfig.from(dummyModel("m1", "4")).model("m1");
    StructField field = EmbeddingTableSchema.vectorField(model);
    assertEquals("emb_m1", field.name());
    assertTrue(field.nullable(), "nullable, so a model column can be added to an existing table");
    assertEquals(new ArrayType(DataTypes.FloatType, false), field.dataType());
    assertEquals("VECTOR(4)", field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
    assertFalse(field.metadata().contains("comment"), "no model information in the column's doc");
  }

  @Test
  void clientClassNameResolvesTheDummyProviderAndAConfiguredClient() {
    assertEquals("org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient",
        EmbeddingClientNames.clientClassName(EmbeddingTableConfig.from(dummyModel("m1", "4")).model("m1")));

    TypedProperties custom = dummyModel("m1", "4");
    custom.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        " " + RecordingClient.class.getName() + " ");
    assertEquals(RecordingClient.class.getName(), EmbeddingClientNames.clientClassName(EmbeddingTableConfig.from(custom).model("m1")));

    TypedProperties notAClient = dummyModel("m1", "4");
    notAClient.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        String.class.getName());
    IllegalArgumentException wrongType = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientNames.clientClassName(EmbeddingTableConfig.from(notAClient).model("m1")));
    assertTrue(wrongType.getMessage().contains(String.class.getName() + ", which is not an "), wrongType.getMessage());

    TypedProperties otherProvider = dummyModel("m1", "4");
    otherProvider.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.provider", "unknown");
    assertThrows(UnsupportedOperationException.class,
        () -> EmbeddingClientNames.clientClassName(EmbeddingTableConfig.from(otherProvider).model("m1")));
  }

  @Test
  void writerReadSchemaIsEmptyForAMissingTableAndForATableWithNoCommit() throws Exception {
    StorageConfiguration<?> conf = HoodieTestUtils.getDefaultStorageConf();
    String missing = tempDir.resolve("missing").toUri().toString();
    assertFalse(EmbeddingTableChecks.tableExists(missing, conf));
    assertFalse(EmbeddingTableSchema.writerReadSchema(missing, conf).isPresent());

    String empty = tempDir.resolve("empty").toString();
    HoodieTestUtils.init(conf, empty);
    assertTrue(EmbeddingTableChecks.tableExists(empty, conf));
    assertFalse(EmbeddingTableSchema.writerReadSchema(empty, conf).isPresent());
  }

  /** With no model declared, the prep_only shape, the table has the fixed columns only. */
  @Test
  void noDeclaredModelGivesTheFixedColumnsOnly() {
    StructType schema = EmbeddingTableSchema.build(EmbeddingTableConfig.from(new TypedProperties()));
    assertEquals(EmbeddingTableSchema.BASE_COLUMNS, Arrays.asList(schema.fieldNames()));
  }

  /** An error is cut on a code-point boundary: a surrogate pair across the cut is dropped whole. */
  @Test
  void truncateErrorNeverSplitsASurrogatePair() {
    int max = EmbeddingTableSchema.MAX_ERROR_CHARS;
    assertNull(EmbeddingTableSchema.truncateError(null));
    String exact = repeat('a', max);
    assertEquals(exact, EmbeddingTableSchema.truncateError(exact));
    assertEquals(exact, EmbeddingTableSchema.truncateError(exact + "b"));
    String pair = new String(Character.toChars(0x1F600));
    String pairAtCut = repeat('a', max - 1) + pair + "tail";
    assertEquals(repeat('a', max - 1), EmbeddingTableSchema.truncateError(pairAtCut));
    String pairBeforeCut = repeat('a', max - 2) + pair + "tail";
    assertEquals(pairBeforeCut.substring(0, max), EmbeddingTableSchema.truncateError(pairBeforeCut));
  }

  /** A partition field named as a fixed column is that column, and is not added again; other fields follow the model columns. */
  @Test
  void partitionFieldsFollowTheModelColumnsAndAFixedColumnIsNotAddedAgain() {
    TypedProperties props = dummyModel("m1", "4");
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "region, source_file_extension,date");
    StructType schema = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props));
    List<String> names = Arrays.asList(schema.fieldNames());
    assertEquals(Arrays.asList("emb_m1", "region", "date"), names.subList(EmbeddingTableSchema.BASE_COLUMN_COUNT, names.size()));
  }

  /** A model column that is also a partition field (ignoring case, as Spark resolves columns) is refused before any write. */
  @Test
  void modelColumnThatIsAlsoAPartitionFieldIsRefused() {
    TypedProperties props = dummyModel("m1", "4");
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "region,EMB_M1");
    HoodieException error = assertThrows(HoodieException.class, () -> EmbeddingTableSchema.build(EmbeddingTableConfig.from(props)));
    assertTrue(error.getMessage().contains("EMB_M1"), error.getMessage());
    assertTrue(error.getMessage().contains("m1 (emb_m1)"), error.getMessage());
    assertTrue(error.getMessage().contains(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key()), error.getMessage());
  }

  /** A partition field listed twice (ignoring case) is refused before any write. */
  @Test
  void partitionFieldListedTwiceIsRefused() {
    for (String fields : new String[] {"region,date,region", "region, Region"}) {
      TypedProperties props = dummyModel("m1", "4");
      props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), fields);
      HoodieException error = assertThrows(HoodieException.class,
          () -> EmbeddingTableSchema.build(EmbeddingTableConfig.from(props)), fields);
      assertTrue(error.getMessage().contains("listed twice"), error.getMessage());
      assertTrue(error.getMessage().contains(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key()), error.getMessage());
    }
  }

  private static String repeat(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  private static TypedProperties dummyModel(String alias, String dimensions) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.unstructured.embedding.models", alias);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + alias + ".provider", "dummy");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + alias + ".dimensions", dimensions);
    return props;
  }

  /** An {@link EmbeddingClient} a model can name through its client class key. */
  public static class RecordingClient implements EmbeddingClient {
    private EmbeddingModelSpec spec;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      this.spec = spec;
    }

    @Override
    public EmbeddingModelSpec spec() {
      return spec;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      throw new UnsupportedOperationException();
    }
  }
}
