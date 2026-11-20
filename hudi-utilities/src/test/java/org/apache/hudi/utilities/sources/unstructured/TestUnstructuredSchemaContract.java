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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The unstructured tables' contract columns, and a user target schema checked against them. */
public class TestUnstructuredSchemaContract {

  private static TypedProperties embedding(String mode, String... models) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), mode);
    if (models.length > 0) {
      props.setProperty(EmbeddingTableConfig.MODELS.key(), String.join(",", models));
      props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), models[models.length - 1]);
      for (String model : models) {
        props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + model + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
        props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
      }
    }
    return props;
  }

  private static StructType contract(UnstructuredMode mode, TypedProperties props) {
    return UnstructuredSchemaContract.contract(mode, props, Option.empty());
  }

  /** {@code schema} as a user's Avro target schema. */
  private static HoodieSchema target(StructType schema) {
    return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(schema, "user_target", "user.ns");
  }

  private static void check(UnstructuredMode mode, StructType contract, StructType schema) {
    UnstructuredSchemaContract.checkTarget(mode, contract, target(schema));
  }

  private static String refusal(UnstructuredMode mode, StructType contract, StructType schema) {
    return assertThrows(HoodieSchemaException.class, () -> check(mode, contract, schema)).getMessage();
  }

  private static List<String> names(StructType schema) {
    return Arrays.asList(schema.fieldNames());
  }

  private static StructType without(StructType schema, String column) {
    return new StructType(Arrays.stream(schema.fields()).filter(f -> !f.name().equals(column)).toArray(StructField[]::new));
  }

  private static StructType replace(StructType schema, StructField replacement) {
    return new StructType(Arrays.stream(schema.fields()).map(f -> f.name().equals(replacement.name()) ? replacement : f)
        .toArray(StructField[]::new));
  }

  /** A nullable BLOB column named {@code name}. */
  private static StructField blob(String name) {
    StructField blob = BlobTableSchema.schema().apply(BlobTableSchema.BLOB);
    return blob.copy(name, blob.dataType(), true, blob.metadata());
  }

  private static StructField vector(String name, int dimension) {
    Metadata metadata = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(" + dimension + ")").build();
    return new StructField(name, DataTypes.createArrayType(DataTypes.FloatType, false), true, metadata);
  }

  @Test
  void goldenContracts() {
    assertEquals(BlobTableSchema.schema(), contract(UnstructuredMode.BLOB, new TypedProperties()));
    TypedProperties partitioned = new TypedProperties();
    partitioned.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "country,SOURCE_FILE_NAME");
    StructType blob = contract(UnstructuredMode.BLOB, partitioned);
    assertEquals(Arrays.asList("source_path_sha", "source_modification_time", "source_file_path", "source_file_name",
        "source_file_extension", "source_file_size", "content_type", "blob", "country"), names(blob), "a fixed name is not repeated");

    List<String> base = new ArrayList<>(EmbeddingTableSchema.BASE_COLUMNS);
    assertEquals(base, names(contract(UnstructuredMode.EMBEDDING, embedding("prep_only"))));
    List<String> one = new ArrayList<>(base);
    one.add("emb_m1");
    assertEquals(one, names(contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1"))));
    List<String> two = new ArrayList<>(one);
    two.add("emb_m2");
    assertEquals(two, names(contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1", "m2"))));
    TypedProperties withPartition = embedding("prep_only");
    withPartition.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "country");
    List<String> partitionedEmbedding = new ArrayList<>(base);
    partitionedEmbedding.add("country");
    assertEquals(partitionedEmbedding, names(contract(UnstructuredMode.EMBEDDING, withPartition)));
  }

  /** On an existing table, a model added later puts its VECTOR after the table's columns, a customer's column included. */
  @Test
  void existingTableWithAModelAddedLater() {
    StructType table = contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1"))
        .add(DataTypes.createStructField("ingest_note", DataTypes.StringType, true))
        .add(DataTypes.createStructField("_hoodie_commit_time", DataTypes.StringType, true));
    StructType contract = UnstructuredSchemaContract.contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1", "m2"),
        Option.of(table));
    List<String> expected = new ArrayList<>(EmbeddingTableSchema.BASE_COLUMNS);
    expected.addAll(Arrays.asList("emb_m1", "ingest_note", "emb_m2"));
    assertEquals(expected, names(contract));
    check(UnstructuredMode.EMBEDDING, contract, contract);
  }

  /**
   * Every declared model's column is reserved, on a new table and on an existing one that does not have it yet: a declared model
   * that is not active today is the table's model column once it is activated, never a customer's VECTOR or BLOB.
   */
  @Test
  void declaredModelColumnIsReservedOnAnExistingTableThatLacksIt() {
    TypedProperties declared = embedding("prep_and_compute", "m2", "m1");
    StructType table = contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1"));
    StructType contract = UnstructuredSchemaContract.contract(UnstructuredMode.EMBEDDING, declared, Option.of(table));
    StructType newTable = contract(UnstructuredMode.EMBEDDING, declared);
    HoodieSchemaException refused = assertThrows(HoodieSchemaException.class, () -> UnstructuredSchemaContract.checkOutput(
        UnstructuredMode.EMBEDDING, contract, newTable, contract.add(vector("emb_m2", 8)), "the rows", false));
    assertTrue(refused.getMessage().contains("the column emb_m2 has a reserved name"), refused.getMessage());
    assertThrows(HoodieSchemaException.class, () -> UnstructuredSchemaContract.checkOutput(UnstructuredMode.EMBEDDING, contract, newTable,
        contract.add(blob("EMB_M2")), "the rows", false));
    // a customer VECTOR or BLOB of another name is the customer's
    UnstructuredSchemaContract.checkOutput(UnstructuredMode.EMBEDDING, contract, newTable, contract.add(vector("user_vector", 8))
        .add(blob("user_blob")), "the rows", false);
  }

  @Test
  void compatibleSchemaWithExtrasPassesInAnyOrder() {
    for (UnstructuredMode mode : UnstructuredMode.values()) {
      StructType contract = contract(mode, mode == UnstructuredMode.BLOB ? new TypedProperties() : embedding("prep_and_compute", "m1"));
      check(mode, contract, contract);
      List<StructField> reversed = new ArrayList<>(Arrays.asList(contract.fields()));
      Collections.reverse(reversed);
      reversed.add(1, DataTypes.createStructField("ingest_note", DataTypes.StringType, true));
      reversed.add(DataTypes.createStructField("tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
      // a VECTOR and a BLOB of the user's own are extra columns
      reversed.add(vector("user_vector", 3));
      StructField blobType = contract(UnstructuredMode.BLOB, new TypedProperties()).apply("blob");
      reversed.add(new StructField("attachment", blobType.dataType(), true, blobType.metadata()));
      check(mode, contract, new StructType(reversed.toArray(new StructField[0])));
      // a non-null contract column declared nullable is a widening
      StructType nullableKey = replace(contract, DataTypes.createStructField("source_path_sha", DataTypes.StringType, true));
      check(mode, contract, nullableKey);
    }
  }

  @Test
  void eachRuleRefuses() {
    StructType blob = contract(UnstructuredMode.BLOB, new TypedProperties());
    UnstructuredMode b = UnstructuredMode.BLOB;
    assertEquals("UnstructuredDataSource (mode blob) refuses the target schema: column blob must be "
        + describe(blob, "blob") + ", found: no such column", refusal(b, blob, without(blob, "blob")));
    assertTrue(refusal(b, blob, replace(blob, DataTypes.createStructField("source_file_size", DataTypes.StringType, false)))
        .contains("column source_file_size must be bigint, found: string"));
    StructField plainBlob = new StructField("blob", blob.apply("blob").dataType(), true, Metadata.empty());
    assertTrue(refusal(b, blob, replace(blob, plainBlob)).contains("column blob must be"));
    assertTrue(refusal(b, blob, replace(blob, DataTypes.createStructField("content_type", DataTypes.StringType, false)))
        .contains("column content_type must be string nullable, found: string non-null"));

    StructType compute = contract(UnstructuredMode.EMBEDDING, embedding("prep_and_compute", "m1"));
    UnstructuredMode e = UnstructuredMode.EMBEDDING;
    assertTrue(refusal(e, compute, replace(compute, vector("emb_m1", 8))).contains("column emb_m1 must be array<float> (VECTOR(4))"),
        "a VECTOR of another size");
    // only the declared models' columns are model columns: any other VECTOR is a column of the user's
    StructType prepOnly = contract(e, embedding("prep_only"));
    check(e, prepOnly, prepOnly.add(vector("emb_m1", 4)));
    check(e, compute, compute.add(vector("user_vector", 4)));
    assertTrue(refusal(e, compute, compute.add(vector("EMB_M1", 4))).contains("the column EMB_M1 has a reserved name"),
        "a declared model's column, ignoring case");
    assertTrue(refusal(b, blob, blob.add(DataTypes.createStructField("note", DataTypes.StringType, false)))
        .contains("the extra column note is non-null"));
    for (String reserved : new String[] {"BLOB", "Source_Path_Sha", "_hoodie_x", "_corrupt_record"}) {
      assertTrue(refusal(b, blob, blob.add(DataTypes.createStructField(reserved, DataTypes.StringType, true)))
          .contains("the column " + reserved + " has a reserved name"), reserved);
    }
  }

  private static String describe(StructType schema, String column) {
    StructField field = schema.apply(column);
    String type = field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD) ? " (" + field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD) + ")" : "";
    return field.dataType().simpleString() + type;
  }

  @Test
  void checkOutputRequiresTheCorruptRecordColumnWhenAsked() {
    StructType blob = contract(UnstructuredMode.BLOB, new TypedProperties());
    UnstructuredSchemaContract.checkOutput(UnstructuredMode.BLOB, blob, blob, blob, "the rows", false);
    HoodieSchemaException missing = assertThrows(HoodieSchemaException.class,
        () -> UnstructuredSchemaContract.checkOutput(UnstructuredMode.BLOB, blob, blob, blob, "the rows", true));
    assertTrue(missing.getMessage().contains("it has no _corrupt_record column"), missing.getMessage());
    UnstructuredSchemaContract.checkOutput(UnstructuredMode.BLOB, blob, blob,
        blob.add(DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)), "the rows", true);
    assertEquals(Collections.emptyList(), Arrays.stream(blob.fieldNames()).filter(n -> n.startsWith("_")).collect(Collectors.toList()));
  }
}
