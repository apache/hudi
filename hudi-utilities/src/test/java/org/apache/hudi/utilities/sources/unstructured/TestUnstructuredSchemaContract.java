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
  }

  @Test
  void compatibleSchemaWithExtrasPassesInAnyOrder() {
    for (UnstructuredMode mode : UnstructuredMode.values()) {
      StructType contract = contract(mode, new TypedProperties());
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
