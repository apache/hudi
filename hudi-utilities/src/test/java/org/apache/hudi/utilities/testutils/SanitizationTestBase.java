/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.utilities.deltastreamer.TestSourceFormatAdapter;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

public class SanitizationTestBase {

  protected static SparkSession spark;
  protected static JavaSparkContext jsc;

  protected final String invalidCharMask = "__";

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(TestSourceFormatAdapter.class.getName())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  public static void shutdown() {
    jsc.close();
    spark.close();
  }

  private static String sanitizeIfNeeded(String src, boolean shouldSanitize) {
    return shouldSanitize ? HoodieAvroUtils.sanitizeName(src, "__") : src;
  }

  protected static StructType getSchemaWithProperNaming() {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField("state", DataTypes.StringType, true, Metadata.empty()),
        new StructField("street", DataTypes.StringType, true, Metadata.empty()),
        new StructField("zip", DataTypes.LongType, true, Metadata.empty()),
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField("address", addressStruct, true, Metadata.empty()),
        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
        new StructField("occupation", DataTypes.StringType, true, Metadata.empty()),
        new StructField("place", DataTypes.StringType, true, Metadata.empty())
    });
    return personStruct;
  }

  protected static StructType getSchemaWithBadAvroNamingForStructType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@_addr*$ess", shouldSanitize),
            addressStruct, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("9name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("_occu9pation", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@plac.e.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty())
    });
    return personStruct;
  }

  protected static StructType getSchemaWithBadAvroNamingForArrayType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@arr@", shouldSanitize),
            new ArrayType(addressStruct, true), true, Metadata.empty())
    });
    return personStruct;
  }

  protected static StructType getSchemaWithBadAvroNamingForMapType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@map9", shouldSanitize),
            new MapType(DataTypes.StringType, addressStruct, true), true, Metadata.empty()),
    });
    return personStruct;
  }

  public static Schema generateProperFormattedSchema() {
    Schema addressSchema = SchemaBuilder.record("Address").fields()
        .requiredString("streetaddress")
        .requiredString("city")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("firstname")
        .requiredString("lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  public static Schema generateRenamedSchemaWithDefaultReplacement() {
    Schema addressSchema = SchemaBuilder.record("__Address").fields()
        .nullableString("__stree9add__ress", "@@@any_address")
        .requiredString("cit__y__")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("__firstname")
        .requiredString("__lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  public static Schema generateRenamedSchemaWithConfiguredReplacement() {
    Schema addressSchema = SchemaBuilder.record("_Address").fields()
        .nullableString("_stree9add_ress", "@@@any_address")
        .requiredString("cit_y_")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("_firstname")
        .requiredString("_lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  protected static Stream<Arguments> provideDataFiles() {
    return Stream.of(
        Arguments.of("src/test/resources/data/avro_sanitization.json", "src/test/resources/data/avro_sanitization.json",
            getSchemaWithProperNaming(), getSchemaWithProperNaming()),
        Arguments.of("src/test/resources/data/avro_sanitization_bad_naming_in.json", "src/test/resources/data/avro_sanitization_bad_naming_out.json",
            getSchemaWithBadAvroNamingForStructType(false), getSchemaWithBadAvroNamingForStructType(true)),
        Arguments.of("src/test/resources/data/avro_sanitization_bad_naming_nested_array_in.json", "src/test/resources/data/avro_sanitization_bad_naming_nested_array_out.json",
            getSchemaWithBadAvroNamingForArrayType(false), getSchemaWithBadAvroNamingForArrayType(true)),
        Arguments.of("src/test/resources/data/avro_sanitization_bad_naming_nested_map_in.json", "src/test/resources/data/avro_sanitization_bad_naming_nested_map_out.json",
            getSchemaWithBadAvroNamingForMapType(false), getSchemaWithBadAvroNamingForMapType(true))
    );
  }
}
