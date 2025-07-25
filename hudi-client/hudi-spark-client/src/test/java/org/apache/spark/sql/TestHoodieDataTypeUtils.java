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

package org.apache.spark.sql;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroWriteSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieDataTypeUtils {
  private static final Schema SMALL_DECIMAL_SCHEMA = LogicalTypes.decimal(18, 10).addToSchema(Schema.createFixed("smallDec", null, "org.apache.hudi.test", 9));
  private static final Schema LARGE_DECIMAL_SCHEMA = LogicalTypes.decimal(20, 10).addToSchema(Schema.createFixed("largeDec", null, "org.apache.hudi.test", 9));

  private static Stream<Arguments> canUseRowWriterCases() {

    Schema listSchema = Schema.createArray(Schema.create(Schema.Type.INT));
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));

    Schema schemaWithSmallDecimal = Schema.createRecord("schemaWithSmallDecimal", null, null, false,
        Collections.singletonList(new Schema.Field("smallDecimal", SMALL_DECIMAL_SCHEMA, null, null)));
    Schema schemaWithSmallDecimalAndList = Schema.createRecord("schemaWithSmallDecimalAndList", null, null, false,
        Arrays.asList(new Schema.Field("smallDecimal", SMALL_DECIMAL_SCHEMA, null, null), new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("listField", listSchema, null, null)));
    Schema schemaWithSmallDecimalAndMap = Schema.createRecord("schemaWithSmallDecimalAndMap", null, null, false,
        Arrays.asList(new Schema.Field("smallDecimal", SMALL_DECIMAL_SCHEMA, null, null), new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("mapField", mapSchema, null, null)));
    Schema schemaWithLargeDecimalAndList = Schema.createRecord("schemaWithLargeDecimalAndList", null, null, false,
        Arrays.asList(new Schema.Field("largeDecimal", LARGE_DECIMAL_SCHEMA, null, null), new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("listField", listSchema, null, null)));
    Schema schemaWithLargeDecimalAndMap = Schema.createRecord("schemaWithLargeDecimalAndMap", null, null, false,
        Arrays.asList(new Schema.Field("largeDecimal", LARGE_DECIMAL_SCHEMA, null, null), new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("mapField", mapSchema, null, null)));
    Schema schemaWithoutSpecialTypes = Schema.createRecord("schemaWithInt", null, null, false,
        Collections.singletonList(new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null)));

    Configuration configurationWithLegacyListFormat = new Configuration(false);
    configurationWithLegacyListFormat.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "true");

    Configuration configurationWithoutLegacyListFormat = new Configuration(false);
    configurationWithoutLegacyListFormat.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");

    return Stream.of(
        Arguments.of(schemaWithoutSpecialTypes, configurationWithoutLegacyListFormat, true),
        Arguments.of(schemaWithSmallDecimal, configurationWithoutLegacyListFormat, true),
        Arguments.of(schemaWithLargeDecimalAndMap, configurationWithoutLegacyListFormat, true),
        Arguments.of(schemaWithSmallDecimalAndMap, configurationWithoutLegacyListFormat, false),
        Arguments.of(schemaWithLargeDecimalAndMap, configurationWithoutLegacyListFormat, true),
        Arguments.of(schemaWithSmallDecimalAndList, configurationWithLegacyListFormat, true),
        Arguments.of(schemaWithSmallDecimalAndList, configurationWithoutLegacyListFormat, false),
        Arguments.of(schemaWithLargeDecimalAndList, configurationWithoutLegacyListFormat, true),
        Arguments.of(schemaWithLargeDecimalAndList, configurationWithLegacyListFormat, true));
  }

  @ParameterizedTest
  @MethodSource("canUseRowWriterCases")
  void testCanUseRowWriter(Schema schema, Configuration conf, boolean expected) {
    assertEquals(expected, HoodieDataTypeUtils.canUseRowWriter(schema, conf));
  }

  private static Stream<Arguments> testAutoModifyParquetWriteLegacyFormatParameterParams() {
    return Arrays.stream(new Object[][] {
            {true, null, true}, {false, null, null},
            {true, false, false}, {true, true, true},
            {false, true, true}, {false, false, false}})
        // add useMap option to test both entry-points
        .map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("testAutoModifyParquetWriteLegacyFormatParameterParams")
  void testAutoModifyParquetWriteLegacyFormatParameter(boolean smallDecimal, Boolean propValue, Boolean expectedPropValue) {
    Schema decimalType = smallDecimal ? SMALL_DECIMAL_SCHEMA : LARGE_DECIMAL_SCHEMA;
    Schema schema = Schema.createRecord("test", null, null, false,
        Collections.singletonList(new Schema.Field("decimalField", decimalType, null, null)));
    TypedProperties options = propValue != null
        ? TypedProperties.fromMap(Collections.singletonMap(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key(), String.valueOf(propValue)))
        : new TypedProperties();

    HoodieDataTypeUtils.tryOverrideParquetWriteLegacyFormatProperty(options, schema);
    Boolean finalPropValue =
        Option.ofNullable(options.get(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key()))
            .map(value -> Boolean.parseBoolean(value.toString()))
            .orElse(null);
    assertEquals(expectedPropValue, finalPropValue);
  }
}