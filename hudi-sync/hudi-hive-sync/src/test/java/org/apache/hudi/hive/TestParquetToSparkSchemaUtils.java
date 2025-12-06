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

package org.apache.hudi.hive;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.sync.common.util.Parquet2SparkSchemaUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestParquetToSparkSchemaUtils {
  private final SparkToParquetSchemaConverter sparkToParquetConverter =
          new SparkToParquetSchemaConverter(new SQLConf());
  private final SparkSqlParser parser = createSqlParser();

  private static SparkSqlParser createSqlParser() {
    try {
      return SparkSqlParser.class.getDeclaredConstructor(SQLConf.class).newInstance(new SQLConf());
    } catch (Exception ne) {
      try { // For spark 3.1, there is no constructor with SQLConf, use the default constructor
        return SparkSqlParser.class.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testConvertBasicTypes() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int NOT NULL, f1 string NOT NULL, f2 bigint NOT NULL, f3 float, f4 double, f5 boolean, f6 binary, f7 binary NOT NULL,"
                    + " f8 decimal(5,2) NOT NULL, f9 decimal(5,2), f10 timestamp, f11 timestamp NOT NULL, f12 timestamp_ntz NOT NULL, f13 timestamp_ntz,"
                    + " f14 date NOT NULL, f15 int NOT NULL, f16 bigint NOT NULL, f17 string NOT NULL, f18 string");

    Schema schemaWithSupportedTypes =
        Schema.createRecord("root", null, null, false, Arrays.asList(
            new Schema.Field("f0", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("f1", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("f2", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("f3", AvroSchemaUtils.createNullableSchema(Schema.Type.FLOAT), null, null),
            new Schema.Field("f4", AvroSchemaUtils.createNullableSchema(Schema.Type.DOUBLE), null, null),
            new Schema.Field("f5", AvroSchemaUtils.createNullableSchema(Schema.Type.BOOLEAN), null, null),
            new Schema.Field("f6", AvroSchemaUtils.createNullableSchema(Schema.Type.BYTES), null, null),
            new Schema.Field("f7", Schema.createFixed("fixed", null, null, 10), null, null),
            new Schema.Field("f8", LogicalTypes.decimal(5, 2).addToSchema(Schema.createFixed("decimal", null, null, 16)), null, null),
            new Schema.Field("f9", AvroSchemaUtils.createNullableSchema(LogicalTypes.decimal(5, 2).addToSchema(Schema.create(Schema.Type.BYTES))), null, null),
            new Schema.Field("f10", AvroSchemaUtils.createNullableSchema(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))), null, null),
            new Schema.Field("f11", LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)), null, null),
            new Schema.Field("f12", LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), null, null),
            new Schema.Field("f13", AvroSchemaUtils.createNullableSchema(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))), null, null),
            new Schema.Field("f14", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), null, null),
            new Schema.Field("f15", LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)), null, null),
            new Schema.Field("f16", LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), null, null),
            new Schema.Field("f17", Schema.createEnum("enum", null, null, Arrays.asList("A", "B", "C")), null, null),
            new Schema.Field("f18", AvroSchemaUtils.createNullableSchema(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))), null, null)
        ));
    MessageType parquetSchema = HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(new Configuration()).convert(schemaWithSupportedTypes);
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema);
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
  }

  @Test
  public void testConvertComplexType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 map<string, int>, f2 array<decimal(10,2)>"
                    + ",f3 map<string, struct<nested: struct<double_nested: int>>>, f4 array<array<double>>"
                    + ",f5 struct<id:int, name:string>");
    Schema schemaWithSupportedTypes =
        Schema.createRecord("root", null, null, false, Arrays.asList(
            new Schema.Field("f0", AvroSchemaUtils.createNullableSchema(Schema.Type.INT), null, null),
            new Schema.Field("f1", AvroSchemaUtils.createNullableSchema(Schema.createMap(AvroSchemaUtils.createNullableSchema(Schema.Type.INT))), null, null),
            new Schema.Field("f2", AvroSchemaUtils.createNullableSchema(Schema.createArray(
                    LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES)))), null, null),
            new Schema.Field("f3", AvroSchemaUtils.createNullableSchema(Schema.createMap(AvroSchemaUtils.createNullableSchema(Schema.createRecord(
                    "struct", null, null, false, Collections.singletonList(
                        new Schema.Field("nested", AvroSchemaUtils.createNullableSchema(Schema.createRecord(
                            "double_nested_struct", null, null, false, Collections.singletonList(
                                new Schema.Field("double_nested", AvroSchemaUtils.createNullableSchema(Schema.Type.INT), null, null)))), null, null)))))), null, null),
            new Schema.Field("f4", AvroSchemaUtils.createNullableSchema(Schema.createArray(Schema.createArray(Schema.create(Schema.Type.DOUBLE)))), null, null),
            new Schema.Field("f5", AvroSchemaUtils.createNullableSchema(Schema.createRecord("struct",
                    null, null, false, Arrays.asList(
                    new Schema.Field("id", AvroSchemaUtils.createNullableSchema(Schema.Type.INT), null, null),
                    new Schema.Field("name", AvroSchemaUtils.createNullableSchema(Schema.Type.STRING), null, null)
            ))), null, null)));
    MessageType parquetSchema = HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(new Configuration()).convert(schemaWithSupportedTypes);
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema);
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test complex type with nullable
    StructField field0 = new StructField("f0", new ArrayType(StringType$.MODULE$, true), false, Metadata.empty());
    StructField field1 = new StructField("f1", new MapType(StringType$.MODULE$, IntegerType$.MODULE$, true), false, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    String sparkSchemaWithNullableJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            sparkToParquetConverter.convert(sparkSchemaWithNullable).asGroupType());
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }
}
