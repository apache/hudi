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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.sync.common.util.SparkSchemaUtils;

import org.apache.spark.sql.execution.SparkSqlParser;
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

public class TestSparkSchemaUtils {
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

    HoodieSchema schemaWithSupportedTypes =
        HoodieSchema.createRecord("root", null, null, false, Arrays.asList(
            HoodieSchemaField.of("f0", HoodieSchema.create(HoodieSchemaType.INT), null, null),
            HoodieSchemaField.of("f1", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
            HoodieSchemaField.of("f2", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
            HoodieSchemaField.of("f3", HoodieSchema.createNullable(HoodieSchemaType.FLOAT), null, null),
            HoodieSchemaField.of("f4", HoodieSchema.createNullable(HoodieSchemaType.DOUBLE), null, null),
            HoodieSchemaField.of("f5", HoodieSchema.createNullable(HoodieSchemaType.BOOLEAN), null, null),
            HoodieSchemaField.of("f6", HoodieSchema.createNullable(HoodieSchemaType.BYTES), null, null),
            HoodieSchemaField.of("f7", HoodieSchema.createFixed("fixed", null, null, 10), null, null),
            HoodieSchemaField.of("f8", HoodieSchema.createDecimal("decimal", null, null, 5, 2, 16), null, null),
            HoodieSchemaField.of("f9", HoodieSchema.createNullable(HoodieSchema.createDecimal(5, 2)), null, null),
            HoodieSchemaField.of("f10", HoodieSchema.createNullable(HoodieSchema.createTimestampMicros()), null, null),
            HoodieSchemaField.of("f11", HoodieSchema.createTimestampMillis(), null, null),
            HoodieSchemaField.of("f12", HoodieSchema.createLocalTimestampMicros(), null, null),
            HoodieSchemaField.of("f13", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis()), null, null),
            HoodieSchemaField.of("f14", HoodieSchema.createDate(), null, null),
            HoodieSchemaField.of("f15", HoodieSchema.createTimeMillis(), null, null),
            HoodieSchemaField.of("f16", HoodieSchema.createTimeMicros(), null, null),
            HoodieSchemaField.of("f17", HoodieSchema.createEnum("enum", null, null, Arrays.asList("A", "B", "C")), null, null),
            HoodieSchemaField.of("f18", HoodieSchema.createNullable(HoodieSchema.createUUID()), null, null)
        ));
    String sparkSchemaJson = SparkSchemaUtils.convertToSparkSchemaJson(schemaWithSupportedTypes);
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
  }

  @Test
  public void testConvertComplexType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 map<string, int>, f2 array<decimal(10,2)>"
                    + ",f3 map<string, struct<nested: struct<double_nested: int>>>, f4 array<array<double>>"
                    + ",f5 struct<id:int, name:string>");
    HoodieSchema schemaWithSupportedTypes =
        HoodieSchema.createRecord("root", null, null, false, Arrays.asList(
            HoodieSchemaField.of("f0", HoodieSchema.createNullable(HoodieSchemaType.INT), null, null),
            HoodieSchemaField.of("f1", HoodieSchema.createNullable(HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchemaType.INT))), null, null),
            HoodieSchemaField.of("f2", HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.createDecimal(10, 2)))), null, null),
            HoodieSchemaField.of("f3", HoodieSchema.createNullable(HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchema.createRecord(
                    "struct", null, null, false, Collections.singletonList(
                        HoodieSchemaField.of("nested", HoodieSchema.createNullable(HoodieSchema.createRecord(
                            "double_nested_struct", null, null, false, Collections.singletonList(
                                HoodieSchemaField.of("double_nested", HoodieSchema.createNullable(HoodieSchemaType.INT), null, null)))), null, null)))))), null, null),
            HoodieSchemaField.of("f4", HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.createArray(
                HoodieSchema.createNullable(HoodieSchemaType.DOUBLE))))), null, null),
            HoodieSchemaField.of("f5", HoodieSchema.createNullable(HoodieSchema.createRecord("struct",
                    null, null, false, Arrays.asList(
                    HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.INT), null, null),
                    HoodieSchemaField.of("name", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, null)
            ))), null, null)));
    String sparkSchemaJson = SparkSchemaUtils.convertToSparkSchemaJson(schemaWithSupportedTypes);
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test complex type with nullable
    StructField field0 = new StructField("f0", new ArrayType(StringType$.MODULE$, true), false, Metadata.empty());
    StructField field1 = new StructField("f1", new MapType(StringType$.MODULE$, IntegerType$.MODULE$, true), false, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    HoodieSchema schemaWithNullable = HoodieSchema.createRecord("root", null, null, false, Arrays.asList(
        HoodieSchemaField.of("f0", HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchemaType.STRING)), null, null),
        HoodieSchemaField.of("f1",HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchemaType.INT)), null, null)
    ));
    String sparkSchemaWithNullableJson = SparkSchemaUtils.convertToSparkSchemaJson(schemaWithNullable);
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }
}
