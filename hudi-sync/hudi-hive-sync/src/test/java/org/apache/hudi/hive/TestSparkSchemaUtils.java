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
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType$;
import org.apache.spark.sql.types.TimestampType$;
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
    // Build expected schema programmatically to avoid SQL parser limitations
    // (timestamp_ntz is not supported by the SQL parser in Spark 3.3)
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("f0", IntegerType$.MODULE$, false, Metadata.empty()),
        new StructField("f1", StringType$.MODULE$, false, Metadata.empty()),
        new StructField("f2", LongType$.MODULE$, false, Metadata.empty()),
        new StructField("f3", FloatType$.MODULE$, true, Metadata.empty()),
        new StructField("f4", DoubleType$.MODULE$, true, Metadata.empty()),
        new StructField("f5", BooleanType$.MODULE$, true, Metadata.empty()),
        new StructField("f6", BinaryType$.MODULE$, true, Metadata.empty()),
        new StructField("f7", BinaryType$.MODULE$, false, Metadata.empty()),
        new StructField("f8", DataTypes.createDecimalType(5, 2), false, Metadata.empty()),
        new StructField("f9", DataTypes.createDecimalType(5, 2), true, Metadata.empty()),
        new StructField("f10", TimestampType$.MODULE$, true, Metadata.empty()),
        new StructField("f11", TimestampType$.MODULE$, false, Metadata.empty()),
        new StructField("f12", TimestampNTZType$.MODULE$, false, Metadata.empty()),
        new StructField("f13", TimestampNTZType$.MODULE$, true, Metadata.empty()),
        new StructField("f14", DateType$.MODULE$, false, Metadata.empty()),
        new StructField("f15", IntegerType$.MODULE$, false, Metadata.empty()),
        new StructField("f16", LongType$.MODULE$, false, Metadata.empty()),
        new StructField("f17", StringType$.MODULE$, false, Metadata.empty()),
        new StructField("f18", StringType$.MODULE$, true, Metadata.empty())
    });

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
