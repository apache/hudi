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

import org.apache.hudi.sync.common.util.Parquet2SparkSchemaUtils;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestParquet2SparkSchemaUtils {
  private final SparkToParquetSchemaConverter spark2ParquetConverter =
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
  public void testConvertPrimitiveType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 string, f3 bigint,"
                    + " f4 decimal(5,2), f5 timestamp, f6 date,"
                    + " f7 short, f8 float, f9 double, f10 byte,"
                    + " f11 tinyint, f12 smallint, f13 binary, f14 boolean");

    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test type with nullable
    StructField field0 = new StructField("f0", StringType$.MODULE$, false, Metadata.empty());
    StructField field1 = new StructField("f1", StringType$.MODULE$, true, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    String sparkSchemaWithNullableJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchemaWithNullable).asGroupType());
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }

  @Test
  public void testConvertComplexType() {
    StructType sparkSchema = parser.parseTableSchema(
            "f0 int, f1 map<string, int>, f2 array<decimal(10,2)>"
                    + ",f3 map<array<date>, bigint>, f4 array<array<double>>"
                    + ",f5 struct<id:int, name:string>");
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchema).asGroupType());
    StructType convertedSparkSchema = (StructType) StructType.fromJson(sparkSchemaJson);
    assertEquals(sparkSchema.json(), convertedSparkSchema.json());
    // Test complex type with nullable
    StructField field0 = new StructField("f0", new ArrayType(StringType$.MODULE$, true), false, Metadata.empty());
    StructField field1 = new StructField("f1", new MapType(StringType$.MODULE$, IntegerType$.MODULE$, true), false, Metadata.empty());
    StructType sparkSchemaWithNullable = new StructType(new StructField[]{field0, field1});
    String sparkSchemaWithNullableJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(
            spark2ParquetConverter.convert(sparkSchemaWithNullable).asGroupType());
    StructType convertedSparkSchemaWithNullable = (StructType) StructType.fromJson(sparkSchemaWithNullableJson);
    assertEquals(sparkSchemaWithNullable.json(), convertedSparkSchemaWithNullable.json());
  }
}
