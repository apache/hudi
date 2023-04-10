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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGenericRddTransform extends SparkClientFunctionalTestHarness {
  @Test
  public void testGenericRddTransform() {
    Dataset ds = spark().range(10).withColumn("null_check_col", when(expr("id % 2 == 0"),
        lit("test")).otherwise(lit(null)));
    StructType structType = new StructType(new StructField[] {
        new StructField("id", DataTypes.StringType, false, Metadata.empty()),
        new StructField("null_check_col", DataTypes.StringType, false, Metadata.empty())});
    Schema nonNullSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType,"record","record");
    Tuple2<RDD<GenericRecord>, RDD<String>> failSafeRdds = HoodieSparkUtils.safeCreateRDD(ds, "record",
        "record",false, Option.of(nonNullSchema));
    assertEquals(5, failSafeRdds._1.count());
    assertEquals(5, failSafeRdds._2.count());
  }
}
