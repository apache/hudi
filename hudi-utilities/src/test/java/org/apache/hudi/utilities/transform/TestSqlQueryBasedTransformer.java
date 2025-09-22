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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSqlQueryBasedTransformer {

  @Test
  public void testSqlQuery() throws IOException {

    SparkSession spark = SparkSession
        .builder()
        .config(getSparkConfForTest(TestSqlQueryBasedTransformer.class.getName()))
        .getOrCreate();

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    // prepare test data
    String testData = "{\n"
        + "  \"ts\": 1622126968000,\n"
        + "  \"uuid\": \"c978e157-72ee-4819-8f04-8e46e1bb357a\",\n"
        + "  \"rider\": \"rider-213\",\n"
        + "  \"driver\": \"driver-213\",\n"
        + "  \"begin_lat\": 0.4726905879569653,\n"
        + "  \"begin_lon\": 0.46157858450465483,\n"
        + "  \"end_lat\": 0.754803407008858,\n"
        + "  \"end_lon\": 0.9671159942018241,\n"
        + "  \"fare\": 34.158284716382845,\n"
        + "  \"partitionpath\": \"americas/brazil/sao_paulo\"\n"
        + "}";

    JavaRDD<String> testRdd = jsc.parallelize(Collections.singletonList(testData), 2);
    Dataset<Row> ds = spark.read().json(testRdd);

    // create a new column dt, whose value is transformed from ts, format is yyyyMMdd
    String transSql = "select\n"
        + "\tuuid,\n"
        + "\tbegin_lat,\n"
        + "\tbegin_lon,\n"
        + "\tdriver,\n"
        + "\tend_lat,\n"
        + "\tend_lon,\n"
        + "\tfare,\n"
        + "\tpartitionpath,\n"
        + "\trider,\n"
        + "\tts,\n"
        + "\tFROM_UNIXTIME(ts / 1000, 'yyyyMMdd') as dt\n"
        + "from\n"
        + "\t<SRC>";
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.transformer.sql", transSql);

    // transform
    SqlQueryBasedTransformer transformer = new SqlQueryBasedTransformer();

    // test if the class throws illegal argument exception when sql-config is missing
    assertThrows(IllegalArgumentException.class, () -> transformer.apply(jsc, spark, ds, new TypedProperties()));

    Dataset<Row> result = transformer.apply(jsc, spark, ds, props);

    // check result
    assertEquals(11, result.columns().length);
    assertNotNull(result.col("dt"));
    assertEquals("20210527", result.first().get(10).toString());

    spark.close();
  }
}
