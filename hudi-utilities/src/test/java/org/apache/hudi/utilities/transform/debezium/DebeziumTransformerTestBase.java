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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;

/**
 * Lightweight Spark test base for the Debezium transformer tests. Spins up a single local
 * SparkSession for the test class and parses inline JSON fixtures into {@link Dataset}s that mimic
 * Debezium change-event envelopes.
 */
public abstract class DebeziumTransformerTestBase {

  protected static SparkSession spark;
  protected static JavaSparkContext jsc;

  @BeforeAll
  static void setUpSpark() throws IOException {
    spark = SparkSession.builder()
        .config(getSparkConfForTest(DebeziumTransformerTestBase.class.getName()))
        .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  static void tearDownSpark() throws IOException {
    if (spark != null) {
      spark.close();
      spark = null;
      jsc = null;
    }
  }

  /**
   * Parses each argument as a single JSON document into a {@link Dataset}. Passing several
   * envelopes together lets Spark infer struct types for {@code before}/{@code after} even when an
   * individual envelope leaves one of them null (inserts have no {@code before}, deletes have no
   * {@code after}).
   */
  protected Dataset<Row> jsonToDataset(String... jsonDocs) {
    return spark.read().json(jsc.parallelize(Arrays.asList(jsonDocs), 1));
  }
}
