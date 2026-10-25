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

package org.apache.hudi.examples.common;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.InputBatch;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for RandomJsonSource.
 */
class TestRandomJsonSource {

  private static JavaSparkContext jsc;
  private static SparkSession spark;

  @BeforeAll
  static void initSpark() {
    spark = SparkSession.builder().master("local[1]").appName("TestRandomJsonSource").getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterAll
  static void stopSpark() {
    if (jsc != null) {
      jsc.stop();
    }
    if (spark != null) {
      spark.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {6, 8})
  void testFetchNextReturnsTwentyRecordsWithCorrectCheckpointVersion(int writeTableVersion) {
    TypedProperties props = new TypedProperties();
    props.setProperty(WRITE_TABLE_VERSION.key(), String.valueOf(writeTableVersion));

    RandomJsonSource source = new RandomJsonSource(props, jsc, spark, null);
    InputBatch<JavaRDD<String>> batch = source.fetchNext(Option.empty(), Long.MAX_VALUE);

    assertNotNull(batch.getBatch());
    assertEquals(20, batch.getBatch().get().count());
    assertNotNull(batch.getCheckpointForNextBatch());
    assertEquals(StreamerCheckpointV1.class, batch.getCheckpointForNextBatch().getClass());
  }
}
