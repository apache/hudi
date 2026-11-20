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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.utilities.unstructured.embedding.NonVectorizedParquetReads.VECTORIZED_READER_CONF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link NonVectorizedParquetReads} turns the vectorized Parquet reader off for its work and restores the session setting after. */
public class TestNonVectorizedParquetReads extends SparkClientFunctionalTestHarness {

  @AfterEach
  void unsetReaderConf() {
    spark().conf().unset(VECTORIZED_READER_CONF);
  }

  @Test
  void restoresASetValue() {
    SparkSession spark = spark();
    spark.conf().set(VECTORIZED_READER_CONF, "true");
    String during = NonVectorizedParquetReads.run(spark, () -> spark.conf().get(VECTORIZED_READER_CONF));
    assertEquals("false", during);
    assertTrue(spark.conf().contains(VECTORIZED_READER_CONF), "a set value is restored, not unset");
    assertEquals("true", spark.conf().get(VECTORIZED_READER_CONF));
  }

  @Test
  void unsetsAValueThatWasUnset() {
    SparkSession spark = spark();
    spark.conf().unset(VECTORIZED_READER_CONF);
    assertEquals("false", NonVectorizedParquetReads.run(spark, () -> spark.conf().get(VECTORIZED_READER_CONF)));
    assertFalse(spark.conf().contains(VECTORIZED_READER_CONF));
  }

  @Test
  void restoresWhenTheWorkThrows() {
    SparkSession spark = spark();
    spark.conf().set(VECTORIZED_READER_CONF, "true");
    assertThrows(IllegalStateException.class, () -> NonVectorizedParquetReads.run(spark, () -> {
      throw new IllegalStateException("work failed");
    }));
    assertTrue(spark.conf().contains(VECTORIZED_READER_CONF), "a set value is restored, not unset");
    assertEquals("true", spark.conf().get(VECTORIZED_READER_CONF));
  }
}
