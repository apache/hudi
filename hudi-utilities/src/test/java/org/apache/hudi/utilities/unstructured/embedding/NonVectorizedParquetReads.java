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

import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

/** Test helper: runs a block of work with Spark's vectorized Parquet reader off, then restores the session setting. */
public final class NonVectorizedParquetReads {

  /** Spark session conf of the vectorized parquet reader, false while {@link #run} runs its work. */
  public static final String VECTORIZED_READER_CONF = "spark.sql.parquet.enableVectorizedReader";

  private NonVectorizedParquetReads() {
  }

  /** Runs {@code work} with {@value #VECTORIZED_READER_CONF} false, then restores the session's previous setting. */
  public static <T> T run(SparkSession spark, Supplier<T> work) {
    boolean wasSet = spark.conf().contains(VECTORIZED_READER_CONF);
    String before = wasSet ? spark.conf().get(VECTORIZED_READER_CONF) : null;
    spark.conf().set(VECTORIZED_READER_CONF, "false");
    try {
      return work.get();
    } finally {
      if (wasSet) {
        spark.conf().set(VECTORIZED_READER_CONF, before);
      } else {
        spark.conf().unset(VECTORIZED_READER_CONF);
      }
    }
  }
}
