/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.EmbeddingTransformerConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding concurrency cap is what keeps one endpoint from being offered
 * (concurrent tasks x max.inflight.requests) requests at once.
 */
public class TestEmbeddingConcurrencyCap {

  private static SparkSession spark;

  /** Records how many callers are inside embed() at the same time. */
  public static class ConcurrencyRecordingProvider implements EmbeddingProvider {
    static final AtomicInteger ACTIVE = new AtomicInteger();
    static final AtomicInteger PEAK = new AtomicInteger();

    @Override
    public List<float[]> embed(List<String> texts) {
      int now = ACTIVE.incrementAndGet();
      PEAK.accumulateAndGet(now, Math::max);
      try {
        // hold the permit long enough that every partition reaches embed() and contends
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        ACTIVE.decrementAndGet();
      }
      List<float[]> vectors = new ArrayList<>(texts.size());
      for (int i = 0; i < texts.size(); i++) {
        vectors.add(new float[] {1.0f, 2.0f});
      }
      return vectors;
    }
  }

  @BeforeAll
  static void startSpark() {
    spark = SparkSession.builder().appName("embedding-concurrency-cap")
        .master("local[8]").config("spark.ui.enabled", "false").getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void concurrentRequestsNeverExceedTheConfiguredCap() {
    int cap = 2;
    ConcurrencyRecordingProvider.ACTIVE.set(0);
    ConcurrencyRecordingProvider.PEAK.set(0);

    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTransformerConfig.PROVIDER_CLASS.key(),
        ConcurrencyRecordingProvider.class.getName());
    props.setProperty(EmbeddingTransformerConfig.SOURCE_COLUMN.key(), "text");
    props.setProperty(EmbeddingTransformerConfig.TARGET_COLUMN.key(), "embedding");
    props.setProperty(EmbeddingTransformerConfig.DIMENSION.key(), "2");
    props.setProperty(EmbeddingTransformerConfig.BATCH_SIZE.key(), "1");
    // 8 partitions x 4 staged batches each: without a shared cap this offers 32 at once
    props.setProperty(EmbeddingTransformerConfig.MAX_INFLIGHT_REQUESTS.key(), "4");
    props.setProperty(EmbeddingTransformerConfig.MAX_CONCURRENT_REQUESTS.key(), String.valueOf(cap));

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      rows.add(RowFactory.create("text " + i));
    }
    StructType schema = new StructType(new StructField[] {
        new StructField("text", DataTypes.StringType, true, org.apache.spark.sql.types.Metadata.empty())});
    Dataset<Row> input = spark.createDataFrame(rows, schema).repartition(8);

    Dataset<Row> out = new EmbeddingTransformer().apply(
        org.apache.spark.api.java.JavaSparkContext.fromSparkContext(spark.sparkContext()),
        spark, input, props);
    assertEquals(64, out.count());

    int peak = ConcurrencyRecordingProvider.PEAK.get();
    assertTrue(peak > 0, "the provider was never called");
    // 8 partitions each staging 4 batches would offer 32 at once without a shared cap, so a
    // peak at or below 2 is only reachable if every partition draws on the same semaphore
    assertTrue(peak <= cap,
        "at most " + cap + " requests should be in flight across the JVM, saw " + peak);
  }

}
