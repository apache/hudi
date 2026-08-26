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

package org.apache.hudi.functional;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.ExecutorMetricRegistry;
import org.apache.hudi.metrics.RecordIndexMetricNames;
import org.apache.hudi.testutils.CapturingMetricsReporter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOT FOR COMMIT. Runs the record index lookup against real forked executor JVMs, which is the only
 * configuration that actually exercises the mechanism: under local[*] the driver and executors share one
 * JVM, so the registry bundle is never serialised and the accumulator merge is a no-op.
 */
public class ScratchRliForkedExecutorVerification {

  private static final long SEED_ROWS = Long.getLong("scratch.seedRows", 400_000L);
  private static final long UPDATE_ROWS = Long.getLong("scratch.updateRows", 100_000L);
  private static final long INSERT_ROWS = Long.getLong("scratch.insertRows", 100_000L);
  private static final String PAYLOAD = String.join("", java.util.Collections.nCopies(512, "x"));

  private static Dataset<Row> gen(SparkSession spark, long from, long count) {
    return spark.range(from, from + count)
        .withColumn("_row_key", concat(lit("key-"), col("id")))
        .withColumn("partition", concat(lit("p"), col("id").mod(4)))
        .withColumn("ts", col("id"))
        .withColumn("payload", lit(PAYLOAD))
        .drop("id");
  }

  private static Map<String, String> opts(String op, String basePath) {
    Map<String, String> m = new HashMap<>();
    m.put("hoodie.datasource.write.recordkey.field", "_row_key");
    m.put("hoodie.datasource.write.partitionpath.field", "partition");
    m.put("hoodie.datasource.write.precombine.field", "ts");
    m.put("hoodie.datasource.write.operation", op);
    m.put(HoodieWriteConfig.TBL_NAME.key(), "rli_forked_exec");
    m.put(HoodieMetadataConfig.ENABLE.key(), "true");
    m.put(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    m.put(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "false");
    m.put(HoodieIndexConfig.INDEX_TYPE.key(), "GLOBAL_RECORD_LEVEL_INDEX");
    m.put(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true");
    m.put(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), "INMEMORY");
    m.put(HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME.key(), CapturingMetricsReporter.class.getName());
    m.put("hoodie.insert.shuffle.parallelism", "8");
    m.put("hoodie.upsert.shuffle.parallelism", "8");
    m.put("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    return m;
  }

  private static Map<String, Long> rliCounters() {
    String marker = ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction()
        + "." + ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier() + ".";
    Map<String, Long> out = new HashMap<>();
    CapturingMetricsReporter.captured().forEach((name, value) -> {
      int at = name.indexOf(marker);
      if (at == 0 || (at > 0 && name.charAt(at - 1) == '.')) {
        out.put(name.substring(at + marker.length()), value);
      }
    });
    return out;
  }

  @Test
  public void countersSurviveRealExecutorJvms() throws Exception {
    String basePath = Files.createTempDirectory("rli-forked").toString() + "/tbl";
    SparkSession spark = SparkSession.builder()
        .appName("rli-forked-executor-verification")
        // Real forked executor JVMs: 2 workers, 2 cores each, 4g heap each.
        .master("local-cluster[2,2,4096]")
        // local-cluster forks executors without inheriting the surefire classpath, so hand it over.
        // local-cluster forks executors through SPARK_HOME/bin/spark-class, which a Maven test JVM
        // does not have; point it at a real 3.5 distribution.
        .config("spark.home", System.getProperty("scratch.sparkHome",
            System.getProperty("user.home") + "/spark-3.5.6-bin-hadoop3"))
        .config("spark.executor.extraClassPath", System.getProperty("java.class.path"))
        .config("spark.driver.extraClassPath", System.getProperty("java.class.path"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate();
    try {
      System.out.println("\n===== forked-executor verification: master=local-cluster[2,2,4096] =====");
      gen(spark, 0, SEED_ROWS).write().format("hudi").options(opts("insert", basePath))
          .mode(SaveMode.Overwrite).save(basePath);
      System.out.println("seeded " + SEED_ROWS + " rows");

      CapturingMetricsReporter.reset();

      // UPDATE_ROWS existing keys (hits) + INSERT_ROWS brand new keys (misses).
      Dataset<Row> updates = gen(spark, 0, UPDATE_ROWS);
      Dataset<Row> inserts = gen(spark, SEED_ROWS, INSERT_ROWS);
      updates.union(inserts).write().format("hudi").options(opts("upsert", basePath))
          .mode(SaveMode.Append).save(basePath);

      Map<String, Long> counters = rliCounters();
      System.out.println("===== counters reported by the drain =====");
      counters.entrySet().stream().sorted(Map.Entry.comparingByKey())
          .forEach(e -> System.out.println(String.format("  %-46s %d", e.getKey(), e.getValue())));

      long looked = counters.getOrDefault(RecordIndexMetricNames.KEY_COUNT, -1L);
      long hits = counters.getOrDefault(RecordIndexMetricNames.KEY_HIT_COUNT, -1L);
      long misses = counters.getOrDefault(RecordIndexMetricNames.KEY_MISS_COUNT, -1L);
      long shards = counters.getOrDefault(RecordIndexMetricNames.SHARDS_READ, -1L);
      System.out.println(String.format(
          "expected: looked=%d hits=%d misses=%d   actual: looked=%d hits=%d misses=%d shards=%d",
          UPDATE_ROWS + INSERT_ROWS, UPDATE_ROWS, INSERT_ROWS, looked, hits, misses, shards));

      assertEquals(UPDATE_ROWS + INSERT_ROWS, looked, "every upserted record must be looked up");
      assertEquals(UPDATE_ROWS, hits, "the updated keys already exist in the index");
      assertEquals(INSERT_ROWS, misses, "the fresh keys must miss");
      assertTrue(shards > 0, "at least one shard read");
      assertTrue(counters.containsKey(RecordIndexMetricNames.LOOKUP_TIME), "timing must be reported");
      System.out.println("===== forked-executor verification PASSED =====\n");
    } finally {
      spark.stop();
    }
  }
}
