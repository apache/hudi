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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Coverage for {@link HoodieRowParquetWriteSupport#resolveSessionLocalTimeZone()}.
 * The full {@code init()} method is exercised end-to-end by integration tests in
 * {@code hudi-spark-datasource/hudi-spark} (e.g. {@code TestHoodieInternalRowParquetWriter},
 * {@code TestSparkAdapterRebaseModePropagation}). Constructing the write support
 * directly from this module is not feasible because it requires a Spark
 * adapter (e.g. {@code Spark3_5Adapter}) that is not on the test classpath
 * here — the adapters live in sibling modules that depend on this one.
 */
class TestHoodieRowParquetWriteSupport extends HoodieClientTestBase {

  private static final String SESSION_LOCAL_TIME_ZONE_KEY = "spark.sql.session.timeZone";

  @Test
  void testResolveSessionLocalTimeZoneWithoutOverride() {
    String expected = TimeZone.getDefault().getID();

    // Driver thread.
    assertEquals(expected, HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone(),
        "driver-side helper did not return the JVM default timezone");

    // Executor task threads via vanilla parallelize().map() — outside any
    // Spark SQL execution context — exercise the SparkEnv-fallback branch.
    List<String> seen = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 4)
        .map(i -> HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone())
        .collect();
    for (int i = 0; i < seen.size(); i++) {
      assertEquals(expected, seen.get(i),
          "executor task #" + i + " resolved sessionLocalTimeZone to '" + seen.get(i)
              + "' with no overrides; expected the JVM default ('" + expected + "')");
    }
  }

  @Test
  void testResolveSessionLocalTimeZoneWithSqlConfOverride() {
    // Pick a non-JVM-default zone so we can distinguish "fix worked" from
    // "fell back to JVM default".
    String jvmDefault = TimeZone.getDefault().getID();
    String customTz = "Asia/Tokyo".equals(jvmDefault) ? "Pacific/Auckland" : "Asia/Tokyo";

    sqlContext.sparkSession().conf().set(SESSION_LOCAL_TIME_ZONE_KEY, customTz);
    try {
      // Driver SQLConf carries the override; helper must return it via the first branch.
      assertEquals(customTz, HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone(),
          "helper did not return the SQLConf override on the driver");
    } finally {
      sqlContext.sparkSession().conf().unset(SESSION_LOCAL_TIME_ZONE_KEY);
    }
  }

  /**
   * SparkConf branch: when the override lives in SparkConf (broadcast to every
   * executor) but is absent from the current thread's SQLConf, the helper
   * must return it via {@code SparkEnv.get.conf}. Mirrors a compaction task
   * thread that is outside any SQL execution context — exactly the
   * production scenario the fix targets.
   */
  @Test
  void testResolveSessionLocalTimeZoneWithSparkConfOverride() {
    String jvmDefault = TimeZone.getDefault().getID();
    String customTz = "Asia/Tokyo".equals(jvmDefault) ? "Pacific/Auckland" : "Asia/Tokyo";
    assertNotEquals(customTz, jvmDefault,
        "test setup is fragile if customTz matches the JVM default");

    // Inject the key into SparkConf and remove it from the SparkSession's
    // SQLConf so only the SparkEnv branch can satisfy the lookup.
    jsc.sc().conf().set(SESSION_LOCAL_TIME_ZONE_KEY, customTz);
    sqlContext.sparkSession().conf().unset(SESSION_LOCAL_TIME_ZONE_KEY);
    try {
      // Driver: SQLConf branch returns null → SparkEnv branch returns customTz.
      assertEquals(customTz, HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone(),
          "driver helper should fall back to SparkEnv when SQLConf is unset");

      // Executor task threads: SQLConf.get returns the fallback default (no
      // override), so the SparkEnv branch is the only one that can satisfy
      // the lookup. SparkEnv is shared by driver and executors in local mode.
      List<String> seen = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 4)
          .map(i -> HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone())
          .collect();
      for (int i = 0; i < seen.size(); i++) {
        assertEquals(customTz, seen.get(i),
            "executor task #" + i + " should resolve to SparkConf override '"
                + customTz + "', got '" + seen.get(i) + "'");
      }
    } finally {
      jsc.sc().conf().remove(SESSION_LOCAL_TIME_ZONE_KEY);
    }
  }
}
