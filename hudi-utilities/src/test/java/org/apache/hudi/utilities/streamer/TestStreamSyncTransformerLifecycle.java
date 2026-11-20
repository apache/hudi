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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A StreamSync owns one transformer close even when its service and outer owner both shut it down. */
@Tag("functional")
class TestStreamSyncTransformerLifecycle extends UtilitiesTestBase {
  @BeforeAll
  static void startServices() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void stopServices() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Test
  void singleShutdownClosesTheOwnedTransformer() {
    CountingTransformer transformer = new CountingTransformer(false);
    sync(transformer).close();
    assertEquals(1, transformer.closes);
  }

  @Test
  void serviceAndOuterShutdownCloseTheOwnedTransformerOnce() {
    CountingTransformer transformer = new CountingTransformer(false);
    StreamSync sync = sync(transformer);
    sync.close();
    sync.close();
    assertEquals(1, transformer.closes);
  }

  @Test
  void failedTransformerCloseIsNotRetriedByTheOuterShutdown() {
    CountingTransformer transformer = new CountingTransformer(true);
    StreamSync sync = sync(transformer);
    assertThrows(IllegalStateException.class, sync::close);
    sync.close();
    assertEquals(1, transformer.closes);
  }

  private StreamSync sync(Transformer transformer) {
    return new StreamSync(new HoodieStreamer.Config(), sparkSession, new TypedProperties(), new HoodieSparkEngineContext(jsc),
        null, null, client -> true, null, Option.empty(), null, Option.of(transformer), false);
  }

  private static final class CountingTransformer implements Transformer {
    private final boolean fails;
    private int closes;

    private CountingTransformer(boolean fails) {
      this.fails = fails;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties properties) {
      return rows;
    }

    @Override
    public void close() {
      closes++;
      if (fails) {
        throw new IllegalStateException("transformer cleanup failed");
      }
    }
  }
}
