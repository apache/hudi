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
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resource lifecycle on the embedding path: the worker pool is released however a task
 * ends, and HTTP clients are shared per JVM rather than created per Spark partition.
 * Both leaks are invisible in a successful run and only show up as unbounded growth on a
 * long-lived executor, so they are asserted directly.
 */
public class TestEmbeddingResourceLifecycle extends UtilitiesTestBase {

  private static final String POOL_THREAD_PREFIX = "embedding-transformer";

  private static HttpServer alwaysFailingServer;

  @BeforeAll
  public static void setupAll() throws Exception {
    initTestServices();
    alwaysFailingServer = HttpServer.create(new InetSocketAddress(0), 0);
    alwaysFailingServer.createContext("/v1/embeddings", exchange -> {
      // 400 is not retriable, so the batch fails immediately and kills the task
      exchange.sendResponseHeaders(400, -1);
      exchange.close();
    });
    alwaysFailingServer.start();
  }

  @AfterAll
  public static void teardownAll() {
    if (alwaysFailingServer != null) {
      alwaysFailingServer.stop(0);
    }
  }

  /**
   * LazyIterableIterator only invokes end() when the input drains normally, so a task
   * killed by an embeddings failure used to leave its fixed thread pool behind on an
   * executor JVM that Spark keeps reusing. Releasing on task completion covers both paths.
   */
  @Test
  void testWorkerPoolIsReleasedWhenTheTaskFails() throws Exception {
    int before = livePoolThreads();

    Dataset<Row> failing = new EmbeddingTransformer()
        .apply(jsc, sparkSession, sourceDataset("doomed text").coalesce(1), failingProps());
    assertThrows(Exception.class, failing::collectAsList);

    assertTrue(awaitPoolThreadsBackTo(before),
        "embedding worker threads leaked after the task failed: " + before
            + " before, " + livePoolThreads() + " still live after");
  }

  /**
   * java.net.http.HttpClient is not Closeable before Java 21 and owns a selector thread
   * plus a connection pool, so one per partition would accumulate on the executor with no
   * release path. Providers sharing a connect timeout must share a client.
   */
  @Test
  void testHttpClientsAreSharedAcrossProviderInstances() {
    TypedProperties props = failingProps();

    OpenAICompatibleEmbeddingProvider first = new OpenAICompatibleEmbeddingProvider();
    first.init(props);
    OpenAICompatibleEmbeddingProvider second = new OpenAICompatibleEmbeddingProvider();
    second.init(props);

    HttpClient firstClient = first.client();
    assertSame(firstClient, first.client(), "repeated calls must reuse one client");
    assertSame(firstClient, second.client(),
        "a second provider with the same timeout must reuse the same client");

    TypedProperties otherTimeout = failingProps();
    otherTimeout.setProperty(EmbeddingTransformerConfig.TIMEOUT_MS.key(), "31000");
    OpenAICompatibleEmbeddingProvider third = new OpenAICompatibleEmbeddingProvider();
    third.init(otherTimeout);
    assertNotSame(firstClient, third.client(), "a different connect timeout needs its own client");
  }

  private TypedProperties failingProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTransformerConfig.ENDPOINT_URL.key(),
        "http://localhost:" + alwaysFailingServer.getAddress().getPort() + "/v1/embeddings");
    props.setProperty(EmbeddingTransformerConfig.MODEL.key(), "stub-model");
    props.setProperty(EmbeddingTransformerConfig.DIMENSION.key(), "2");
    props.setProperty(EmbeddingTransformerConfig.MAX_INFLIGHT_REQUESTS.key(), "3");
    return props;
  }

  private Dataset<Row> sourceDataset(String... texts) {
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("path", DataTypes.StringType, false),
        DataTypes.createStructField("extracted_text", DataTypes.StringType, true)});
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < texts.length; i++) {
      rows.add(RowFactory.create("file-" + i, texts[i]));
    }
    return sparkSession.createDataFrame(rows, schema);
  }

  private static int livePoolThreads() {
    int count = 0;
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread.isAlive() && thread.getName().startsWith(POOL_THREAD_PREFIX)) {
        count++;
      }
    }
    return count;
  }

  /** shutdownNow interrupts rather than joins, so give the pool threads a moment to die. */
  private static boolean awaitPoolThreadsBackTo(int expected) throws InterruptedException {
    for (int attempt = 0; attempt < 100; attempt++) {
      if (livePoolThreads() <= expected) {
        return true;
      }
      Thread.sleep(100);
    }
    return false;
  }
}
