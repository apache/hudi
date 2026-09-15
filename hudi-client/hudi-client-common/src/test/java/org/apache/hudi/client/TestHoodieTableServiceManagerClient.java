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

package org.apache.hudi.client;

import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieRemoteException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieTableServiceManagerClient} with the HTTP transport mocked by a
 * local in-process {@link HttpServer}. Assertions cover the request path, the query
 * parameters sent to the service, and retry/error handling.
 */
public class TestHoodieTableServiceManagerClient {

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";

  @TempDir
  Path tempDir;

  private HttpServer server;

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  private HoodieTableMetaClient initMetaClient() throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), TABLE_NAME);
    props.setProperty(HoodieTableConfig.DATABASE_NAME.key(), DB_NAME);
    return HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(),
        tempDir.resolve("table").toString(),
        HoodieTableType.COPY_ON_WRITE,
        props);
  }

  private HoodieTableServiceManagerConfig configFor(String uri) {
    Properties props = new Properties();
    // Keep retry cheap so the error-path test stays fast.
    props.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_RETRIES.key(), "2");
    props.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_RETRY_DELAY_SEC.key(), "1");
    props.setProperty(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_TIMEOUT_SEC.key(), "5");
    return HoodieTableServiceManagerConfig.newBuilder().fromProperties(props).setURIs(uri).build();
  }

  /**
   * Parses a raw query string of the form {@code a=1&b=2} into a decoded map.
   */
  private static Map<String, String> parseQuery(String rawQuery) throws IOException {
    Map<String, String> params = new HashMap<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return params;
    }
    for (String pair : rawQuery.split("&")) {
      int idx = pair.indexOf('=');
      String key = idx >= 0 ? pair.substring(0, idx) : pair;
      String value = idx >= 0 ? pair.substring(idx + 1) : "";
      params.put(
          URLDecoder.decode(key, StandardCharsets.UTF_8.name()),
          URLDecoder.decode(value, StandardCharsets.UTF_8.name()));
    }
    return params;
  }

  /**
   * Starts a local HTTP server that records the request path and query params of the last
   * request and replies 200. Returns the base URI (scheme + host + port).
   */
  private String startRecordingServer(Map<String, Object> captured) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        captured.put("path", exchange.getRequestURI().getPath());
        captured.put("query", parseQuery(exchange.getRequestURI().getRawQuery()));
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      }
    });
    server.start();
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @Test
  public void testExecuteCompactionSendsExpectedRequest() throws IOException {
    Map<String, Object> captured = new HashMap<>();
    String uri = startRecordingServer(captured);
    HoodieTableServiceManagerClient client =
        new HoodieTableServiceManagerClient(initMetaClient(), configFor(uri));

    Option<String> result = client.executeCompaction();

    // With no pending compaction the instant range is empty, but the request is still sent.
    assertTrue(result.isPresent());
    assertEquals("", result.get());
    assertEquals(HoodieTableServiceManagerClient.EXECUTE_COMPACTION, captured.get("path"));

    @SuppressWarnings("unchecked")
    Map<String, String> query = (Map<String, String>) captured.get("query");
    assertEquals(HoodieTableServiceManagerClient.Action.REQUEST.name(),
        query.get(HoodieTableServiceManagerClient.ACTION));
    assertEquals(DB_NAME, query.get(HoodieTableServiceManagerClient.DATABASE_NAME_PARAM));
    assertEquals(TABLE_NAME, query.get(HoodieTableServiceManagerClient.TABLE_NAME_PARAM));
    assertTrue(query.containsKey(HoodieTableServiceManagerClient.BASEPATH_PARAM));
    assertTrue(query.containsKey(HoodieTableServiceManagerClient.INSTANT_PARAM));
    assertTrue(query.containsKey(HoodieTableServiceManagerClient.EXECUTION_ENGINE));
    assertTrue(query.containsKey(HoodieTableServiceManagerClient.PARALLELISM));
  }

  @Test
  public void testExecuteCleanTargetsCleanEndpoint() throws IOException {
    Map<String, Object> captured = new HashMap<>();
    String uri = startRecordingServer(captured);
    HoodieTableServiceManagerClient client =
        new HoodieTableServiceManagerClient(initMetaClient(), configFor(uri));

    client.executeClean();
    assertEquals(HoodieTableServiceManagerClient.EXECUTE_CLEAN, captured.get("path"));
  }

  @Test
  public void testExecuteClusteringTargetsClusterEndpoint() throws IOException {
    Map<String, Object> captured = new HashMap<>();
    String uri = startRecordingServer(captured);
    HoodieTableServiceManagerClient client =
        new HoodieTableServiceManagerClient(initMetaClient(), configFor(uri));

    client.executeClustering();
    assertEquals(HoodieTableServiceManagerClient.EXECUTE_CLUSTERING, captured.get("path"));
  }

  @Test
  public void testServerErrorRetriesAndSurfacesRemoteException() throws IOException {
    AtomicInteger requestCount = new AtomicInteger(0);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      requestCount.incrementAndGet();
      // Always fail so the retry limit is exhausted and an exception propagates.
      byte[] body = "boom".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(500, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    server.start();
    String uri = "http://127.0.0.1:" + server.getAddress().getPort();

    HoodieTableServiceManagerClient client =
        new HoodieTableServiceManagerClient(initMetaClient(), configFor(uri));

    assertThrows(HoodieRemoteException.class, client::executeCompaction);
    // Two retries configured means the request is attempted more than once.
    assertTrue(requestCount.get() >= 2,
        "expected at least 2 attempts, got " + requestCount.get());
  }

  @Test
  public void testUnreachableServerSurfacesRemoteException() throws IOException {
    // Port 1 is a privileged, unbound port: the connection will be refused.
    HoodieTableServiceManagerClient client =
        new HoodieTableServiceManagerClient(initMetaClient(), configFor("http://127.0.0.1:1"));
    assertThrows(HoodieRemoteException.class, client::executeCompaction);
  }
}
