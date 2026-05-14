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

package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.TestcontainersConfig;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

import lombok.extern.slf4j.Slf4j;

/**
 * Service wrapper for the Trino coordinator. Mirrors {@link HiveService} in shape, but
 * execs the bundled {@code trino} CLI inside the coordinator container itself rather
 * than from an adhoc Spark container - Trino 472's CLI jar requires JDK 22+, which the
 * Spark 3.5 adhoc images (JDK 11) do not ship. The coordinator image bundles its own
 * JDK 23, so {@code execInContainer("trino", ...)} just works.
 *
 * <p>The default catalog is {@code hudi} (the native trino-hudi connector registered
 * by {@code HudiConnectorFactory#getName}) and the default schema is {@code default}.
 *
 * <p>Output format is {@code CSV_UNQUOTED} so substring assertions stay simple and
 * match the existing {@link HiveService} ergonomics.
 */
@Slf4j
public class TrinoService {

  private static final String CLI = "trino";
  private static final String SERVER = "localhost:" + TestcontainersConfig.Network.TRINO_PORT;
  private static final String DEFAULT_CATALOG = "hudi";
  private static final String DEFAULT_SCHEMA = "default";

  private final CommandExecutor executor;

  public TrinoService(ContainerProvider provider) {
    this.executor = new CommandExecutor(
        provider.getContainer(TestcontainersConfig.Containers.TRINOCOORDINATOR));
  }

  /**
   * Execute a single Trino SQL statement against the default {@code hudi.default}
   * catalog/schema. Returns a {@link CommandResult} so the fluent assertions used by
   * other services apply unchanged.
   */
  public CommandResult execute(String sql) throws Exception {
    return execute(DEFAULT_CATALOG, DEFAULT_SCHEMA, sql);
  }

  /**
   * Execute a single Trino SQL statement against an explicit catalog/schema. Useful
   * for {@code SHOW CATALOGS} or cross-catalog probes where the schema is irrelevant.
   */
  public CommandResult execute(String catalog, String schema, String sql) throws Exception {
    String[] cmd = {
        CLI,
        "--server", SERVER,
        "--catalog", catalog,
        "--schema", schema,
        "--output-format", "CSV_UNQUOTED",
        "--execute", sql
    };
    return executor.executeCommand(cmd);
  }

  /**
   * Block until the coordinator is ready to serve queries. Trino reports a healthy
   * HTTP {@code /v1/info} well before plugin discovery finishes, so the cheapest
   * reliable readiness probe is to actually issue a query.
   */
  public void waitUntilReady() throws Exception {
    int max = TestcontainersConfig.Timeouts.TRINO_READY_MAX_RETRIES;
    long sleepMs = TestcontainersConfig.Timeouts.TRINO_READY_RETRY_INTERVAL.toMillis();
    for (int i = 1; i <= max; i++) {
      try {
        execute("system", "runtime", "SELECT 1").expectToSucceed();
        log.info("Trino coordinator is ready (attempt {}/{})", i, max);
        return;
      } catch (Throwable t) {
        if (i == max) {
          throw new RuntimeException(
              "Trino coordinator did not become ready after " + max + " retries", t);
        }
        log.info("Waiting for Trino coordinator to be ready (attempt {}/{}): {}", i, max, t.getMessage());
        Thread.sleep(sleepMs);
      }
    }
  }
}
