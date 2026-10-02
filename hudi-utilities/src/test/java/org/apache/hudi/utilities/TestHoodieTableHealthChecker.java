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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieTableHealthChecker} end to end: check selection, output rendering, and the
 * exit codes a scheduled run depends on.
 */
public class TestHoodieTableHealthChecker {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  private String basePath;

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private HoodieTableHealthChecker.Config configFor(String checks) {
    HoodieTableHealthChecker.Config cfg = new HoodieTableHealthChecker.Config();
    cfg.basePath = basePath;
    cfg.checks = checks;
    return cfg;
  }

  /** Runs the checker, returning its exit code and whatever it printed. */
  private Result runChecker(HoodieTableHealthChecker.Config cfg) {
    PrintStream originalOut = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
      int exitCode = new HoodieTableHealthChecker(new Configuration(), cfg).run();
      return new Result(exitCode, new String(captured.toByteArray(), StandardCharsets.UTF_8));
    } catch (RuntimeException e) {
      // Surface the tool's own unchecked exceptions unwrapped, so tests can assert on their type.
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      System.setOut(originalOut);
    }
  }

  private HoodieTableMetaClient healthyMorTable() throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable.of(metaClient).addDeltaCommit("001").addDeltaCommit("002");
    return metaClient;
  }

  private HoodieTableMetaClient laggingMorTable() throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 60; i++) {
      table.addDeltaCommit(String.format("%05d", i));
    }
    return metaClient;
  }

  @Test
  public void healthyTableExitsZero() throws Exception {
    // given a table whose services are keeping up, with writer properties supplied
    healthyMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("all");
    cfg.configs = Arrays.asList(
        HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key() + "=NUM_COMMITS",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() + "=5",
        HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key() + "=1",
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() + "=30");

    // when the checker runs
    Result result = runChecker(cfg);

    // then it exits 0 and says so
    assertEquals(HoodieTableHealthChecker.EXIT_HEALTHY, result.exitCode);
    assertTrue(result.output.contains("overall   : HEALTHY"), result.output);
  }

  @Test
  public void unhealthyTableExitsOne() throws Exception {
    // given a table badly behind on both compaction and archival
    laggingMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("all");
    cfg.configs = Arrays.asList(
        HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key() + "=NUM_COMMITS",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() + "=5",
        HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key() + "=1",
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() + "=10");

    // when the checker runs
    Result result = runChecker(cfg);

    // then it exits 1, so a cron job notices without parsing anything
    assertEquals(HoodieTableHealthChecker.EXIT_UNHEALTHY, result.exitCode);
    assertTrue(result.output.contains("overall   : UNHEALTHY"), result.output);
  }

  @Test
  public void tableWithNoSuppliedPropertiesSkipsEverythingAndStillExitsZero() throws Exception {
    // given a badly lagging table, but no writer properties to judge it against
    laggingMorTable();

    // when the checker runs with nothing supplied
    Result result = runChecker(configFor("all"));

    // then every check skips and the run exits 0 -- SKIPPED is silence, not a failure signal
    assertEquals(HoodieTableHealthChecker.EXIT_HEALTHY, result.exitCode);
    assertTrue(result.output.contains("SKIPPED"), result.output);
    assertTrue(result.output.contains("--apply-all-defaults"), result.output);
  }

  @Test
  public void applyAllDefaultsTurnsTheSameTableUnhealthy() throws Exception {
    // given the same lagging table and still no supplied properties
    laggingMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("all");
    cfg.applyAllDefaults = true;

    // when the operator explicitly accepts Hudi defaults
    Result result = runChecker(cfg);

    // then the checks actually evaluate, and the lag is caught
    assertEquals(HoodieTableHealthChecker.EXIT_UNHEALTHY, result.exitCode);
  }

  @Test
  public void checksCanBeSelectedIndividually() throws Exception {
    // given any table
    healthyMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("archival");
    cfg.configs = Arrays.asList(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() + "=30");

    // when only one check is requested
    Result result = runChecker(cfg);

    // then only that check is reported
    assertTrue(result.output.contains("archival"), result.output);
    assertTrue(!result.output.contains("] compaction"), result.output);
  }

  @Test
  public void unknownCheckNameIsRejectedWithTheAvailableNames() throws Exception {
    // given a request for a check that does not exist
    healthyMorTable();

    // when the checker runs
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> runChecker(configFor("no-such-check")));

    // then the error names the valid options rather than failing opaquely
    assertTrue(thrown.getMessage().contains("no-such-check"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("compaction"), thrown.getMessage());
  }

  @Test
  public void jsonOutputIsMachineReadableAndCarriesEveryVerdict() throws Exception {
    // given a lagging table
    laggingMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("all");
    cfg.output = HoodieTableHealthChecker.OutputFormat.JSON;
    cfg.configs = Arrays.asList(
        HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key() + "=NUM_COMMITS",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() + "=5",
        HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key() + "=1",
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() + "=10");

    // when the checker runs with JSON output
    Result result = runChecker(cfg);

    // then the report parses, and every check appears with its status and effective config
    JsonNode root = MAPPER.readTree(result.output);
    assertEquals("UNHEALTHY", root.get("overallStatus").asText());
    assertEquals(HoodieTableType.MERGE_ON_READ.name(), root.get("tableType").asText());
    assertEquals(4, root.get("checks").size());

    JsonNode compaction = root.get("checks").get(0);
    assertEquals("compaction", compaction.get("name").asText());
    assertEquals("UNHEALTHY", compaction.get("status").asText());
    assertTrue(compaction.get("findings").size() > 0);
    assertTrue(compaction.get("effectiveConfigs")
        .has(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key()));
  }

  @Test
  public void verboseOutputEchoesTheConfigEachCheckUsed() throws Exception {
    // given a healthy table checked verbosely
    healthyMorTable();
    HoodieTableHealthChecker.Config cfg = configFor("archival");
    cfg.verbose = true;
    cfg.configs = Arrays.asList(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() + "=30");

    // when the checker runs
    Result result = runChecker(cfg);

    // then the verdict can be audited against the values that produced it
    assertTrue(result.output.contains("config used:"), result.output);
    assertTrue(result.output.contains(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key()), result.output);
  }

  private static class Result {
    final int exitCode;
    final String output;

    Result(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
