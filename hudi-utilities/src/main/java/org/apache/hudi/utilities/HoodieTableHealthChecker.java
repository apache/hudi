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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.health.ArchivalCadenceCheck;
import org.apache.hudi.utilities.health.CleanerCadenceCheck;
import org.apache.hudi.utilities.health.CompactionCadenceCheck;
import org.apache.hudi.utilities.health.HealthCheck;
import org.apache.hudi.utilities.health.HealthCheckContext;
import org.apache.hudi.utilities.health.HealthCheckResult;
import org.apache.hudi.utilities.health.HealthStatus;
import org.apache.hudi.utilities.health.SavepointArchivalBlockCheck;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reports whether a Hudi table's table services are keeping up with ingestion.
 *
 * <p>Intended to be run on a schedule -- daily is typical -- against tables an operator is
 * responsible for. Each check answers one question about whether a service is progressing as
 * expected, and the process exit code makes the overall verdict usable from a cron job or CI
 * pipeline without parsing the output.
 *
 * <p>This tool is strictly read-only. It inspects timeline state and reports; it never schedules or
 * runs a table service, and never writes to the table.
 *
 * <h3>Writer properties are required</h3>
 *
 * <p>Nearly every question this tool answers is relative to how the table is written. Whether
 * fifty delta commits since the last compaction is fine or alarming depends on the configured
 * trigger; whether a thousand instants in the active timeline is fine depends on the configured
 * retention. Hudi has a default for each of these, so the tool could always produce an answer --
 * but for a table written with non-default settings that answer would be wrong, and wrong in the
 * direction that erodes trust, reporting healthy tables as unhealthy.
 *
 * <p>So by default a check whose governing property was not supplied reports {@code SKIPPED} and
 * names the property, rather than guessing. Supply properties with {@code --props} or
 * {@code --hoodie-conf}, or pass {@code --apply-all-defaults} to explicitly accept Hudi defaults
 * for anything unset.
 *
 * <h3>Exit codes</h3>
 * <ul>
 *   <li>{@code 0} -- every check that ran reported healthy</li>
 *   <li>{@code 1} -- at least one check reported unhealthy</li>
 *   <li>{@code 2} -- bad arguments, or the table could not be read</li>
 * </ul>
 *
 * <h3>Example</h3>
 * <pre>
 * spark-submit --master local \
 *   --class org.apache.hudi.utilities.HoodieTableHealthChecker \
 *   $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
 *   --base-path s3://bucket/path/to/table \
 *   --props s3://bucket/path/to/writer.properties \
 *   --output JSON
 * </pre>
 */
@Slf4j
public class HoodieTableHealthChecker implements Serializable {

  private static final long serialVersionUID = 1L;

  static final int EXIT_HEALTHY = 0;
  static final int EXIT_UNHEALTHY = 1;
  static final int EXIT_ERROR = 2;

  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final Config cfg;
  private final TypedProperties props;
  private final HoodieTableMetaClient metaClient;

  public HoodieTableHealthChecker(Configuration hadoopConf, Config cfg) {
    this.cfg = cfg;
    this.props = UtilHelpers.buildProperties(hadoopConf, cfg.propsFilePath, cfg.configs);
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
        .setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  /** All checks this tool knows about, in report order. */
  static List<HealthCheck> allChecks() {
    return Arrays.asList(
        new CompactionCadenceCheck(),
        new CleanerCadenceCheck(),
        new SavepointArchivalBlockCheck(),
        new ArchivalCadenceCheck());
  }

  /**
   * Runs the selected checks and renders the report.
   *
   * @return the exit code the process should terminate with.
   */
  public int run() {
    List<HealthCheck> selected = selectChecks(cfg.checks);
    HealthCheckContext context = new HealthCheckContext(metaClient, props, cfg.applyAllDefaults);

    List<HealthCheckResult> results = new ArrayList<>();
    List<String> erroredChecks = new ArrayList<>();

    for (HealthCheck check : selected) {
      try {
        results.add(check.check(context));
      } catch (Exception e) {
        // One check failing to reach a verdict must not hide the others. Record it and continue.
        log.error("Health check '{}' failed to run for table {}", check.getName(), cfg.basePath, e);
        erroredChecks.add(check.getName() + ": " + e.getMessage());
      }
    }

    if (OutputFormat.JSON == cfg.output) {
      System.out.println(renderJson(results, erroredChecks));
    } else {
      System.out.println(renderTable(results, erroredChecks));
    }

    if (!erroredChecks.isEmpty()) {
      return EXIT_ERROR;
    }
    return results.stream().anyMatch(r -> r.getStatus().isUnhealthy()) ? EXIT_UNHEALTHY : EXIT_HEALTHY;
  }

  private List<HealthCheck> selectChecks(String requested) {
    List<HealthCheck> available = allChecks();
    if (StringUtils.isNullOrEmpty(requested) || "all".equalsIgnoreCase(requested)) {
      return available;
    }

    Map<String, HealthCheck> byName = new LinkedHashMap<>();
    available.forEach(c -> byName.put(c.getName(), c));

    List<HealthCheck> selected = new ArrayList<>();
    for (String name : requested.split(",")) {
      String trimmed = name.trim().toLowerCase();
      if (!byName.containsKey(trimmed)) {
        throw new IllegalArgumentException(String.format(
            "Unknown check '%s'. Available checks: %s", trimmed, String.join(", ", byName.keySet())));
      }
      selected.add(byName.get(trimmed));
    }
    return selected;
  }

  private HealthStatus overallStatus(List<HealthCheckResult> results) {
    return results.stream()
        .map(HealthCheckResult::getStatus)
        .reduce(HealthStatus.SKIPPED, HealthStatus::max);
  }

  private String renderTable(List<HealthCheckResult> results, List<String> erroredChecks) {
    StringBuilder sb = new StringBuilder();
    sb.append("\nHudi table health report\n");
    sb.append("  table     : ").append(cfg.basePath).append('\n');
    sb.append("  type      : ").append(metaClient.getTableType()).append('\n');
    sb.append("  overall   : ").append(overallStatus(results)).append("\n\n");

    for (HealthCheckResult result : results) {
      sb.append(String.format("[%-9s] %s%n", result.getStatus(), result.getCheckName()));
      sb.append("    ").append(result.getSummary()).append('\n');

      for (String finding : result.getFindings()) {
        sb.append("    - ").append(finding).append('\n');
      }

      if (cfg.verbose && !result.getEffectiveConfigs().isEmpty()) {
        sb.append("    config used:\n");
        result.getEffectiveConfigs().forEach((k, v) -> sb.append("      ").append(k).append(" = ").append(v).append('\n'));
      }
      sb.append('\n');
    }

    if (!erroredChecks.isEmpty()) {
      sb.append("Checks that could not run:\n");
      erroredChecks.forEach(e -> sb.append("  - ").append(e).append('\n'));
    }

    return sb.toString();
  }

  private String renderJson(List<HealthCheckResult> results, List<String> erroredChecks) {
    ObjectNode root = JSON_MAPPER.createObjectNode();
    root.put("basePath", cfg.basePath);
    root.put("tableType", metaClient.getTableType().name());
    root.put("overallStatus", overallStatus(results).name());

    ArrayNode checks = root.putArray("checks");
    for (HealthCheckResult result : results) {
      ObjectNode node = checks.addObject();
      node.put("name", result.getCheckName());
      node.put("status", result.getStatus().name());
      node.put("summary", result.getSummary());

      ArrayNode findings = node.putArray("findings");
      result.getFindings().forEach(findings::add);

      ObjectNode configs = node.putObject("effectiveConfigs");
      result.getEffectiveConfigs().forEach(configs::put);
    }

    ArrayNode errors = root.putArray("errors");
    erroredChecks.forEach(errors::add);

    try {
      return JSON_MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to render health report as JSON", e);
    }
  }

  public enum OutputFormat {
    TABLE, JSON
  }

  public static class Config implements Serializable {

    private static final long serialVersionUID = 1L;

    @Parameter(names = {"--base-path", "-sp"}, description = "Base path of the Hudi table to check", required = true)
    public String basePath = null;

    @Parameter(names = {"--checks"}, description = "Comma-separated checks to run, or 'all'. Available: "
        + "compaction, cleaner, savepoint, archival")
    public String checks = "all";

    @Parameter(names = {"--apply-all-defaults"}, description = "Evaluate checks against Hudi default values for any "
        + "writer property not supplied via --props or --hoodie-conf. Off by default: without the writer properties "
        + "the table was actually written with, a verdict derived from defaults can report a healthy table as "
        + "unhealthy, so such checks are skipped instead.")
    public Boolean applyAllDefaults = false;

    @Parameter(names = {"--output"}, description = "Output format: TABLE or JSON")
    public OutputFormat output = OutputFormat.TABLE;

    @Parameter(names = {"--verbose", "-v"}, description = "Include the effective configuration each check used")
    public Boolean verbose = false;

    @Parameter(names = {"--props"}, description = "Path to a properties file on local fs or dfs, holding the writer "
        + "configuration this table is written with")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any writer configuration that can be set in the properties "
        + "file (using --props) can also be passed on the command line here. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HoodieTableHealthCheckerConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --checks " + checks + ", \n"
          + "   --apply-all-defaults " + applyAllDefaults + ", \n"
          + "   --output " + output + ", \n"
          + "   --verbose " + verbose + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }
  }

  public static void main(String[] args) {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      printAvailableChecks();
      System.exit(EXIT_ERROR);
    }

    try {
      System.exit(new HoodieTableHealthChecker(new Configuration(), cfg).run());
    } catch (IllegalArgumentException e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(EXIT_ERROR);
    } catch (Throwable t) {
      log.error("Failed to run table health check for {}", cfg, t);
      System.exit(EXIT_ERROR);
    }
  }

  private static void printAvailableChecks() {
    System.out.println("\nAvailable checks:");
    allChecks().forEach(c -> System.out.printf("  %-12s %s%n", c.getName(), c.getDescription()));
    System.out.println("\nExit codes: 0 = healthy, 1 = unhealthy, 2 = error\n");
  }
}
