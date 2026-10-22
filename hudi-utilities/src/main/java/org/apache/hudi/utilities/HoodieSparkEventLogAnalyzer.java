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

import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.eventlog.EventLogAnalysis;
import org.apache.hudi.utilities.eventlog.EventLogParser;
import org.apache.hudi.utilities.eventlog.StageStats;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Summarises a Spark event log per stage, joined to the Hudi write phase each stage belongs to.
 *
 * <p>A generic Spark profiler can tell you that stage 14 was slow. It cannot tell you that stage 14
 * was bloom-index probing, so its advice stays generic. This tool reads the Spark job description
 * that Hudi publishes for each of its jobs and maps it onto a named Hudi phase, so a stage report
 * reads in Hudi's own vocabulary. The phase attribution is the point of the tool; the Spark metrics
 * around it are the standard ones.
 *
 * <p>It needs no {@code SparkContext}: it reads a file and is runnable as a plain {@code java -cp}
 * entry point. Paths are read through {@link HoodieStorage}, so an s3 or gcs path works wherever the
 * matching filesystem implementation is on the classpath.
 *
 * <p>The tool is strictly read-only. It never writes to the event log or to any Hudi table.
 */
@Slf4j
public class HoodieSparkEventLogAnalyzer {

  /** Bumped when the JSON envelope changes shape. Consumers should check it. */
  public static final String SCHEMA_VERSION = "1.1.0";

  /** Successful parse. */
  private static final int EXIT_OK = 0;

  /** Bad arguments, or an event log that could not be read or parsed. */
  private static final int EXIT_BAD_INPUT = 2;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Config cfg;
  private final HoodieStorage storage;

  public HoodieSparkEventLogAnalyzer(Config cfg, HoodieStorage storage) {
    this.cfg = cfg;
    this.storage = storage;
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--event-log", "-e"},
        description = "Path to the Spark event log. Either a single file, optionally compressed "
            + "(.gz, .lz4, .snappy, .zstd), or a directory of rolling log parts.",
        required = true)
    public String eventLogPath = null;

    @Parameter(names = {"--output", "-o"},
        description = "Output format: TABLE for humans, JSON for machines.")
    public String output = "TABLE";

    @Parameter(names = {"--top-n", "-n"},
        description = "Maximum number of stage rows to print in TABLE output, longest first. "
            + "Ignored for JSON output, which always carries every stage.")
    public Integer topN = 20;

    @Parameter(names = {"--min-stage-seconds", "-m"},
        description = "Omit stages shorter than this from TABLE output. "
            + "Ignored for JSON output, which always carries every stage.")
    public Double minStageSeconds = 0.0;

    @Parameter(names = {"--include-tasks"},
        description = "Include per-task duration percentiles for every stage in JSON output. "
            + "Off by default because it makes the document substantially larger.")
    public boolean includeTasks = false;

    @Parameter(names = {"--output-file", "-f"},
        description = "Write the report to this local file instead of stdout. Worth using for JSON, "
            + "whose document would otherwise be interleaved with whatever the deployment's logging "
            + "configuration sends to stdout.")
    public String outputFile = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public boolean help = false;
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = JCommander.newBuilder().addObject(cfg).build();

    // A bad flag must exit 2 (bad input), not JCommander's own failure path.
    try {
      cmd.parse(args);
    } catch (ParameterException e) {
      System.err.println("Invalid arguments: " + e.getMessage());
      cmd.usage();
      System.exit(EXIT_BAD_INPUT);
      return;
    }

    if (cfg.help) {
      cmd.usage();
      System.exit(EXIT_OK);
      return;
    }

    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf();
    HoodieStorage storage = null;
    try {
      storage = HoodieStorageUtils.getStorage(cfg.eventLogPath, storageConf);
      new HoodieSparkEventLogAnalyzer(cfg, storage).run();
      System.exit(EXIT_OK);
    } catch (Exception e) {
      log.error("Failed to analyze Spark event log at {}", cfg.eventLogPath, e);
      System.err.println("Failed to analyze Spark event log at " + cfg.eventLogPath + ": " + e.getMessage());
      System.exit(EXIT_BAD_INPUT);
    } finally {
      closeQuietly(storage);
    }
  }

  private static void closeQuietly(HoodieStorage storage) {
    if (storage == null) {
      return;
    }
    try {
      storage.close();
    } catch (IOException e) {
      log.warn("Failed to close storage", e);
    }
  }

  /** Parses the event log and writes the report to stdout, or to {@code --output-file}. */
  public void run() throws IOException {
    EventLogAnalysis analysis = new EventLogParser(storage).parse(new StoragePath(cfg.eventLogPath));

    String report = "JSON".equalsIgnoreCase(cfg.output)
        ? MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toJson(analysis))
        : renderTable(analysis);

    if (cfg.outputFile == null) {
      System.out.println(report);
    } else {
      Files.write(Paths.get(cfg.outputFile), report.getBytes(StandardCharsets.UTF_8));
      log.info("Wrote {} report to {}", cfg.output.toUpperCase(Locale.ROOT), cfg.outputFile);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // JSON output
  // ---------------------------------------------------------------------------------------------

  /**
   * Builds the JSON envelope. The shape is a stable contract read by downstream tooling and is
   * documented in {@code HoodieSparkEventLogAnalyzer.md}; see that file before changing it.
   */
  ObjectNode toJson(EventLogAnalysis analysis) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("schemaVersion", SCHEMA_VERSION);
    root.put("eventLogPath", cfg.eventLogPath);
    root.set("application", applicationJson(analysis));
    root.set("operationSummary", groupSummaryJson(analysis.getOperationSummaries(), "operation"));
    root.set("phaseSummary", groupSummaryJson(analysis.getPhaseSummaries(), "phase"));
    root.set("stages", stagesJson(analysis));

    ArrayNode caveats = root.putArray("caveats");
    analysis.getCaveats().forEach(caveats::add);

    ArrayNode warnings = root.putArray("warnings");
    analysis.getWarnings().forEach(warnings::add);
    return root;
  }

  private ObjectNode applicationJson(EventLogAnalysis analysis) {
    ObjectNode application = MAPPER.createObjectNode();
    application.put("name", analysis.getAppName());
    application.put("id", analysis.getAppId());
    application.put("startTimeEpochMillis", analysis.getStartTime());
    application.put("endTimeEpochMillis", analysis.getEndTime());
    application.put("sparkVersion", analysis.getSparkProperties().get("spark.version"));

    ObjectNode observed = application.putObject("observed");
    observed.put("durationMillis", analysis.getDurationMillis());
    observed.put("criticalPathMillis", analysis.getCriticalPathMillis());
    observed.put("jobCount", analysis.getJobCount());
    observed.put("failedJobCount", analysis.getFailedJobCount());
    observed.put("stageCount", analysis.getStages().size());
    observed.put("failedStageCount", analysis.getFailedStageCount());
    observed.put("taskCount", analysis.getTotalTaskCount());
    observed.put("failedTaskCount", analysis.getTotalFailedTaskCount());
    observed.put("executorRunTimeMillis", analysis.getTotalExecutorRunTimeMillis());
    observed.put("shuffleReadBytes", analysis.getTotalShuffleReadBytes());
    observed.put("shuffleWriteBytes", analysis.getTotalShuffleWriteBytes());
    observed.put("memoryBytesSpilled", analysis.getTotalMemoryBytesSpilled());
    observed.put("diskBytesSpilled", analysis.getTotalDiskBytesSpilled());
    observed.put("peakConcurrentExecutors", analysis.getPeakConcurrentExecutors());
    observed.put("eventsRead", analysis.getTotalEventsRead());

    ObjectNode removals = application.putObject("executorRemovalReasons");
    analysis.getExecutorRemovalReasons().forEach(removals::put);
    return application;
  }

  /**
   * Renders one summary array. Operation and phase summaries have the same shape and differ only in
   * the name of their label field, so they share this method.
   */
  private ArrayNode groupSummaryJson(List<EventLogAnalysis.GroupSummary> summaries, String labelField) {
    long totalWallClock = summaries.stream()
        .mapToLong(EventLogAnalysis.GroupSummary::getStageWallClockMillis).sum();

    ArrayNode rows = MAPPER.createArrayNode();
    for (EventLogAnalysis.GroupSummary summary : summaries) {
      ObjectNode node = rows.addObject();
      node.put(labelField, summary.getLabel());
      node.put("stageCount", summary.getStageCount());
      node.put("taskCount", summary.getTaskCount());
      node.put("stageWallClockMillis", summary.getStageWallClockMillis());
      node.put("executorRunTimeMillis", summary.getExecutorRunTimeMillis());
      node.put("shuffleBytes", summary.getShuffleBytes());
      node.put("spilledBytes", summary.getSpilledBytes());
      node.put("shareOfStageWallClock",
          totalWallClock > 0 ? round((double) summary.getStageWallClockMillis() / totalWallClock) : -1.0);
    }
    return rows;
  }

  private ArrayNode stagesJson(EventLogAnalysis analysis) {
    ArrayNode stages = MAPPER.createArrayNode();
    for (StageStats stage : analysis.getStages()) {
      ObjectNode node = stages.addObject();
      node.put("stageId", stage.getStageId());
      node.put("attemptId", stage.getAttemptId());
      node.put("name", stage.getName());
      node.put("jobId", stage.getJobId());
      node.put("jobDescription", stage.getJobDescription());
      node.put("module", stage.getModule());
      node.put("operation", stage.getOperation().name());
      node.put("operationSource", stage.getOperationSource().toJson());
      node.put("phase", stage.getPhase().name());
      node.put("subPhase", stage.getSubPhase());
      if (!stage.getBundledSteps().isEmpty()) {
        ArrayNode bundled = node.putArray("bundledSteps");
        stage.getBundledSteps().forEach(bundled::add);
      }
      node.put("submissionTimeEpochMillis", stage.getSubmissionTime());
      node.put("completionTimeEpochMillis", stage.getCompletionTime());
      node.put("failureReason", stage.getFailureReason());

      ObjectNode observed = node.putObject("observed");
      observed.put("numTasksPlanned", stage.getNumTasksPlanned());
      observed.put("numTasksObserved", stage.getNumTasksObserved());
      observed.put("durationMillis", stage.getDurationMillis());
      observed.put("executorRunTimeMillis", stage.getExecutorRunTimeMillis());
      observed.put("executorDeserializeTimeMillis", stage.getExecutorDeserializeTimeMillis());
      observed.put("jvmGcTimeMillis", stage.getJvmGcTimeMillis());
      observed.put("gcFractionOfRunTime", round(stage.getGcFraction()));
      observed.put("resultSizeBytes", stage.getResultSizeBytes());
      observed.put("inputBytes", stage.getInputBytes());
      observed.put("inputRecords", stage.getInputRecords());
      observed.put("outputBytes", stage.getOutputBytes());
      observed.put("outputRecords", stage.getOutputRecords());
      observed.put("shuffleReadBytes", stage.getShuffleReadBytes());
      observed.put("shuffleReadRecords", stage.getShuffleReadRecords());
      observed.put("shuffleFetchWaitTimeMillis", stage.getShuffleFetchWaitTimeMillis());
      observed.put("shuffleWriteBytes", stage.getShuffleWriteBytes());
      observed.put("shuffleWriteRecords", stage.getShuffleWriteRecords());
      observed.put("shuffleWriteTimeNanos", stage.getShuffleWriteTimeNanos());
      observed.put("memoryBytesSpilled", stage.getMemoryBytesSpilled());
      observed.put("diskBytesSpilled", stage.getDiskBytesSpilled());
      observed.put("tasksSucceeded", stage.getTasksSucceeded());
      observed.put("tasksFailed", stage.getTasksFailed());
      observed.put("tasksKilled", stage.getTasksKilled());
      observed.put("tasksSpeculative", stage.getTasksSpeculative());
      observed.put("skewRatio", round(stage.getSkewRatio()));

      ObjectNode taskDuration = observed.putObject("taskDurationMillis");
      taskDuration.put("p50", stage.getTaskDurationPercentile(50));
      taskDuration.put("p95", stage.getTaskDurationPercentile(95));
      taskDuration.put("max", stage.getMaxTaskDurationMillis());
      if (cfg.includeTasks) {
        taskDuration.put("p25", stage.getTaskDurationPercentile(25));
        taskDuration.put("p75", stage.getTaskDurationPercentile(75));
        taskDuration.put("p99", stage.getTaskDurationPercentile(99));
      }

      ObjectNode failureCounts = node.putObject("taskFailureReasons");
      stage.getFailureReasonCounts().forEach(failureCounts::put);
    }
    return stages;
  }

  private static double round(double value) {
    return value < 0 ? value : Math.round(value * 1000.0) / 1000.0;
  }

  // ---------------------------------------------------------------------------------------------
  // TABLE output
  // ---------------------------------------------------------------------------------------------

  String renderTable(EventLogAnalysis analysis) {
    StringBuilder report = new StringBuilder();
    appendApplicationSection(report, analysis);
    appendGroupSection(report, "operation", analysis.getOperationSummaries());
    appendGroupSection(report, "phase", analysis.getPhaseSummaries());
    appendStageSection(report, analysis);
    appendCaveatSection(report, analysis);
    appendWarningSection(report, analysis);
    return report.toString();
  }

  private void appendApplicationSection(StringBuilder report, EventLogAnalysis analysis) {
    report.append("== Application ==\n");
    report.append(String.format("  name                     %s%n", nullToDash(analysis.getAppName())));
    report.append(String.format("  id                       %s%n", nullToDash(analysis.getAppId())));
    report.append(String.format("  spark version            %s%n",
        nullToDash(analysis.getSparkProperties().get("spark.version"))));
    report.append(String.format("  duration                 %s%n", formatMillis(analysis.getDurationMillis())));
    report.append(String.format("  critical path            %s  (floor on wall clock: longest stage per job, summed)%n",
        formatMillis(analysis.getCriticalPathMillis())));
    report.append(String.format("  jobs / stages / tasks    %d / %d / %d%n",
        analysis.getJobCount(), analysis.getStages().size(), analysis.getTotalTaskCount()));
    report.append(String.format("  failed jobs/stages/tasks %d / %d / %d%n",
        analysis.getFailedJobCount(), analysis.getFailedStageCount(), analysis.getTotalFailedTaskCount()));
    report.append(String.format("  executor run time        %s%n",
        formatMillis(analysis.getTotalExecutorRunTimeMillis())));
    report.append(String.format("  shuffle read / write     %s / %s%n",
        formatBytes(analysis.getTotalShuffleReadBytes()), formatBytes(analysis.getTotalShuffleWriteBytes())));
    report.append(String.format("  spill memory / disk      %s / %s%n",
        formatBytes(analysis.getTotalMemoryBytesSpilled()), formatBytes(analysis.getTotalDiskBytesSpilled())));
    report.append(String.format("  peak executors           %d%n", analysis.getPeakConcurrentExecutors()));
    report.append('\n');
  }

  /** Renders one summary table. Operation and phase summaries share a shape, so they share this. */
  private void appendGroupSection(StringBuilder report, String columnHeading,
                                  List<EventLogAnalysis.GroupSummary> summaries) {
    long totalWallClock = summaries.stream()
        .mapToLong(EventLogAnalysis.GroupSummary::getStageWallClockMillis).sum();

    report.append(String.format("== Hudi %ss, by share of stage wall clock ==%n",
        columnHeading.toLowerCase(Locale.ROOT)));
    report.append(String.format("  %-24s %8s %7s %8s %12s %12s%n",
        columnHeading.toUpperCase(Locale.ROOT), "SHARE", "STAGES", "TASKS", "WALL CLOCK", "SHUFFLE"));
    for (EventLogAnalysis.GroupSummary summary : summaries) {
      double share = totalWallClock > 0 ? (double) summary.getStageWallClockMillis() / totalWallClock : 0.0;
      report.append(String.format("  %-24s %7.1f%% %7d %8d %12s %12s%n",
          summary.getLabel(),
          share * 100.0,
          summary.getStageCount(),
          summary.getTaskCount(),
          formatMillis(summary.getStageWallClockMillis()),
          formatBytes(summary.getShuffleBytes())));
    }
    report.append('\n');
  }

  private void appendCaveatSection(StringBuilder report, EventLogAnalysis analysis) {
    if (analysis.getCaveats().isEmpty()) {
      return;
    }
    report.append("== How far to trust the attribution ==\n");
    for (String caveat : analysis.getCaveats()) {
      report.append("  - ").append(caveat).append('\n');
    }
    report.append('\n');
  }

  private void appendStageSection(StringBuilder report, EventLogAnalysis analysis) {
    long minMillis = (long) (cfg.minStageSeconds * 1000);
    List<StageStats> candidates = new ArrayList<>();
    for (StageStats stage : analysis.getStages()) {
      if (stage.getDurationMillis() >= minMillis) {
        candidates.add(stage);
      }
    }
    candidates.sort(Comparator.comparingLong(StageStats::getDurationMillis).reversed()
        .thenComparingInt(StageStats::getStageId)
        .thenComparingInt(StageStats::getAttemptId));

    int shown = Math.min(cfg.topN == null ? candidates.size() : cfg.topN, candidates.size());
    report.append(String.format("== Stages (%d of %d shown, longest first", shown, analysis.getStages().size()));
    if (minMillis > 0) {
      report.append(String.format(", stages under %.1fs omitted", cfg.minStageSeconds));
    }
    report.append(") ==\n");

    report.append(String.format("  %7s %-18s %-22s %6s %10s %9s %9s %6s %10s %6s%n",
        "STAGE", "OPERATION", "PHASE", "TASKS", "DURATION", "P50", "MAX", "SKEW", "SHUFFLE", "FAIL"));
    for (StageStats stage : candidates.subList(0, shown)) {
      report.append(String.format("  %7s %-18s %-22s %6d %10s %9s %9s %6s %10s %6d%n",
          stage.getStageId() + "." + stage.getAttemptId(),
          stage.getOperation().name(),
          stage.getPhase().name(),
          stage.getNumTasksObserved(),
          formatMillis(stage.getDurationMillis()),
          formatMillis(stage.getTaskDurationPercentile(50)),
          formatMillis(stage.getMaxTaskDurationMillis()),
          stage.getSkewRatio() < 0 ? "-" : String.format("%.1fx", stage.getSkewRatio()),
          formatBytes(stage.getShuffleReadBytes() + stage.getShuffleWriteBytes()),
          stage.getTasksFailed()));

      // When a bucket is large, spell out in words what its number covers. Hudi never tagged these
      // steps, so the bucket name alone can still read as a single attribution.
      if (!stage.getBundledSteps().isEmpty()) {
        report.append(String.format("          this time covers: %s (not separable from this log)%n",
            String.join(" + ", stage.getBundledSteps())));
      }

      // The buckets are coarse on purpose, so the finer label and the verbatim description are the
      // escape hatches that make an attribution checkable. The stage name stands in when there is no
      // description at all.
      String detail = stage.getJobDescription() != null
          ? "desc: " + stage.getJobDescription()
          : "stage name: " + stage.getName();
      if (stage.getSubPhase() != null) {
        detail = "[" + stage.getSubPhase() + "] " + detail;
      }
      report.append(String.format("          %s%n", truncate(detail, 150)));

      if (!stage.getFailureReasonCounts().isEmpty()) {
        report.append(String.format("          task failures: %s%n",
            truncate(stage.getFailureReasonCounts().toString(), 150)));
      }
      if (stage.getFailureReason() != null) {
        report.append(String.format("          stage failed: %s%n", truncate(stage.getFailureReason(), 150)));
      }
    }
    report.append('\n');
  }

  private void appendWarningSection(StringBuilder report, EventLogAnalysis analysis) {
    if (analysis.getWarnings().isEmpty()) {
      return;
    }
    report.append("== Warnings ==\n");
    for (String warning : analysis.getWarnings()) {
      report.append("  ").append(warning).append('\n');
    }
    report.append('\n');
  }

  private static String nullToDash(String value) {
    return value == null || value.isEmpty() ? "-" : value;
  }

  private static String truncate(String value, int maxLength) {
    return value.length() <= maxLength ? value : value.substring(0, maxLength - 3) + "...";
  }

  static String formatMillis(long millis) {
    if (millis < 0) {
      return "-";
    }
    if (millis < 1000) {
      return millis + "ms";
    }
    if (millis < 60_000) {
      return String.format(Locale.ROOT, "%.1fs", millis / 1000.0);
    }
    if (millis < 3_600_000) {
      return String.format(Locale.ROOT, "%dm%02ds", millis / 60_000, (millis % 60_000) / 1000);
    }
    return String.format(Locale.ROOT, "%dh%02dm", millis / 3_600_000, (millis % 3_600_000) / 60_000);
  }

  static String formatBytes(long bytes) {
    if (bytes < 0) {
      return "-";
    }
    if (bytes < 1024) {
      return bytes + "B";
    }
    String[] units = {"KB", "MB", "GB", "TB", "PB"};
    double value = bytes;
    int unitIndex = -1;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    return String.format(Locale.ROOT, "%.1f%s", value, units[unitIndex]);
  }
}
