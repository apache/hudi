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

package org.apache.hudi.utilities.eventlog;

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Parses a Spark event log into {@link EventLogAnalysis}.
 *
 * <p>The log is newline-delimited JSON, one event per line. Parsing deliberately does not use
 * Spark's own {@code JsonProtocol}: that class is private to Spark and coupled to the writer's Spark
 * version, whereas the field names read here have been stable for many releases. Unknown events are
 * ignored, missing fields default to zero, and a line that does not parse is recorded as a warning
 * rather than failing the run -- event logs from killed applications routinely end mid-line.
 */
@Slf4j
public class EventLogParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Rolling event logs put the status marker in the same directory; it carries no events. */
  private static final String APP_STATUS_FILE_PREFIX = "appstatus";

  /**
   * Share of total stage wall clock at or above which a bucket that bundles untagged work spells out
   * what it contains. Documented in the tool's .md.
   */
  private static final double COMPOSITE_BUCKET_SHARE_THRESHOLD = 0.15;

  /**
   * The untagged steps each composite bucket absorbs. Hudi sets no job status around these, and
   * Spark's laziness forces their cost into the stage named by the key, so they cannot be measured
   * separately from an event log.
   */
  private static final Map<HudiPhase, List<String>> BUNDLED_STEPS = buildBundledSteps();

  private static Map<HudiPhase, List<String>> buildBundledSteps() {
    Map<HudiPhase, List<String>> bundled = new LinkedHashMap<>();
    bundled.put(HudiPhase.SOURCE_READ_AND_TRANSFORM,
        Collections.unmodifiableList(Arrays.asList("source read", "transformation", "record creation")));
    bundled.put(HudiPhase.DEDUP_AND_INDEX_TAGGING,
        Collections.unmodifiableList(Arrays.asList("deduplication", "index tagging")));
    return Collections.unmodifiableMap(bundled);
  }

  private final HoodieStorage storage;
  private final List<String> warnings = new ArrayList<>();

  /** Stage attempts, keyed by "stageId.attemptId" so that retried stages stay distinct. */
  private final Map<String, StageStats> stagesByAttempt = new LinkedHashMap<>();

  /** Job id to the job description it was submitted with. */
  private final Map<Integer, String> jobDescriptions = new HashMap<>();
  private final Map<Integer, String> jobModules = new HashMap<>();

  /** Stage id to the job that owns it. A stage belongs to exactly one job. */
  private final Map<Integer, Integer> stageToJob = new HashMap<>();

  private final EventLogAnalysis analysis = new EventLogAnalysis();

  public EventLogParser(HoodieStorage storage) {
    this.storage = storage;
  }

  /**
   * Reads every event-log file under the given path and returns the completed analysis.
   *
   * @param eventLogPath a single event-log file, or a directory of rolling log parts.
   */
  public EventLogAnalysis parse(StoragePath eventLogPath) throws IOException {
    List<StoragePath> parts = resolveLogParts(eventLogPath);
    if (parts.isEmpty()) {
      throw new IOException("No event log files found under " + eventLogPath);
    }

    for (StoragePath part : parts) {
      parseOnePart(part);
    }

    if (analysis.getTotalEventsRead() == 0) {
      throw new IOException("No parseable Spark events found under " + eventLogPath
          + "; is this a Spark event log?");
    }

    finishAnalysis();
    return analysis;
  }

  /**
   * Expands a path into the ordered list of files to read. Rolling logs are a directory of
   * {@code events_<n>_<appId>} parts, which must be read in numeric order so that the application
   * start event is seen before the stages that follow it.
   */
  private List<StoragePath> resolveLogParts(StoragePath eventLogPath) throws IOException {
    if (!storage.exists(eventLogPath)) {
      throw new IOException("Event log path does not exist: " + eventLogPath);
    }

    StoragePathInfo info = storage.getPathInfo(eventLogPath);
    if (!info.isDirectory()) {
      return Collections.singletonList(eventLogPath);
    }

    List<StoragePath> parts = new ArrayList<>();
    for (StoragePathInfo entry : storage.listDirectEntries(eventLogPath)) {
      if (entry.isDirectory()) {
        continue;
      }
      String fileName = entry.getPath().getName();
      if (fileName.startsWith(APP_STATUS_FILE_PREFIX) || fileName.startsWith(".")) {
        continue;
      }
      parts.add(entry.getPath());
    }
    parts.sort(Comparator.comparingLong(EventLogParser::rollingPartIndex)
        .thenComparing(path -> path.getName()));
    return parts;
  }

  /**
   * Extracts {@code n} from a rolling part named {@code events_<n>_<appId>}, so that part 2 sorts
   * before part 10. Returns {@link Long#MAX_VALUE} for anything that is not a rolling part, which
   * leaves such files sorted by name after the numbered ones.
   */
  private static long rollingPartIndex(StoragePath path) {
    String name = path.getName();
    if (!name.startsWith("events_")) {
      return Long.MAX_VALUE;
    }
    int end = name.indexOf('_', "events_".length());
    if (end < 0) {
      return Long.MAX_VALUE;
    }
    try {
      return Long.parseLong(name.substring("events_".length(), end));
    } catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }

  private void parseOnePart(StoragePath part) throws IOException {
    try (InputStream rawStream = storage.open(part);
         InputStream decoded = decompress(rawStream, part.getName());
         BufferedReader reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8))) {

      String line;
      long lineNumber = 0;
      String lastUnparseableLine = null;
      long unparseableLines = 0;

      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (line.trim().isEmpty()) {
          continue;
        }
        try {
          handleEvent(MAPPER.readTree(line));
          analysis.incrementEventsRead();
        } catch (IOException parseFailure) {
          unparseableLines++;
          lastUnparseableLine = "line " + lineNumber + " of " + part.getName();
        }
      }

      if (unparseableLines > 0) {
        addWarning(unparseableLines + " line(s) in " + part.getName() + " could not be parsed as JSON "
            + "and were skipped (last: " + lastUnparseableLine + "). A truncated final line is expected "
            + "for an application that was killed.");
      }
    }
  }

  /**
   * Wraps the raw stream in a decompressor when the file name says it is compressed.
   *
   * <p>Gzip is handled by the JDK. The codecs Spark can also use ({@code lz4}, {@code snappy},
   * {@code zstd}) are resolved through Hadoop's {@code CompressionCodecFactory} reflectively, so that
   * this tool still runs on a classpath without Hadoop and so that it does not pin a codec library
   * version -- Spark 4.2 moved lz4-java to a different groupId, and a direct dependency here would
   * break on one Spark line or the other.
   */
  private InputStream decompress(InputStream rawStream, String fileName) throws IOException {
    String lowerName = fileName.toLowerCase(java.util.Locale.ROOT);
    if (lowerName.endsWith(".gz")) {
      return new GZIPInputStream(rawStream);
    }
    if (lowerName.endsWith(".lz4") || lowerName.endsWith(".snappy") || lowerName.endsWith(".zstd")
        || lowerName.endsWith(".zst")) {
      InputStream viaHadoop = decompressViaHadoopCodec(rawStream, fileName);
      if (viaHadoop != null) {
        return viaHadoop;
      }
      throw new IOException("Cannot decompress " + fileName + ": no Hadoop compression codec is "
          + "available for it on the classpath. Decompress the log first, or run with hadoop-common "
          + "and the matching codec library present.");
    }
    return rawStream;
  }

  private InputStream decompressViaHadoopCodec(InputStream rawStream, String fileName) {
    try {
      Class<?> factoryClass = Class.forName("org.apache.hadoop.io.compress.CompressionCodecFactory");
      Class<?> confClass = Class.forName("org.apache.hadoop.conf.Configuration");
      Class<?> pathClass = Class.forName("org.apache.hadoop.fs.Path");

      Object configuration = confClass.getDeclaredConstructor().newInstance();
      Object factory = factoryClass.getDeclaredConstructor(confClass).newInstance(configuration);
      Object path = pathClass.getDeclaredConstructor(String.class).newInstance(fileName);

      Object codec = factoryClass.getMethod("getCodec", pathClass).invoke(factory, path);
      if (codec == null) {
        return null;
      }
      Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.CompressionCodec");
      return (InputStream) codecClass.getMethod("createInputStream", InputStream.class)
          .invoke(codec, rawStream);
    } catch (ReflectiveOperationException | RuntimeException e) {
      log.debug("Hadoop compression codec lookup failed for {}", fileName, e);
      return null;
    }
  }

  private void handleEvent(JsonNode event) {
    String eventType = text(event, "Event");
    if (eventType == null) {
      return;
    }

    switch (eventType) {
      case "SparkListenerApplicationStart":
        analysis.setAppName(text(event, "App Name"));
        analysis.setAppId(text(event, "App ID"));
        analysis.setStartTime(longAt(event, "Timestamp", -1L));
        break;
      case "SparkListenerApplicationEnd":
        analysis.setEndTime(longAt(event, "Timestamp", -1L));
        break;
      case "SparkListenerEnvironmentUpdate":
        handleEnvironmentUpdate(event);
        break;
      case "SparkListenerJobStart":
        handleJobStart(event);
        break;
      case "SparkListenerJobEnd":
        handleJobEnd(event);
        break;
      case "SparkListenerStageSubmitted":
        handleStageSubmitted(event);
        break;
      case "SparkListenerStageCompleted":
        handleStageCompleted(event);
        break;
      case "SparkListenerTaskEnd":
        handleTaskEnd(event);
        break;
      case "SparkListenerExecutorAdded":
        analysis.recordExecutorAdded(text(event, "Executor ID"), longAt(event, "Timestamp", -1L));
        break;
      case "SparkListenerExecutorRemoved":
        analysis.recordExecutorRemoved(text(event, "Executor ID"), longAt(event, "Timestamp", -1L),
            text(event, "Removed Reason"));
        break;
      default:
        break;
    }
  }

  private void handleEnvironmentUpdate(JsonNode event) {
    JsonNode sparkProperties = event.path("Spark Properties");
    if (sparkProperties.isObject()) {
      Map<String, String> properties = new LinkedHashMap<>();
      sparkProperties.fieldNames().forEachRemaining(key -> properties.put(key, sparkProperties.path(key).asText("")));
      analysis.setSparkProperties(properties);
    }
  }

  private void handleJobStart(JsonNode event) {
    int jobId = intAt(event, "Job ID", -1);
    JsonNode properties = event.path("Properties");
    String description = properties.isObject() ? text(properties, "spark.job.description") : null;
    String jobGroup = properties.isObject() ? text(properties, "spark.jobGroup.id") : null;

    if (description != null) {
      jobDescriptions.put(jobId, description);
    }
    String module = HudiPhaseResolver.resolveModule(description, jobGroup);
    if (module != null) {
      jobModules.put(jobId, module);
    }

    for (JsonNode stageId : event.path("Stage IDs")) {
      stageToJob.put(stageId.asInt(), jobId);
    }
    analysis.recordJobStart(jobId, longAt(event, "Submission Time", -1L));
  }

  private void handleJobEnd(JsonNode event) {
    int jobId = intAt(event, "Job ID", -1);
    String result = event.path("Job Result").path("Result").asText("");
    analysis.recordJobEnd(jobId, longAt(event, "Completion Time", -1L), result);
  }

  private void handleStageSubmitted(JsonNode event) {
    JsonNode stageInfo = event.path("Stage Info");
    StageStats stage = stageFor(stageInfo);
    stage.setName(text(stageInfo, "Stage Name"));
    stage.setNumTasksPlanned(intAt(stageInfo, "Number of Tasks", 0));

    long submissionTime = longAt(stageInfo, "Submission Time", -1L);
    if (submissionTime < 0) {
      submissionTime = longAt(event, "Submission Time", -1L);
    }
    if (submissionTime >= 0) {
      stage.setSubmissionTime(submissionTime);
    }
  }

  private void handleStageCompleted(JsonNode event) {
    JsonNode stageInfo = event.path("Stage Info");
    StageStats stage = stageFor(stageInfo);
    if (stage.getName().isEmpty()) {
      stage.setName(text(stageInfo, "Stage Name"));
    }
    if (stage.getNumTasksPlanned() == 0) {
      stage.setNumTasksPlanned(intAt(stageInfo, "Number of Tasks", 0));
    }
    if (stage.getSubmissionTime() < 0) {
      stage.setSubmissionTime(longAt(stageInfo, "Submission Time", -1L));
    }
    stage.setCompletionTime(longAt(stageInfo, "Completion Time", -1L));

    String failureReason = text(stageInfo, "Failure Reason");
    if (failureReason != null && !failureReason.isEmpty()) {
      stage.setFailureReason(firstLine(failureReason));
    }
  }

  private void handleTaskEnd(JsonNode event) {
    int stageId = intAt(event, "Stage ID", -1);
    int attemptId = intAt(event, "Stage Attempt ID", 0);
    StageStats stage = stageFor(stageId, attemptId);

    JsonNode taskInfo = event.path("Task Info");
    long launchTime = longAt(taskInfo, "Launch Time", -1L);
    long finishTime = longAt(taskInfo, "Finish Time", -1L);
    long duration = launchTime >= 0 && finishTime >= launchTime ? finishTime - launchTime : -1L;

    boolean failed = taskInfo.path("Failed").asBoolean(false);
    boolean killed = taskInfo.path("Killed").asBoolean(false);
    boolean speculative = taskInfo.path("Speculative").asBoolean(false);

    stage.addTask(duration, failed, killed, speculative, taskFailureLabel(event));

    JsonNode metrics = event.path("Task Metrics");
    if (metrics.isObject()) {
      stage.addMetrics(
          longAt(metrics, "Executor Run Time", 0L),
          longAt(metrics, "Executor Deserialize Time", 0L),
          longAt(metrics, "JVM GC Time", 0L),
          longAt(metrics, "Result Size", 0L),
          longAt(metrics, "Memory Bytes Spilled", 0L),
          longAt(metrics, "Disk Bytes Spilled", 0L));

      JsonNode input = metrics.path("Input Metrics");
      JsonNode output = metrics.path("Output Metrics");
      stage.addInputOutput(
          longAt(input, "Bytes Read", 0L),
          longAt(input, "Records Read", 0L),
          longAt(output, "Bytes Written", 0L),
          longAt(output, "Records Written", 0L));

      JsonNode shuffleRead = metrics.path("Shuffle Read Metrics");
      JsonNode shuffleWrite = metrics.path("Shuffle Write Metrics");
      stage.addShuffle(
          longAt(shuffleRead, "Remote Bytes Read", 0L) + longAt(shuffleRead, "Local Bytes Read", 0L),
          longAt(shuffleRead, "Total Records Read", longAt(shuffleRead, "Records Read", 0L)),
          longAt(shuffleRead, "Fetch Wait Time", 0L),
          longAt(shuffleWrite, "Shuffle Bytes Written", 0L),
          longAt(shuffleWrite, "Shuffle Records Written", 0L),
          longAt(shuffleWrite, "Shuffle Write Time", 0L));
    }
  }

  /**
   * A short, groupable label for why a task ended badly: the reason, qualified by the exception class
   * for {@code ExceptionFailure} so that an OOM and a timeout do not collapse into one bucket.
   */
  private static String taskFailureLabel(JsonNode event) {
    JsonNode endReason = event.path("Task End Reason");
    String reason = text(endReason, "Reason");
    if (reason == null || "Success".equals(reason)) {
      return null;
    }
    String className = text(endReason, "Class Name");
    if (className != null && !className.isEmpty()) {
      return reason + ": " + className;
    }
    return reason;
  }

  private StageStats stageFor(JsonNode stageInfo) {
    return stageFor(intAt(stageInfo, "Stage ID", -1), intAt(stageInfo, "Stage Attempt ID", 0));
  }

  private StageStats stageFor(int stageId, int attemptId) {
    return stagesByAttempt.computeIfAbsent(stageId + "." + attemptId,
        key -> new StageStats(stageId, attemptId));
  }

  /**
   * Joins stages to their job description, phase and operation, then hands the stage list to the
   * analysis. Phase comes from the description alone; operation needs the whole job, and restore
   * detection needs the whole application, so this runs in three passes.
   */
  private void finishAnalysis() {
    List<StageStats> stages = new ArrayList<>(stagesByAttempt.values());
    stages.sort(Comparator.comparingInt(StageStats::getStageId).thenComparingInt(StageStats::getAttemptId));

    assignPhases(stages);
    flagLargeCompositeBuckets(stages);
    assignOperations(stages);

    analysis.setStages(stages);
    analysis.setWarnings(warnings);
  }

  /**
   * Pass one: every stage gets its job description, module, phase and sub-phase.
   *
   * <p>The job description is the primary signal. When it says nothing -- several Hudi code paths,
   * notably the record-index lookups, set no job status at all -- the Spark stage name is consulted
   * as a fallback, because Spark names a stage after the class and line that created the RDD.
   */
  private void assignPhases(List<StageStats> stages) {
    for (StageStats stage : stages) {
      Integer jobId = stageToJob.get(stage.getStageId());
      stage.setJobId(jobId);
      String description = jobId == null ? null : jobDescriptions.get(jobId);
      if (jobId != null) {
        stage.setJobDescription(description);
        stage.setModule(jobModules.get(jobId));
      }

      HudiPhaseResolver.Match match = HudiPhaseResolver.resolveMatch(description);
      if (match.getPhase() == HudiPhase.UNKNOWN) {
        HudiPhaseResolver.Match fromStageName = HudiPhaseResolver.resolveFromStageName(stage.getName());
        if (fromStageName.getPhase() != HudiPhase.UNKNOWN || fromStageName.getSubPhase() != null) {
          match = fromStageName;
        }
      }
      stage.setPhase(match.getPhase());

      // The description decides the phase; the stage name can only refine the sub-phase within it,
      // naming which component actually ran inside a job that covers several.
      String refinedSubPhase = HudiPhaseResolver.refineSubPhase(match.getPhase(), stage.getName());
      stage.setSubPhase(refinedSubPhase != null ? refinedSubPhase : match.getSubPhase());
    }
  }

  /**
   * Pass two: flag the buckets whose name already says they bundle untagged work, when they are big
   * enough for that bundling to matter to a reader.
   *
   * <p>{@link HudiPhase#SOURCE_READ_AND_TRANSFORM} and {@link HudiPhase#DEDUP_AND_INDEX_TAGGING} are
   * named for everything they contain, because Hudi leaves the transformer, record creation and
   * deduplication untagged and Spark's laziness forces their cost into the neighbouring stage. No
   * heuristic can separate them, and inventing one would present a guess as a measurement.
   *
   * <p>What this pass adds is emphasis, not information: when such a stage takes a material share of
   * wall clock, it is marked so the report can spell out in words what the number covers. Below the
   * threshold the note is omitted, because it only earns its place when the number is big enough to
   * act on.
   */
  private void flagLargeCompositeBuckets(List<StageStats> stages) {
    long totalStageWallClock = 0;
    for (StageStats stage : stages) {
      if (stage.getDurationMillis() > 0) {
        totalStageWallClock += stage.getDurationMillis();
      }
    }
    if (totalStageWallClock <= 0) {
      return;
    }

    for (StageStats stage : stages) {
      if (stage.getDurationMillis() <= 0) {
        continue;
      }
      List<String> bundled = BUNDLED_STEPS.get(stage.getPhase());
      if (bundled == null) {
        continue;
      }
      double share = (double) stage.getDurationMillis() / totalStageWallClock;
      if (share >= COMPOSITE_BUCKET_SHARE_THRESHOLD) {
        stage.setBundledSteps(bundled);
      }
    }
  }

  /**
   * Pass three: resolve the operation per job from the module and the job's phase sequence, then
   * promote repeated rollbacks across the application to a restore.
   */
  private void assignOperations(List<StageStats> stages) {
    Map<Integer, List<HudiPhase>> phasesByJob = new LinkedHashMap<>();
    Map<Integer, List<String>> subPhasesByJob = new LinkedHashMap<>();
    for (StageStats stage : stages) {
      if (stage.getJobId() != null) {
        phasesByJob.computeIfAbsent(stage.getJobId(), key -> new ArrayList<>()).add(stage.getPhase());
        subPhasesByJob.computeIfAbsent(stage.getJobId(), key -> new ArrayList<>()).add(stage.getSubPhase());
      }
    }

    Map<Integer, HudiOperationResolver.Resolution> resolutionsByJob = new LinkedHashMap<>();
    List<Integer> rollbackJobIds = new ArrayList<>();
    for (Map.Entry<Integer, List<HudiPhase>> entry : phasesByJob.entrySet()) {
      HudiOperationResolver.Resolution resolution = HudiOperationResolver.resolve(
          jobModules.get(entry.getKey()), entry.getValue(), subPhasesByJob.get(entry.getKey()));
      resolutionsByJob.put(entry.getKey(), resolution);
      if (resolution.getOperation() == HudiOperation.ROLLBACK) {
        rollbackJobIds.add(entry.getKey());
      }
    }

    HudiOperationResolver.promoteRepeatedRollbacksToRestore(resolutionsByJob, rollbackJobIds);

    for (StageStats stage : stages) {
      HudiOperationResolver.Resolution resolution = resolutionsByJob.get(stage.getJobId());
      if (resolution != null) {
        stage.setOperation(resolution.getOperation());
        stage.setOperationSource(resolution.getSource());
      }
    }
  }

  private void addWarning(String warning) {
    warnings.add(warning);
    log.warn(warning);
  }

  private static String firstLine(String value) {
    int newline = value.indexOf('\n');
    return newline < 0 ? value : value.substring(0, newline);
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }

  private static long longAt(JsonNode node, String field, long defaultValue) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? defaultValue : value.asLong(defaultValue);
  }

  private static int intAt(JsonNode node, String field, int defaultValue) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? defaultValue : value.asInt(defaultValue);
  }
}
