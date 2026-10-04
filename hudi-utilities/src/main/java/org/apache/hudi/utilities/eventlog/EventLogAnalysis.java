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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * The parsed result of one Spark event log: application-level totals, the per-phase breakdown, and
 * the per-stage detail.
 */
public class EventLogAnalysis {

  /**
   * Application wall-clock attributable to one group of stages: a Hudi phase, or a Hudi operation.
   * Both summaries have the same shape, so they share this type and differ only in their label.
   */
  public static class GroupSummary {
    private final String label;
    private long stageWallClockMillis;
    private long executorRunTimeMillis;
    private int stageCount;
    private long taskCount;
    private long shuffleBytes;
    private long spilledBytes;

    GroupSummary(String label) {
      this.label = label;
    }

    /** The phase or operation name this row summarises. */
    public String getLabel() {
      return label;
    }

    public long getStageWallClockMillis() {
      return stageWallClockMillis;
    }

    public long getExecutorRunTimeMillis() {
      return executorRunTimeMillis;
    }

    public int getStageCount() {
      return stageCount;
    }

    public long getTaskCount() {
      return taskCount;
    }

    public long getShuffleBytes() {
      return shuffleBytes;
    }

    public long getSpilledBytes() {
      return spilledBytes;
    }
  }

  /** One job's start, end and result, used for the critical-path computation. */
  private static class JobRecord {
    private long submissionTime = -1L;
    private long completionTime = -1L;
    private String result = "";
  }

  private String appName;
  private String appId;
  private long startTime = -1L;
  private long endTime = -1L;

  private Map<String, String> sparkProperties = Collections.emptyMap();
  private List<StageStats> stages = new ArrayList<>();
  private List<String> warnings = new ArrayList<>();
  private long totalEventsRead;

  private final Map<Integer, JobRecord> jobs = new LinkedHashMap<>();

  /** Executor add/remove timestamps, used to derive peak concurrency. */
  private final List<long[]> executorEvents = new ArrayList<>();
  private final Map<String, String> executorRemovalReasons = new TreeMap<>();
  private final Map<String, Long> liveExecutors = new HashMap<>();

  /**
   * Total application wall clock. Falls back to the span of observed stage activity when the log has
   * no application start/end pair, which happens for a log from a killed application.
   */
  public long getDurationMillis() {
    if (startTime >= 0 && endTime >= startTime) {
      return endTime - startTime;
    }
    long earliest = Long.MAX_VALUE;
    long latest = Long.MIN_VALUE;
    for (StageStats stage : stages) {
      if (stage.getSubmissionTime() >= 0) {
        earliest = Math.min(earliest, stage.getSubmissionTime());
      }
      if (stage.getCompletionTime() >= 0) {
        latest = Math.max(latest, stage.getCompletionTime());
      }
    }
    return earliest == Long.MAX_VALUE || latest == Long.MIN_VALUE ? -1L : latest - earliest;
  }

  /**
   * A floor on achievable wall clock: for each job, the longest single stage in it, summed over jobs.
   *
   * <p>Jobs in a Hudi write run essentially back to back, and within a job the stages that matter run
   * in a dependency chain, so no amount of extra parallelism can bring the run below this. Comparing
   * it to the actual duration says how much headroom scheduling and job overhead are costing.
   */
  public long getCriticalPathMillis() {
    Map<Integer, Long> longestStagePerJob = new TreeMap<>();
    for (StageStats stage : stages) {
      if (stage.getJobId() == null || stage.getDurationMillis() < 0) {
        continue;
      }
      longestStagePerJob.merge(stage.getJobId(), stage.getDurationMillis(), Math::max);
    }
    return longestStagePerJob.values().stream().mapToLong(Long::longValue).sum();
  }

  /** Per-phase totals, ordered by descending wall clock so the headline phase comes first. */
  public List<GroupSummary> getPhaseSummaries() {
    return summarise(stage -> stage.getPhase().name());
  }

  /** Per-operation totals, ordered by descending wall clock. The level above the phase summary. */
  public List<GroupSummary> getOperationSummaries() {
    return summarise(stage -> stage.getOperation().name());
  }

  /** Groups stages by the given label and totals each group, descending by wall clock then name. */
  private List<GroupSummary> summarise(Function<StageStats, String> labeller) {
    Map<String, GroupSummary> byLabel = new TreeMap<>();
    for (StageStats stage : stages) {
      GroupSummary summary = byLabel.computeIfAbsent(labeller.apply(stage), GroupSummary::new);
      summary.stageCount++;
      summary.taskCount += stage.getNumTasksObserved();
      summary.executorRunTimeMillis += stage.getExecutorRunTimeMillis();
      summary.shuffleBytes += stage.getShuffleReadBytes() + stage.getShuffleWriteBytes();
      summary.spilledBytes += stage.getMemoryBytesSpilled() + stage.getDiskBytesSpilled();
      if (stage.getDurationMillis() > 0) {
        summary.stageWallClockMillis += stage.getDurationMillis();
      }
    }

    List<GroupSummary> summaries = new ArrayList<>(byLabel.values());
    summaries.sort((left, right) -> {
      int byWallClock = Long.compare(right.stageWallClockMillis, left.stageWallClockMillis);
      return byWallClock != 0 ? byWallClock : left.label.compareTo(right.label);
    });
    return summaries;
  }

  /** Share of stage wall clock in UNKNOWN above which the streaming-write caveat is worth raising. */
  private static final double UNATTRIBUTED_SHARE_THRESHOLD = 0.25;

  /**
   * Warns when a large unattributed share is explained by streaming writes to the metadata table.
   *
   * <p>With {@code hoodie.metadata.streaming.write.enabled=true}, Hudi's default on Spark, the
   * metadata-table write path runs without any {@code setJobStatus}, so its stages carry no
   * description to interpret. On a measured run this accounted for 61% of stage wall clock. The tool
   * does not invent an attribution for it; it says plainly that the reported shares understate the
   * write path, and names the config so a user can confirm.
   */
  private void addStreamingMetadataWriteCaveat(List<String> caveats) {
    long totalWallClock = 0;
    long unattributedWallClock = 0;
    boolean sawStreamingMetadataWrite = false;

    for (StageStats stage : stages) {
      if (stage.getDurationMillis() > 0) {
        totalWallClock += stage.getDurationMillis();
        if (stage.getPhase() == HudiPhase.UNKNOWN) {
          unattributedWallClock += stage.getDurationMillis();
        }
      }
      if (HudiPhaseResolver.STREAMING_METADATA_WRITE_SUB_PHASE.equals(stage.getSubPhase())) {
        sawStreamingMetadataWrite = true;
      }
    }

    if (!sawStreamingMetadataWrite || totalWallClock <= 0) {
      return;
    }
    if ((double) unattributedWallClock / totalWallClock < UNATTRIBUTED_SHARE_THRESHOLD) {
      return;
    }

    caveats.add(String.format(
        "Streaming writes to the metadata table appear to be enabled (hoodie.metadata.streaming.write"
            + ".enabled, Hudi's default on Spark). Hudi sets no job description on that path, so its "
            + "stages cannot be attributed: %.0f%% of stage wall clock here is UNKNOWN, and the "
            + "reported phase shares therefore understate the write path. Stages on this path carry "
            + "subPhase \"%s\". Set that config to false to get a fully attributable breakdown.",
        100.0 * unattributedWallClock / totalWallClock,
        HudiPhaseResolver.STREAMING_METADATA_WRITE_SUB_PHASE));
  }

  /**
   * Caveats about how far the phase attribution can be trusted for this particular log.
   *
   * <p>A phase names where work was <em>forced</em>, not where it logically belongs: Hudi sets the
   * job description before the Spark action and Spark evaluates lazily, so a phase can absorb the
   * cost of an untagged step that ran before it. These rows name the phases in this log that are
   * known to be prone to that, so a downstream optimizer does not treat a single share as ground
   * truth. Ordering is deterministic.
   */
  public List<String> getCaveats() {
    List<String> caveats = new ArrayList<>();
    Set<HudiPhase> present = EnumSet.noneOf(HudiPhase.class);
    for (StageStats stage : stages) {
      present.add(stage.getPhase());
    }

    // This one leads when it fires: it is the only caveat that names a config a user can change, and
    // it bounds how far the whole breakdown can be trusted.
    addStreamingMetadataWriteCaveat(caveats);

    caveats.add("A bucket names where work was forced, not where it logically belongs. Hudi sets the "
        + "job description before the Spark action and Spark evaluates lazily, so a bucket can carry "
        + "the cost of an untagged step that ran before it. Read a share as an attribution, not as a "
        + "measurement of one step.");

    if (present.contains(HudiPhase.SOURCE_READ_AND_TRANSFORM)) {
      caveats.add("SOURCE_READ_AND_TRANSFORM covers source read, user transformation and record "
          + "creation together: StreamSync applies the transformer untagged and HoodieStreamerUtils "
          + "sets no job status at all, so none of the three can be separated from this log.");
    }
    if (present.contains(HudiPhase.DEDUP_AND_INDEX_TAGGING)) {
      caveats.add("DEDUP_AND_INDEX_TAGGING covers deduplication and tagging together: BaseWriteHelper "
          + "tags only the tagging step and deduplicateRecords runs untagged immediately before it.");
    }
    if (present.contains(HudiPhase.DATA_TABLE_WRITE)) {
      caveats.add("DATA_TABLE_WRITE includes the small-file probe, and can also absorb earlier write "
          + "work that was not forced until the commit action ran. The subPhase on each stage says "
          + "which part of the bucket it was.");
    }
    if (present.contains(HudiPhase.UNKNOWN)) {
      caveats.add("Some stages carry no recognised job description and are reported as UNKNOWN; they "
          + "may be Hudi work that is not instrumented, or work from the surrounding application. A "
          + "job description of just \":\" is one known case, seen in practice on the source read.");
    }
    if (present.contains(HudiPhase.ARCHIVAL)) {
      caveats.add("ARCHIVAL is mostly driver-side and normally absent or trivial in a Spark log; a "
          + "non-trivial duration here is itself worth investigating.");
    }
    return caveats;
  }

  /**
   * Highest number of executors alive at once, derived by replaying add and remove events in
   * timestamp order. The driver is not counted; Spark does not emit an ExecutorAdded for it.
   */
  public int getPeakConcurrentExecutors() {
    List<long[]> sorted = new ArrayList<>(executorEvents);
    // Sort by timestamp, and on a tie process removals before additions so the peak is not inflated
    // by an executor that was replaced at the same millisecond.
    sorted.sort((left, right) -> {
      int byTime = Long.compare(left[0], right[0]);
      return byTime != 0 ? byTime : Long.compare(left[1], right[1]);
    });

    int live = 0;
    int peak = 0;
    for (long[] event : sorted) {
      live += (int) event[1];
      peak = Math.max(peak, live);
    }
    return peak;
  }

  public long getTotalTaskCount() {
    return stages.stream().mapToLong(StageStats::getNumTasksObserved).sum();
  }

  public long getTotalExecutorRunTimeMillis() {
    return stages.stream().mapToLong(StageStats::getExecutorRunTimeMillis).sum();
  }

  public long getTotalShuffleReadBytes() {
    return stages.stream().mapToLong(StageStats::getShuffleReadBytes).sum();
  }

  public long getTotalShuffleWriteBytes() {
    return stages.stream().mapToLong(StageStats::getShuffleWriteBytes).sum();
  }

  public long getTotalMemoryBytesSpilled() {
    return stages.stream().mapToLong(StageStats::getMemoryBytesSpilled).sum();
  }

  public long getTotalDiskBytesSpilled() {
    return stages.stream().mapToLong(StageStats::getDiskBytesSpilled).sum();
  }

  public long getTotalFailedTaskCount() {
    return stages.stream().mapToLong(StageStats::getTasksFailed).sum();
  }

  public int getFailedStageCount() {
    return (int) stages.stream().filter(stage -> stage.getFailureReason() != null).count();
  }

  public int getJobCount() {
    return jobs.size();
  }

  public int getFailedJobCount() {
    return (int) jobs.values().stream().filter(job -> !"JobSucceeded".equals(job.result)).count();
  }

  void recordJobStart(int jobId, long submissionTime) {
    jobs.computeIfAbsent(jobId, key -> new JobRecord()).submissionTime = submissionTime;
  }

  void recordJobEnd(int jobId, long completionTime, String result) {
    JobRecord job = jobs.computeIfAbsent(jobId, key -> new JobRecord());
    job.completionTime = completionTime;
    job.result = result == null ? "" : result;
  }

  void recordExecutorAdded(String executorId, long timestamp) {
    if (timestamp < 0) {
      return;
    }
    executorEvents.add(new long[] {timestamp, 1});
    if (executorId != null) {
      liveExecutors.put(executorId, timestamp);
    }
  }

  void recordExecutorRemoved(String executorId, long timestamp, String reason) {
    if (timestamp >= 0) {
      executorEvents.add(new long[] {timestamp, -1});
    }
    if (executorId != null) {
      liveExecutors.remove(executorId);
      if (reason != null && !reason.isEmpty()) {
        executorRemovalReasons.put(executorId, reason.split("\n", 2)[0]);
      }
    }
  }

  void incrementEventsRead() {
    totalEventsRead++;
  }

  public Map<String, String> getExecutorRemovalReasons() {
    return Collections.unmodifiableMap(executorRemovalReasons);
  }

  public String getAppName() {
    return appName;
  }

  void setAppName(String appName) {
    this.appName = appName;
  }

  public String getAppId() {
    return appId;
  }

  void setAppId(String appId) {
    this.appId = appId;
  }

  public long getStartTime() {
    return startTime;
  }

  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public Map<String, String> getSparkProperties() {
    return sparkProperties;
  }

  void setSparkProperties(Map<String, String> sparkProperties) {
    this.sparkProperties = sparkProperties;
  }

  public List<StageStats> getStages() {
    return stages;
  }

  void setStages(List<StageStats> stages) {
    this.stages = stages;
  }

  public List<String> getWarnings() {
    return warnings;
  }

  void setWarnings(List<String> warnings) {
    this.warnings = warnings;
  }

  public long getTotalEventsRead() {
    return totalEventsRead;
  }
}
