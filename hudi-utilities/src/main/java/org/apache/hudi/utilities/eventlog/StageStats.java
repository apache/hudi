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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Accumulated metrics for one stage attempt, joined to the Hudi phase of the job that owns it.
 *
 * <p>Task durations are collected so that percentiles can be computed once the stage is complete;
 * everything else is summed as tasks end.
 */
public class StageStats {

  private final int stageId;
  private final int attemptId;
  private String name = "";

  private Integer jobId;
  private String jobDescription;
  private String module;
  private HudiPhase phase = HudiPhase.UNKNOWN;
  private String subPhase;
  private List<String> bundledSteps = Collections.emptyList();
  private HudiOperation operation = HudiOperation.UNKNOWN;
  private HudiOperation.Source operationSource = HudiOperation.Source.NONE;

  private long submissionTime = -1L;
  private long completionTime = -1L;
  private int numTasksPlanned;
  private String failureReason;

  private final List<Long> taskDurations = new ArrayList<>();
  private long executorRunTimeMillis;
  private long executorDeserializeTimeMillis;
  private long jvmGcTimeMillis;
  private long resultSizeBytes;
  private long memoryBytesSpilled;
  private long diskBytesSpilled;

  private long inputBytes;
  private long inputRecords;
  private long outputBytes;
  private long outputRecords;

  private long shuffleReadBytes;
  private long shuffleReadRecords;
  private long shuffleFetchWaitTimeMillis;
  private long shuffleWriteBytes;
  private long shuffleWriteRecords;
  private long shuffleWriteTimeNanos;

  private int tasksSucceeded;
  private int tasksFailed;
  private int tasksKilled;
  private int tasksSpeculative;

  /** Distinct task failure reasons, with how many tasks hit each. Sorted for deterministic output. */
  private final Map<String, Integer> failureReasonCounts = new TreeMap<>();

  public StageStats(int stageId, int attemptId) {
    this.stageId = stageId;
    this.attemptId = attemptId;
  }

  /**
   * Folds one completed task into this stage.
   *
   * @param durationMillis wall-clock task duration, or a negative value when it could not be derived.
   */
  public void addTask(long durationMillis, boolean failed, boolean killed, boolean speculative, String failureClass) {
    if (durationMillis >= 0) {
      taskDurations.add(durationMillis);
    }
    if (failed) {
      tasksFailed++;
    } else if (killed) {
      tasksKilled++;
    } else {
      tasksSucceeded++;
    }
    if (speculative) {
      tasksSpeculative++;
    }
    if (failureClass != null && !failureClass.isEmpty()) {
      failureReasonCounts.merge(failureClass, 1, Integer::sum);
    }
  }

  public void addMetrics(long runTime, long deserializeTime, long gcTime, long resultSize,
                         long memorySpilled, long diskSpilled) {
    executorRunTimeMillis += runTime;
    executorDeserializeTimeMillis += deserializeTime;
    jvmGcTimeMillis += gcTime;
    resultSizeBytes += resultSize;
    memoryBytesSpilled += memorySpilled;
    diskBytesSpilled += diskSpilled;
  }

  public void addInputOutput(long inBytes, long inRecords, long outBytes, long outRecords) {
    inputBytes += inBytes;
    inputRecords += inRecords;
    outputBytes += outBytes;
    outputRecords += outRecords;
  }

  public void addShuffle(long readBytes, long readRecords, long fetchWaitMillis,
                         long writeBytes, long writeRecords, long writeTimeNanos) {
    shuffleReadBytes += readBytes;
    shuffleReadRecords += readRecords;
    shuffleFetchWaitTimeMillis += fetchWaitMillis;
    shuffleWriteBytes += writeBytes;
    shuffleWriteRecords += writeRecords;
    shuffleWriteTimeNanos += writeTimeNanos;
  }

  /**
   * Wall-clock duration of the stage, in milliseconds, or -1 when the log did not record both ends
   * (routine for the stage that was running when an application was killed).
   */
  public long getDurationMillis() {
    return submissionTime >= 0 && completionTime >= submissionTime ? completionTime - submissionTime : -1L;
  }

  public long getTaskDurationPercentile(double percentile) {
    if (taskDurations.isEmpty()) {
      return -1L;
    }
    List<Long> sorted = new ArrayList<>(taskDurations);
    Collections.sort(sorted);
    int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
    return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
  }

  public long getMaxTaskDurationMillis() {
    return taskDurations.stream().mapToLong(Long::longValue).max().orElse(-1L);
  }

  /**
   * Max task duration over median task duration: the standard one-number read on stage skew. Returns
   * -1 when there are no tasks, and -1 when the median is zero (the ratio is undefined, and a huge
   * number there would be read as catastrophic skew when the stage is simply very fast).
   */
  public double getSkewRatio() {
    long median = getTaskDurationPercentile(50);
    long max = getMaxTaskDurationMillis();
    if (median <= 0 || max < 0) {
      return -1.0;
    }
    return (double) max / median;
  }

  /** GC time as a fraction of executor run time, or -1 when no run time was recorded. */
  public double getGcFraction() {
    return executorRunTimeMillis > 0 ? (double) jvmGcTimeMillis / executorRunTimeMillis : -1.0;
  }

  public int getNumTasksObserved() {
    return tasksSucceeded + tasksFailed + tasksKilled;
  }

  public Map<String, Integer> getFailureReasonCounts() {
    return new LinkedHashMap<>(failureReasonCounts);
  }

  public int getStageId() {
    return stageId;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name == null ? "" : name;
  }

  public Integer getJobId() {
    return jobId;
  }

  public void setJobId(Integer jobId) {
    this.jobId = jobId;
  }

  public String getJobDescription() {
    return jobDescription;
  }

  public void setJobDescription(String jobDescription) {
    this.jobDescription = jobDescription;
  }

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
  }

  public HudiPhase getPhase() {
    return phase;
  }

  public void setPhase(HudiPhase phase) {
    this.phase = phase;
  }

  /** A finer label within the phase, or null when the phase needs no subdivision. */
  public String getSubPhase() {
    return subPhase;
  }

  public void setSubPhase(String subPhase) {
    this.subPhase = subPhase;
  }

  /**
   * The untagged steps whose cost this stage's number also contains, spelled out in words. Empty
   * unless the stage is large enough for the bundling to matter; see {@code EventLogParser}. Hudi
   * sets no job status around these steps and Spark's laziness forces their cost here, so they
   * cannot be separated from an event log.
   */
  public List<String> getBundledSteps() {
    return bundledSteps;
  }

  public void setBundledSteps(List<String> bundledSteps) {
    this.bundledSteps = bundledSteps == null ? Collections.emptyList() : bundledSteps;
  }

  public HudiOperation getOperation() {
    return operation;
  }

  public void setOperation(HudiOperation operation) {
    this.operation = operation;
  }

  /** Which signal produced {@link #getOperation()}, so a reader can judge confidence. */
  public HudiOperation.Source getOperationSource() {
    return operationSource;
  }

  public void setOperationSource(HudiOperation.Source operationSource) {
    this.operationSource = operationSource;
  }

  public long getSubmissionTime() {
    return submissionTime;
  }

  public void setSubmissionTime(long submissionTime) {
    this.submissionTime = submissionTime;
  }

  public long getCompletionTime() {
    return completionTime;
  }

  public void setCompletionTime(long completionTime) {
    this.completionTime = completionTime;
  }

  public int getNumTasksPlanned() {
    return numTasksPlanned;
  }

  public void setNumTasksPlanned(int numTasksPlanned) {
    this.numTasksPlanned = numTasksPlanned;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public void setFailureReason(String failureReason) {
    this.failureReason = failureReason;
  }

  public long getExecutorRunTimeMillis() {
    return executorRunTimeMillis;
  }

  public long getExecutorDeserializeTimeMillis() {
    return executorDeserializeTimeMillis;
  }

  public long getJvmGcTimeMillis() {
    return jvmGcTimeMillis;
  }

  public long getResultSizeBytes() {
    return resultSizeBytes;
  }

  public long getMemoryBytesSpilled() {
    return memoryBytesSpilled;
  }

  public long getDiskBytesSpilled() {
    return diskBytesSpilled;
  }

  public long getInputBytes() {
    return inputBytes;
  }

  public long getInputRecords() {
    return inputRecords;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  public long getShuffleReadBytes() {
    return shuffleReadBytes;
  }

  public long getShuffleReadRecords() {
    return shuffleReadRecords;
  }

  public long getShuffleFetchWaitTimeMillis() {
    return shuffleFetchWaitTimeMillis;
  }

  public long getShuffleWriteBytes() {
    return shuffleWriteBytes;
  }

  public long getShuffleWriteRecords() {
    return shuffleWriteRecords;
  }

  public long getShuffleWriteTimeNanos() {
    return shuffleWriteTimeNanos;
  }

  public int getTasksSucceeded() {
    return tasksSucceeded;
  }

  public int getTasksFailed() {
    return tasksFailed;
  }

  public int getTasksKilled() {
    return tasksKilled;
  }

  public int getTasksSpeculative() {
    return tasksSpeculative;
  }
}
