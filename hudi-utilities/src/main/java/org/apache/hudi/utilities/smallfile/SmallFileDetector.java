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

package org.apache.hudi.utilities.smallfile;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/**
 * Stateless small-file pile-up detector — the reusable core extracted from {@code TableSizeStats}
 * so external callers can score a table without embarking on the full Spark utility. All state
 * comes in through arguments; nothing here reads or mutates instance state.
 *
 * <p>Detection has three pieces, each independently callable:
 * <ol>
 *   <li>{@link #isQualifying}/{@link #isFlagged} — per-partition classification. A partition
 *       qualifies once it has {@code minFilesPerPartition} base files, and is flagged when its
 *       average base-file size is below {@code thresholdBytes}. Callers that iterate partitions
 *       themselves (e.g., the monitoring job's partition-level extractor) drive these directly.</li>
 *   <li>{@link #classifyVerdict} — table-level tier from the aggregated
 *       {@code (qualifying, flagged, ingestCommitCount)} triplet. Below {@code minTableCommits}
 *       ingest commits the verdict is {@link Verdict#SKIPPED} (young-table gate — the signal is
 *       dominated by initial-write noise); above that, {@code MODERATE} at {@code >= moderatePct}
 *       and {@code SEVERE} at {@code >= severePct}.</li>
 *   <li>{@link #countTotalIngestCommits} — walks the active timeline (and, only if under the
 *       young-table gate, the archived timeline) counting completed ingest commits. Clustering
 *       replacecommits are excluded; INSERT_OVERWRITE(_TABLE) replacecommits are counted.</li>
 * </ol>
 *
 * <p>For callers that already have per-partition file-count/byte-total data, {@link #run} is a
 * one-shot convenience that folds the pieces together.
 */
public final class SmallFileDetector {

  private static final Logger LOG = LoggerFactory.getLogger(SmallFileDetector.class);

  private SmallFileDetector() {
  }

  /** Small-file verdict tiers based on the prevalence (flagged / qualifying ratio). */
  public enum Verdict {
    /** Table is healthy (or the flagged ratio is below the MODERATE gate). */
    CLEAN,
    /** {@code moderatePct <= flagged/qualifying < severePct}. */
    MODERATE,
    /** {@code flagged/qualifying >= severePct}. */
    SEVERE,
    /** Table has fewer than {@code minTableCommits} ingest commits — signal is unreliable. */
    SKIPPED
  }

  /**
   * Threshold configuration for the detector. Defaults mirror the {@code TableSizeStats} CLI
   * defaults so that programmatic callers get the same behavior as the standalone utility.
   * Immutable after construction; use {@link #defaults()} or {@link Builder}.
   */
  public static final class Config implements Serializable {
    /** Per-partition file-count gate for qualifying (default 5). */
    public static final int DEFAULT_MIN_FILES_PER_PARTITION = 5;
    /** Avg-file-size threshold (bytes) for flagging a qualifying partition (default 50 MB). */
    public static final long DEFAULT_THRESHOLD_BYTES = 50L * 1024 * 1024;
    /** flagged / qualifying ratio that triggers MODERATE (default 0.10). */
    public static final double DEFAULT_MODERATE_PCT = 0.10;
    /** flagged / qualifying ratio that triggers SEVERE (default 0.30). */
    public static final double DEFAULT_SEVERE_PCT = 0.30;
    /** Minimum total ingest commits before the verdict is scored (default 10). */
    public static final int DEFAULT_MIN_TABLE_COMMITS = 10;

    private final int minFilesPerPartition;
    private final long thresholdBytes;
    private final double moderatePct;
    private final double severePct;
    private final int minTableCommits;

    private Config(int minFilesPerPartition, long thresholdBytes, double moderatePct,
                   double severePct, int minTableCommits) {
      this.minFilesPerPartition = minFilesPerPartition;
      this.thresholdBytes = thresholdBytes;
      this.moderatePct = moderatePct;
      this.severePct = severePct;
      this.minTableCommits = minTableCommits;
    }

    public static Config defaults() {
      return new Builder().build();
    }

    public static Builder builder() {
      return new Builder();
    }

    public int getMinFilesPerPartition() {
      return minFilesPerPartition;
    }

    public long getThresholdBytes() {
      return thresholdBytes;
    }

    public double getModeratePct() {
      return moderatePct;
    }

    public double getSeverePct() {
      return severePct;
    }

    public int getMinTableCommits() {
      return minTableCommits;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Config)) {
        return false;
      }
      Config c = (Config) o;
      return minFilesPerPartition == c.minFilesPerPartition
          && thresholdBytes == c.thresholdBytes
          && Double.compare(moderatePct, c.moderatePct) == 0
          && Double.compare(severePct, c.severePct) == 0
          && minTableCommits == c.minTableCommits;
    }

    @Override
    public int hashCode() {
      return Objects.hash(minFilesPerPartition, thresholdBytes, moderatePct, severePct, minTableCommits);
    }

    public static final class Builder {
      private int minFilesPerPartition = DEFAULT_MIN_FILES_PER_PARTITION;
      private long thresholdBytes = DEFAULT_THRESHOLD_BYTES;
      private double moderatePct = DEFAULT_MODERATE_PCT;
      private double severePct = DEFAULT_SEVERE_PCT;
      private int minTableCommits = DEFAULT_MIN_TABLE_COMMITS;

      public Builder minFilesPerPartition(int v) {
        this.minFilesPerPartition = v;
        return this;
      }

      public Builder thresholdBytes(long v) {
        this.thresholdBytes = v;
        return this;
      }

      public Builder moderatePct(double v) {
        this.moderatePct = v;
        return this;
      }

      public Builder severePct(double v) {
        this.severePct = v;
        return this;
      }

      public Builder minTableCommits(int v) {
        this.minTableCommits = v;
        return this;
      }

      public Config build() {
        if (moderatePct > severePct) {
          throw new IllegalArgumentException(
              "moderatePct (" + moderatePct + ") must be <= severePct (" + severePct + ")");
        }
        return new Config(minFilesPerPartition, thresholdBytes, moderatePct, severePct, minTableCommits);
      }
    }
  }

  /**
   * Per-partition input for the detector. Callers construct one of these per partition from
   * whatever file-listing source they have (Hudi {@code HoodieTableFileSystemView} in the
   * monitoring job, an aggregated {@code PartitionRow} in {@code TableSizeStats}, …).
   */
  public static final class PartitionStats implements Serializable {
    private final String partition;
    private final long fileCount;
    private final long totalBytes;

    public PartitionStats(String partition, long fileCount, long totalBytes) {
      ValidationUtils.checkArgument(fileCount >= 0, "fileCount must be >= 0, got " + fileCount);
      ValidationUtils.checkArgument(totalBytes >= 0, "totalBytes must be >= 0, got " + totalBytes);
      this.partition = partition;
      this.fileCount = fileCount;
      this.totalBytes = totalBytes;
    }

    public String getPartition() {
      return partition;
    }

    public long getFileCount() {
      return fileCount;
    }

    public long getTotalBytes() {
      return totalBytes;
    }

    /** Integer average — matches the historical {@code TableSizeStats} behavior. */
    public long avgFileSize() {
      return fileCount == 0 ? 0L : totalBytes / fileCount;
    }
  }

  /** Aggregated detector output. */
  public static final class Result implements Serializable {
    private final Verdict verdict;
    private final long qualifyingPartitions;
    private final long flaggedPartitions;
    private final double flaggedPct;
    private final long tableIngestCommitCount;

    // Package-private — Results are outputs from the detector, not caller-constructed. Tests
    // that need to build a Result live in the same package.
    Result(Verdict verdict, long qualifyingPartitions, long flaggedPartitions,
           double flaggedPct, long tableIngestCommitCount) {
      this.verdict = verdict;
      this.qualifyingPartitions = qualifyingPartitions;
      this.flaggedPartitions = flaggedPartitions;
      this.flaggedPct = flaggedPct;
      this.tableIngestCommitCount = tableIngestCommitCount;
    }

    public Verdict getVerdict() {
      return verdict;
    }

    public long getQualifyingPartitions() {
      return qualifyingPartitions;
    }

    public long getFlaggedPartitions() {
      return flaggedPartitions;
    }

    public double getFlaggedPct() {
      return flaggedPct;
    }

    public long getTableIngestCommitCount() {
      return tableIngestCommitCount;
    }
  }

  // ---- per-partition classification ----

  /** A partition qualifies once it has at least {@code cfg.minFilesPerPartition} base files. */
  public static boolean isQualifying(PartitionStats p, Config cfg) {
    return p.getFileCount() >= cfg.getMinFilesPerPartition();
  }

  /**
   * A qualifying partition is flagged when its average base-file size is strictly below
   * {@code cfg.thresholdBytes}. Non-qualifying partitions are never flagged, regardless of
   * average size — this mirrors {@code TableSizeStats}.
   */
  public static boolean isFlagged(PartitionStats p, Config cfg) {
    return isQualifying(p, cfg) && p.avgFileSize() < cfg.getThresholdBytes();
  }

  // ---- table-level classification ----

  /**
   * Classifies the table-level verdict from aggregated counts. Order of precedence:
   * <ol>
   *   <li>{@code SKIPPED} — when {@code tableIngestCommitCount < cfg.minTableCommits}. Beats
   *       everything else, even a fully-flagged table.</li>
   *   <li>{@code CLEAN} — when {@code qualifyingPartitions == 0} (nothing to score).</li>
   *   <li>{@code SEVERE} — when flagged/qualifying ≥ {@code cfg.severePct}.</li>
   *   <li>{@code MODERATE} — when flagged/qualifying ≥ {@code cfg.moderatePct}.</li>
   *   <li>{@code CLEAN} — otherwise.</li>
   * </ol>
   *
   * @param tableIngestCommitCount total ingest commits (active + archived)
   * @param qualifyingPartitions   partitions with {@code fileCount ≥ cfg.minFilesPerPartition}
   * @param flaggedPartitions      qualifying partitions with {@code avgFileSize < cfg.thresholdBytes}
   */
  public static Verdict classifyVerdict(long tableIngestCommitCount,
                                        long qualifyingPartitions,
                                        long flaggedPartitions,
                                        Config cfg) {
    if (tableIngestCommitCount < cfg.getMinTableCommits()) {
      return Verdict.SKIPPED;
    }
    if (qualifyingPartitions == 0) {
      return Verdict.CLEAN;
    }
    double flaggedPct = (double) flaggedPartitions / qualifyingPartitions;
    if (flaggedPct >= cfg.getSeverePct()) {
      return Verdict.SEVERE;
    }
    if (flaggedPct >= cfg.getModeratePct()) {
      return Verdict.MODERATE;
    }
    return Verdict.CLEAN;
  }

  // ---- commit counting ----

  /**
   * Counts completed ingest commits ({@code commit} + {@code deltacommit} + real-ingest
   * {@code replacecommit}) across the active and, only when needed, the archived timeline.
   * Clustering replacecommits are excluded so frequent clustering can't mask the young-table
   * gate on a genuinely low-commit table.
   *
   * <p>The archived timeline is only scanned when the active timeline alone has fewer than
   * {@code cfg.minTableCommits} ingest commits — mature tables never pay for the archive read.
   * A read failure is logged and returns 0, yielding a SKIPPED verdict rather than a
   * misleading tier.
   */
  public static long countTotalIngestCommits(HoodieTableMetaClient metaClient, Config cfg) {
    try {
      HoodieActiveTimeline active = metaClient.getActiveTimeline();
      long activeCount = countIngestCommits(active);
      if (activeCount >= cfg.getMinTableCommits()) {
        return activeCount;
      }
      HoodieArchivedTimeline archived = metaClient.getArchivedTimeline();
      long archivedCount = countIngestCommits(archived);
      return activeCount + archivedCount;
    } catch (Exception e) {
      LOG.warn("Failed to count ingest commits: " + e.getMessage());
      return 0;
    }
  }

  /**
   * Counts completed ingest commits on a single timeline. Commit and deltacommit are counted
   * unconditionally; each replacecommit is deserialized to distinguish real ingests
   * ({@code INSERT_OVERWRITE} / {@code INSERT_OVERWRITE_TABLE}) from clustering, which is skipped.
   * An unreadable replacecommit body is skipped — we'd rather undercount than misclassify.
   */
  public static long countIngestCommits(HoodieTimeline timeline) {
    return timeline.filterCompletedInstants().getInstantsAsStream()
        .filter(i -> {
          String a = i.getAction();
          if (a.equals(HoodieTimeline.COMMIT_ACTION) || a.equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
            return true;
          }
          if (!a.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
            return false;
          }
          try {
            HoodieCommitMetadata cm = timeline.readInstantContent(i, HoodieCommitMetadata.class);
            return cm.getOperationType() != WriteOperationType.CLUSTER;
          } catch (Exception e) {
            LOG.warn("Skipping replacecommit {} during ingest-commit count: {}", i.requestedTime(), e.toString());
            return false;
          }
        }).count();
  }

  // ---- one-shot ----

  /**
   * One-shot convenience: aggregates per-partition qualifying/flagged counts, counts ingest
   * commits, and returns the classified verdict. Equivalent to invoking the per-piece methods
   * above in sequence — offered for callers that don't need the individual steps.
   */
  public static Result run(HoodieTableMetaClient metaClient,
                           Collection<PartitionStats> partitions,
                           Config cfg) {
    long qualifying = 0;
    long flagged = 0;
    for (PartitionStats p : partitions) {
      if (!isQualifying(p, cfg)) {
        continue;
      }
      qualifying++;
      // isFlagged repeats the qualifying check, but we've already confirmed it — inline the
      // avg-size comparison so we don't re-check the file-count gate on the hot path. Kept in
      // sync with isFlagged's threshold semantics (strict '<' against thresholdBytes).
      if (p.avgFileSize() < cfg.getThresholdBytes()) {
        flagged++;
      }
    }
    long commits = countTotalIngestCommits(metaClient, cfg);
    Verdict verdict = classifyVerdict(commits, qualifying, flagged, cfg);
    double flaggedPct = qualifying == 0 ? 0.0 : (double) flagged / qualifying;
    return new Result(verdict, qualifying, flagged, flaggedPct, commits);
  }
}
