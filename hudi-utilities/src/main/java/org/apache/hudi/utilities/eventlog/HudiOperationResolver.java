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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Derives the {@link HudiOperation} a job belongs to, from two signals in priority order.
 *
 * <ol>
 *   <li><b>The module</b> half of the job description, which frequently names the operation outright
 *       ({@code HoodieCompactor}, {@code SparkInsertOverwriteTableCommitActionExecutor},
 *       {@code SavepointActionExecutor}). This is the stronger signal and is tried first.</li>
 *   <li><b>The phase sequence</b> within a job, used when the module is absent or too generic to
 *       decide. Table services have a planning-then-execution shape that write operations do not,
 *       which is what makes the sequence informative.</li>
 * </ol>
 *
 * <p>When neither resolves, the operation is {@link HudiOperation#UNKNOWN} with source
 * {@link HudiOperation.Source#NONE}. Nothing is guessed, and the source is reported alongside the
 * operation so a reader can judge how much to trust it.
 *
 * <h3>Restore versus rollback</h3>
 * {@code BaseRestoreActionExecutor} sets no job status of its own: it loops over the instants to
 * roll back and calls the ordinary rollback path for each. A restore therefore looks exactly like N
 * repetitions of the rollback phases with no other marker, and the only way to tell the two apart
 * from a log is to count the repetitions. That promotion happens in
 * {@link #promoteRepeatedRollbacksToRestore}, over the whole application rather than per job.
 */
public final class HudiOperationResolver {

  /**
   * Number of distinct rollback job groups in one application above which the run is read as a
   * restore rather than a sequence of independent rollbacks. Two is deliberately conservative: a
   * single failed write rolls back once, and an application that rolls back repeatedly is either a
   * restore or something a reader wants flagged either way.
   */
  private static final int ROLLBACK_REPETITIONS_FOR_RESTORE = 3;

  /**
   * Module-name substrings mapped to the operation they name, longest substring first so that
   * {@code ScheduleCompactionActionExecutor} is not swallowed by a bare {@code Compaction} rule.
   * Matching is case-insensitive and on substring, because module names vary by engine and version
   * ({@code SparkHoodieBackedTableMetadataWriterTableVersionSix} really occurs).
   */
  private static final Map<String, HudiOperation> MODULE_RULES = buildModuleRules();

  private HudiOperationResolver() {
  }

  private static Map<String, HudiOperation> buildModuleRules() {
    Map<String, HudiOperation> rules = new LinkedHashMap<>();

    // Longest / most specific first. LinkedHashMap preserves this order for iteration.
    rules.put("insertoverwritetablecommitactionexecutor", HudiOperation.WRITE_OPERATION);
    rules.put("insertoverwritecommitactionexecutor", HudiOperation.WRITE_OPERATION);
    rules.put("deletepartitioncommitactionexecutor", HudiOperation.WRITE_OPERATION);
    rules.put("schedulecompactionactionexecutor", HudiOperation.COMPACTION);
    rules.put("compactionplangenerator", HudiOperation.COMPACTION);
    rules.put("runcompactionactionexecutor", HudiOperation.COMPACTION);
    rules.put("logcompaction", HudiOperation.LOG_COMPACTION);
    rules.put("compactionadminclient", HudiOperation.COMPACTION);
    rules.put("hoodiecompactor", HudiOperation.COMPACTION);
    rules.put("clusteringcommitactionexecutor", HudiOperation.CLUSTERING);
    rules.put("clusteringexecutionstrategy", HudiOperation.CLUSTERING);
    rules.put("executionstrategy", HudiOperation.CLUSTERING);
    rules.put("cleanplanactionexecutor", HudiOperation.CLEANING);
    rules.put("cleanactionexecutor", HudiOperation.CLEANING);
    rules.put("restoreactionexecutor", HudiOperation.RESTORE);
    rules.put("listingbasedrollbackstrategy", HudiOperation.ROLLBACK);
    rules.put("rollbackhelper", HudiOperation.ROLLBACK);
    rules.put("savepointactionexecutor", HudiOperation.SAVEPOINT);
    rules.put("bootstrapcommitactionexecutor", HudiOperation.BOOTSTRAP);
    rules.put("timelinearchiver", HudiOperation.ARCHIVAL);
    rules.put("streamsync", HudiOperation.STREAMER_SYNC);
    rules.put("recordindexer", HudiOperation.INDEXING);
    rules.put("secondaryindex", HudiOperation.INDEXING);
    rules.put("filesindexer", HudiOperation.INDEXING);
    rules.put("metadatawriteclient", HudiOperation.INDEXING);
    rules.put("backedtablemetadatawriter", HudiOperation.INDEXING);
    // Generic write executors. These are last so a more specific rule above always wins.
    rules.put("upsertpartitioner", HudiOperation.WRITE_OPERATION);
    rules.put("commitactionexecutor", HudiOperation.WRITE_OPERATION);
    rules.put("writehelper", HudiOperation.WRITE_OPERATION);
    rules.put("writeclient", HudiOperation.WRITE_OPERATION);

    return Collections.unmodifiableMap(rules);
  }

  /**
   * Resolves the operation a job belongs to from its module and the phases its stages fell into.
   *
   * @param module    the module half of the job description, or null when neither encoding gave one.
   * @param phases    the phases of the stages belonging to this job.
   * @param subPhases the sub-phases of those stages; these disambiguate the shared PLANNING and
   *                  EXECUTION buckets.
   * @return the operation and the signal that produced it.
   */
  public static Resolution resolve(String module, List<HudiPhase> phases, List<String> subPhases) {
    HudiOperation fromModule = matchModule(module);
    if (fromModule != HudiOperation.UNKNOWN) {
      return new Resolution(fromModule, HudiOperation.Source.MODULE);
    }

    HudiOperation fromSequence = matchSequence(phases, subPhases == null ? Collections.emptyList() : subPhases);
    if (fromSequence != HudiOperation.UNKNOWN) {
      return new Resolution(fromSequence, HudiOperation.Source.SEQUENCE);
    }
    return new Resolution(HudiOperation.UNKNOWN, HudiOperation.Source.NONE);
  }

  /** Convenience overload for callers with no sub-phase information. */
  public static Resolution resolve(String module, List<HudiPhase> phases) {
    return resolve(module, phases, Collections.emptyList());
  }

  private static HudiOperation matchModule(String module) {
    if (module == null || module.isEmpty()) {
      return HudiOperation.UNKNOWN;
    }
    String lowerCase = module.toLowerCase(Locale.ROOT);
    for (Map.Entry<String, HudiOperation> rule : MODULE_RULES.entrySet()) {
      if (lowerCase.contains(rule.getKey())) {
        return rule.getValue();
      }
    }
    return HudiOperation.UNKNOWN;
  }

  /**
   * Sub-phase prefixes that name the table service a {@link HudiPhase#PLANNING} or
   * {@link HudiPhase#EXECUTION} stage belongs to. Those two buckets recur across every table
   * service, so the bucket alone cannot say which; the sub-phase can.
   */
  private static final Map<String, HudiOperation> SUB_PHASE_RULES = buildSubPhaseRules();

  private static Map<String, HudiOperation> buildSubPhaseRules() {
    Map<String, HudiOperation> rules = new LinkedHashMap<>();
    // Record-index lookups tag records for a write; the listing is metadata-table work. Both come
    // from stage names rather than job descriptions, so there is no module to fall back on.
    rules.put("recordindexlookup", HudiOperation.WRITE_OPERATION);
    rules.put("metadatalisting", HudiOperation.INDEXING);
    rules.put("logcompaction", HudiOperation.LOG_COMPACTION);
    rules.put("compaction", HudiOperation.COMPACTION);
    rules.put("clustering", HudiOperation.CLUSTERING);
    rules.put("clean", HudiOperation.CLEANING);
    rules.put("rollback", HudiOperation.ROLLBACK);
    rules.put("savepoint", HudiOperation.SAVEPOINT);
    rules.put("bootstrap", HudiOperation.BOOTSTRAP);
    return Collections.unmodifiableMap(rules);
  }

  /**
   * Infers the operation from the phases and sub-phases a job's stages fell into.
   *
   * <p>The coarse buckets are deliberately shared across operations, so {@code PLANNING} alone says
   * nothing about which table service is running. The sub-phase does, which is why it is consulted
   * first here. Falling back to the bucket alone can still identify a write operation, because the
   * write buckets are not shared with the table services -- but it can never say <em>which</em>
   * write operation, since upsert, insert, bulk_insert and delete all produce the same shape.
   */
  private static HudiOperation matchSequence(List<HudiPhase> phases, List<String> subPhases) {
    if (phases == null || phases.isEmpty()) {
      return HudiOperation.UNKNOWN;
    }

    for (String subPhase : subPhases) {
      if (subPhase == null) {
        continue;
      }
      String lowerCase = subPhase.toLowerCase(Locale.ROOT);
      for (Map.Entry<String, HudiOperation> rule : SUB_PHASE_RULES.entrySet()) {
        if (lowerCase.contains(rule.getKey())) {
          return rule.getValue();
        }
      }
    }

    for (HudiPhase phase : phases) {
      switch (phase) {
        case ARCHIVAL:
          return HudiOperation.ARCHIVAL;
        case METADATA_TABLE_WRITE:
          return HudiOperation.INDEXING;
        case SOURCE_READ_AND_TRANSFORM:
          return HudiOperation.STREAMER_SYNC;
        case DEDUP_AND_INDEX_TAGGING:
        case DATA_TABLE_WRITE:
        case MARKER_RECONCILIATION:
          return HudiOperation.WRITE_OPERATION;
        default:
          break;
      }
    }
    return HudiOperation.UNKNOWN;
  }

  /**
   * Promotes repeated rollbacks within one application to a single {@link HudiOperation#RESTORE}.
   *
   * <p>A restore is N rollbacks driven by one restore instant, and the rollback path emits no marker
   * saying so: {@code BaseRestoreActionExecutor} sets no job status and simply calls the ordinary
   * rollback path for each instant. The child jobs therefore carry ordinary rollback modules such as
   * {@code ListingBasedRollbackStrategy}, and counting them is the only signal a log offers. This
   * runs once over the whole application rather than per job, and the resulting operation is always
   * attributed to {@link HudiOperation.Source#SEQUENCE}, because the count is what decided it.
   *
   * <p>A job that already resolved to {@link HudiOperation#RESTORE} from its module is left alone;
   * an explicit restore module is a stronger signal than the count and needs no promotion.
   *
   * @param resolutionsByJob operation resolutions keyed by job id; mutated in place.
   * @param rollbackJobIds   the ids of jobs whose operation resolved to a rollback.
   */
  public static void promoteRepeatedRollbacksToRestore(Map<Integer, Resolution> resolutionsByJob,
                                                       List<Integer> rollbackJobIds) {
    if (rollbackJobIds.size() < ROLLBACK_REPETITIONS_FOR_RESTORE) {
      return;
    }
    for (Integer jobId : rollbackJobIds) {
      Resolution existing = resolutionsByJob.get(jobId);
      if (existing != null && existing.getOperation() == HudiOperation.ROLLBACK) {
        resolutionsByJob.put(jobId, new Resolution(HudiOperation.RESTORE, HudiOperation.Source.SEQUENCE));
      }
    }
  }

  /** The module-name substrings this resolver knows, for documentation and tests. */
  public static List<String> getKnownModulePatterns() {
    return Collections.unmodifiableList(new ArrayList<>(MODULE_RULES.keySet()));
  }

  /** The five buckets a write operation's time falls into, rather than a table service's two. */
  public static List<HudiPhase> getWritePipelinePhases() {
    return Collections.unmodifiableList(Arrays.asList(
        HudiPhase.SOURCE_READ_AND_TRANSFORM, HudiPhase.DEDUP_AND_INDEX_TAGGING,
        HudiPhase.DATA_TABLE_WRITE, HudiPhase.MARKER_RECONCILIATION, HudiPhase.METADATA_TABLE_WRITE));
  }

  /** An operation together with the signal that produced it. */
  public static final class Resolution {
    private final HudiOperation operation;
    private final HudiOperation.Source source;

    Resolution(HudiOperation operation, HudiOperation.Source source) {
      this.operation = operation;
      this.source = source;
    }

    public HudiOperation getOperation() {
      return operation;
    }

    public HudiOperation.Source getSource() {
      return source;
    }
  }
}
