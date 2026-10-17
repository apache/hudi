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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Derives a {@link HudiPhase}, and where available a sub-phase, from the Spark job description that
 * Hudi publishes via {@code HoodieEngineContext#setJobStatus(activeModule, activityDescription)}.
 *
 * <h3>Two wire encodings</h3>
 * Hudi has used two different encodings, and logs of both are still in circulation, so both are
 * supported:
 * <ul>
 *   <li><b>Current</b> (HUDI-8596 onwards): {@code setJobDescription(module + ":" + activity)}, so
 *       {@code spark.job.description} reads {@code HoodieWriteHelper:Tagging: my_table}.</li>
 *   <li><b>Legacy</b> (before HUDI-8596): {@code setJobGroup(module, activity)}, so the module lands
 *       in {@code spark.jobGroup.id} and {@code spark.job.description} holds the bare activity,
 *       {@code Tagging: my_table}.</li>
 * </ul>
 * Matching therefore runs against the raw description and, if that fails, against the description
 * with a leading {@code module:} stripped. The module itself is not used to decide the phase; it is
 * used separately, by {@link HudiOperationResolver}, to decide the operation.
 *
 * <h3>Prefix matching</h3>
 * Most activity strings end with a separator because Hudi appends a value (usually the table name),
 * and a few embed a count mid-string ({@code Listing 12 partitions from filesystem}). Each rule is
 * therefore a prefix, matched case-insensitively against the activity. Rules are evaluated longest
 * prefix first so that more specific strings win over shorter ones that share a prefix. A
 * description that matches nothing yields {@link HudiPhase#UNKNOWN}; nothing is guessed.
 */
public final class HudiPhaseResolver {

  /** One activity-prefix rule, with an optional sub-phase label. */
  private static final class Rule {
    private final String prefixLowerCase;
    private final HudiPhase phase;
    private final String subPhase;

    Rule(String prefix, HudiPhase phase) {
      this(prefix, phase, null);
    }

    Rule(String prefix, HudiPhase phase, String subPhase) {
      this.prefixLowerCase = prefix.toLowerCase(Locale.ROOT);
      this.phase = phase;
      this.subPhase = subPhase;
    }
  }

  /** One activity rule for a description that embeds a count, so a prefix cannot express it. */
  private static final class CountedRule {
    private final Pattern pattern;
    private final HudiPhase phase;

    CountedRule(String regex, HudiPhase phase) {
      this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      this.phase = phase;
    }
  }

  /** The phase and sub-phase a description resolved to. */
  public static final class Match {
    private final HudiPhase phase;
    private final String subPhase;

    private Match(HudiPhase phase, String subPhase) {
      this.phase = phase;
      this.subPhase = subPhase;
    }

    public HudiPhase getPhase() {
      return phase;
    }

    /** A finer label within the phase, or null when the phase needs no subdivision. */
    public String getSubPhase() {
      return subPhase;
    }
  }

  private static final Match NO_MATCH = new Match(HudiPhase.UNKNOWN, null);

  /** The activity {@code FSUtils} uses for its parallel listing, which carries the paths listed. */
  private static final String PARALLEL_LISTING_ACTIVITY = "Parallel listing paths ";

  /** Path segment identifying the metadata table, used to tell its listings from data-table ones. */
  private static final String METADATA_PATH_SEGMENT = "/metadata/";

  /** Sub-phase marking stages on the untagged streaming metadata-write path. */
  public static final String STREAMING_METADATA_WRITE_SUB_PHASE = "streamingMetadataWrite";

  /**
   * Stage-name markers that <b>refine the sub-phase</b> within an already-decided phase.
   *
   * <p>The job description decides the phase and is authoritative; these never change it. They exist
   * because one Hudi job description can cover several components: {@code Building workload profile:}
   * is the job under which deduplication, index tagging and profiling all run, so the description is
   * correct but coarser than the component boundary a reader cares about. Spark names each stage
   * after the class that created the RDD, which says which component actually ran.
   *
   * <p>This adds detail; it never contradicts the description, so no caveat is raised for it.
   */
  private static final Map<String, String> SUB_PHASE_REFINEMENTS = buildSubPhaseRefinements();

  private static Map<String, String> buildSubPhaseRefinements() {
    Map<String, String> rules = new LinkedHashMap<>();
    rules.put("SparkMetadataTableGlobalRecordLevelIndex", "recordIndexLookup");
    rules.put("SparkMetadataTableRecordLevelIndex", "recordIndexLookup");
    rules.put("SparkHoodieBloomIndexHelper", "bloomIndexLookup");
    rules.put("HoodieBloomIndexCheckFunction", "bloomIndexLookup");
    rules.put("HoodieGlobalBloomIndex", "bloomIndexLookup");
    rules.put("HoodieBloomIndex", "bloomIndexLookup");
    return Collections.unmodifiableMap(rules);
  }

  /** Phases whose sub-phase a stage name is allowed to refine. */
  private static final Set<HudiPhase> REFINABLE_PHASES =
      Collections.unmodifiableSet(EnumSet.of(HudiPhase.DEDUP_AND_INDEX_TAGGING));

  /**
   * Stage-name markers consulted only as a <b>fallback</b>, after the job description has failed to
   * resolve. Weaker than {@link #STAGE_NAME_OVERRIDES}: these never contradict a description.
   */
  private static final Map<String, Match> STAGE_NAME_FALLBACKS = buildStageNameFallbacks();

  private static Map<String, Match> buildStageNameFallbacks() {
    Map<String, Match> rules = new LinkedHashMap<>();
    // Streaming writes to the metadata table. The phase stays UNKNOWN on purpose: this path is
    // genuinely unattributable, and labelling it would be a false claim. The sub-phase lets a reader
    // see what the unattributed work is without the tool pretending to know where it belongs.
    rules.put("SparkStreamingMetadataWriteHandler",
        new Match(HudiPhase.UNKNOWN, STREAMING_METADATA_WRITE_SUB_PHASE));
    return Collections.unmodifiableMap(rules);
  }

  /**
   * Activity prefixes, with the phase each maps to. Sorted by descending prefix length at class-init
   * so that longest-prefix wins.
   */
  private static final List<Rule> RULES = buildRules();

  /**
   * Activities whose text embeds a count. These are anchored at the start and deliberately narrow,
   * so that Spark's own {@code Listing leaf files and directories ...} does not match.
   */
  private static final List<CountedRule> COUNTED_RULES = Collections.unmodifiableList(Arrays.asList(
      new CountedRule("^Listing \\d+ partitions from filesystem", HudiPhase.METADATA_TABLE_WRITE),
      new CountedRule("^Creating \\d+ file groups for partition ", HudiPhase.METADATA_TABLE_WRITE)));

  private HudiPhaseResolver() {
  }

  private static List<Rule> buildRules() {
    List<Rule> rules = new ArrayList<>();

    // -- Source read, transformation and record creation (inseparable; see HudiPhase) -----------
    rules.add(new Rule("Fetching next batch: ", HudiPhase.SOURCE_READ_AND_TRANSFORM, "sourceFetch"));
    rules.add(new Rule("Checking if input is empty: ", HudiPhase.SOURCE_READ_AND_TRANSFORM, "emptyInputCheck"));

    // -- Deduplication and index tagging (inseparable; see HudiPhase) ---------------------------
    rules.add(new Rule("Tagging: ", HudiPhase.DEDUP_AND_INDEX_TAGGING, "tagging"));
    rules.add(new Rule("Obtain key ranges for file slices",
        HudiPhase.DEDUP_AND_INDEX_TAGGING, "bloomRangePruning"));
    rules.add(new Rule("Load meta index key ranges for file slices: ",
        HudiPhase.DEDUP_AND_INDEX_TAGGING, "bloomMetaIndexRanges"));
    rules.add(new Rule("Compute all comparisons needed between records and files: ",
        HudiPhase.DEDUP_AND_INDEX_TAGGING, "bloomComparisonFanout"));
    rules.add(new Rule("Load latest base files from all partitions: ",
        HudiPhase.DEDUP_AND_INDEX_TAGGING, "baseFileListing"));
    // "Building workload profile:" is the job under which deduplication, index tagging AND profiling
    // all run -- Hudi sets the description once and the whole lazy chain executes beneath it. It
    // therefore belongs with the tagging work rather than with the write. Measured: with
    // hoodie.index.type=RECORD_INDEX the record-index lookups execute inside this job, so bucketing
    // it as a write made DEDUP_AND_INDEX_TAGGING read 0% on tables where tagging dominated.
    rules.add(new Rule("Building workload profile:", HudiPhase.DEDUP_AND_INDEX_TAGGING, "workloadProfile"));

    // -- The data-table write, with profiling and small-file probing folded in ------------------
    rules.add(new Rule("Doing partition and writing data: ", HudiPhase.DATA_TABLE_WRITE, "write"));
    rules.add(new Rule("Commit write status collect: ", HudiPhase.DATA_TABLE_WRITE, "writeStatusCollect"));
    rules.add(new Rule("Committing stats: ", HudiPhase.DATA_TABLE_WRITE, "commitStats"));
    // Mostly driver and listing work, so folded into the write bucket; the sub-phase keeps it
    // visible when it turns out to be the expensive part.
    rules.add(new Rule("Getting small files from partitions: ", HudiPhase.DATA_TABLE_WRITE, "smallFileProbe"));
    // Insert-overwrite and delete-partition resolve which file groups are being replaced. These are
    // sub-steps of the write: the strings only occur in the insert-overwrite-table executor and the
    // Flink write client, so they are specific to those operations rather than a peer bucket.
    rules.add(new Rule("Getting ExistingFileIds of all partitions", HudiPhase.DATA_TABLE_WRITE, "partitionReplace"));
    rules.add(new Rule("Getting ExistingFileIds of matching static partitions",
        HudiPhase.DATA_TABLE_WRITE, "partitionReplace"));
    rules.add(new Rule("Resolving file groups being replaced across all partitions",
        HudiPhase.DATA_TABLE_WRITE, "partitionReplace"));
    rules.add(new Rule("Gather all file ids from all deleting partitions.",
        HudiPhase.DATA_TABLE_WRITE, "partitionReplace"));

    // -- Metadata table -----------------------------------------------------------------------
    rules.add(new Rule("Committing to metadata table: ", HudiPhase.METADATA_TABLE_WRITE));
    rules.add(new Rule("Dropping partitions from metadata table: ", HudiPhase.METADATA_TABLE_WRITE));
    rules.add(new Rule("Creating records for metadata FILES partition", HudiPhase.METADATA_TABLE_WRITE, "filesPartition"));
    rules.add(new Rule("Record Index: reading record keys from ", HudiPhase.METADATA_TABLE_WRITE, "recordIndex"));
    rules.add(new Rule("Secondary Index: reading secondary keys from ", HudiPhase.METADATA_TABLE_WRITE, "secondaryIndex"));
    // "Listing <n> partitions from filesystem" and "Creating <n> file groups for partition ..." embed
    // a count, so they are matched by a regex rather than a prefix (see COUNTED_RULES).
    // Format strings from HoodieBackedTableMetadataWriter / SparkHoodieBackedTableMetadataWriter.
    rules.add(new Rule("Bulk inserting at ", HudiPhase.METADATA_TABLE_WRITE));
    rules.add(new Rule("Upserting at ", HudiPhase.METADATA_TABLE_WRITE));
    rules.add(new Rule("Upserting with instant ", HudiPhase.METADATA_TABLE_WRITE));

    // -- Markers and write finalization -------------------------------------------------------
    rules.add(new Rule("Obtaining marker files for all created, merged paths", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Deleting marker directory: ", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Cleaning up marker directories for commit ", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Delete all partially written files: ", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Delete invalid files generated during the write operation: ", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Wait for all files to appear/disappear:", HudiPhase.MARKER_RECONCILIATION));
    rules.add(new Rule("Preparing data for missing files to assist with generating write stats",
        HudiPhase.MARKER_RECONCILIATION, "missingFileStats"));
    rules.add(new Rule("Generating writeStat for missing log files", HudiPhase.MARKER_RECONCILIATION, "missingFileStats"));

    // -- Table services. PLANNING and EXECUTION recur across all of them; which service a stage
    // -- belongs to comes from HudiOperation, not from the phase.
    rules.add(new Rule("Compaction: generating compaction plan", HudiPhase.PLANNING, "compactionPlan"));
    rules.add(new Rule("Looking for files to compact: ", HudiPhase.PLANNING, "compactionCandidateScan"));
    rules.add(new Rule("Validate compaction operations: ", HudiPhase.PLANNING, "compactionValidation"));
    rules.add(new Rule("Execute unschedule operations: ", HudiPhase.PLANNING, "compactionUnschedule"));
    rules.add(new Rule("Compacting file slices: ", HudiPhase.EXECUTION, "compaction"));
    rules.add(new Rule("Preparing compaction metadata: ", HudiPhase.EXECUTION, "compactionMetadata"));
    rules.add(new Rule("Collect compaction write status and commit compaction: ",
        HudiPhase.EXECUTION, "compactionCommit"));
    rules.add(new Rule("Collect log compaction write status and commit compaction",
        HudiPhase.EXECUTION, "logCompactionCommit"));

    rules.add(new Rule("Clustering records for ", HudiPhase.EXECUTION, "clustering"));
    rules.add(new Rule("Handling updates which are under clustering: ",
        HudiPhase.EXECUTION, "clusteringUpdateHandling"));
    rules.add(new Rule("Collect clustering write status and commit clustering",
        HudiPhase.EXECUTION, "clusteringCommit"));

    rules.add(new Rule("Obtaining list of partitions to be cleaned: ", HudiPhase.PLANNING, "cleanPartitionScan"));
    rules.add(new Rule("Generating list of file slices to be cleaned: ", HudiPhase.PLANNING, "cleanFileSliceScan"));
    rules.add(new Rule("Perform cleaning of table: ", HudiPhase.EXECUTION, "cleaning"));

    // Rollback. Identifying which instants to roll back is itself often the expensive part, and the
    // listing-based strategy is where that cost shows up; it is folded into PLANNING, with the
    // sub-phase distinguishing it.
    rules.add(new Rule("Creating Listing Rollback Plan: ", HudiPhase.PLANNING, "rollbackInstantListing"));
    rules.add(new Rule("Perform rollback actions: ", HudiPhase.EXECUTION, "rollback"));
    rules.add(new Rule("Collect rollback stats: ", HudiPhase.EXECUTION, "rollbackStats"));
    rules.add(new Rule("Collect rollback stats for upgrade/downgrade: ",
        HudiPhase.EXECUTION, "rollbackStatsUpgradeDowngrade"));

    // Savepoint and bootstrap: coarse on purpose, the operation carries the meaning.
    rules.add(new Rule("Collecting latest files for savepoint ", HudiPhase.PLANNING, "savepointFileListing"));
    rules.add(new Rule("Run metadata-only bootstrap operation: ", HudiPhase.EXECUTION, "bootstrap"));

    // -- Archival ------------------------------------------------------------------------------
    rules.add(new Rule("Delete archived instants: ", HudiPhase.ARCHIVAL, "archivedInstantDelete"));


    // Longest prefix first, with the prefix itself as a tie-break so ordering is deterministic.
    rules.sort((left, right) -> {
      int byLength = Integer.compare(right.prefixLowerCase.length(), left.prefixLowerCase.length());
      return byLength != 0 ? byLength : left.prefixLowerCase.compareTo(right.prefixLowerCase);
    });
    return Collections.unmodifiableList(rules);
  }

  /**
   * Resolves the phase for a Spark job description.
   *
   * @param jobDescription the verbatim {@code spark.job.description}, possibly null or empty.
   * @return the derived phase, {@link HudiPhase#UNKNOWN} when nothing matched.
   */
  public static HudiPhase resolve(String jobDescription) {
    return resolveMatch(jobDescription).getPhase();
  }

  /**
   * Resolves the phase and sub-phase for a Spark job description.
   *
   * @param jobDescription the verbatim {@code spark.job.description}, possibly null or empty.
   * @return the match, never null; {@link HudiPhase#UNKNOWN} with a null sub-phase when nothing matched.
   */
  public static Match resolveMatch(String jobDescription) {
    if (jobDescription == null || jobDescription.isEmpty()) {
      return NO_MATCH;
    }

    Match direct = matchActivity(jobDescription);
    if (direct.getPhase() != HudiPhase.UNKNOWN) {
      return direct;
    }

    // Current encoding: "<module>:<activity>". Strip the first segment and retry. Only the first
    // colon is stripped, because several activities contain colons of their own.
    int colon = jobDescription.indexOf(':');
    if (colon >= 0 && colon + 1 < jobDescription.length()) {
      Match stripped = matchActivity(jobDescription.substring(colon + 1));
      if (stripped.getPhase() != HudiPhase.UNKNOWN) {
        return stripped;
      }
    }

    return matchParallelListing(jobDescription);
  }

  /**
   * {@code FSUtils} tags its parallel listing with the paths being listed, which is the only thing
   * saying what the listing is for. Paths under the metadata table are metadata-table work; a
   * listing of the data table is left {@link HudiPhase#UNKNOWN} rather than guessed at.
   */
  private static Match matchParallelListing(String jobDescription) {
    int pathsAt = jobDescription.indexOf(PARALLEL_LISTING_ACTIVITY);
    if (pathsAt < 0) {
      return NO_MATCH;
    }
    String paths = jobDescription.substring(pathsAt + PARALLEL_LISTING_ACTIVITY.length());
    return paths.contains(METADATA_PATH_SEGMENT)
        ? new Match(HudiPhase.METADATA_TABLE_WRITE, "metadataListing")
        : NO_MATCH;
  }

  /**
   * Refines the sub-phase of an already-decided phase using the Spark stage name.
   *
   * <p>The phase comes from the job description and is not changed here. This only names which
   * component ran, inside a job whose description covers several -- see
   * {@link #SUB_PHASE_REFINEMENTS}.
   *
   * @param phase     the phase the job description resolved to.
   * @param stageName the Spark stage name, possibly null or empty.
   * @return the refined sub-phase, or null to keep whatever the description implied.
   */
  public static String refineSubPhase(HudiPhase phase, String stageName) {
    if (!REFINABLE_PHASES.contains(phase) || stageName == null || stageName.isEmpty()) {
      return null;
    }
    for (Map.Entry<String, String> rule : SUB_PHASE_REFINEMENTS.entrySet()) {
      if (stageName.contains(rule.getKey())) {
        return rule.getValue();
      }
    }
    return null;
  }

  /**
   * Resolves a phase from the Spark stage name, for Hudi code paths that set no job description at
   * all.
   *
   * <p>Checked only <em>after</em> the description has failed. Spark names a stage after the class
   * and line that created the RDD, so the name is the only signal left for an uninstrumented path.
   *
   * @param stageName the Spark stage name, possibly null or empty.
   * @return the match, never null; {@link HudiPhase#UNKNOWN} when the stage name says nothing.
   */
  public static Match resolveFromStageName(String stageName) {
    return matchStageName(stageName, STAGE_NAME_FALLBACKS);
  }

  private static Match matchStageName(String stageName, Map<String, Match> rules) {
    if (stageName == null || stageName.isEmpty()) {
      return NO_MATCH;
    }
    for (Map.Entry<String, Match> rule : rules.entrySet()) {
      if (stageName.contains(rule.getKey())) {
        return rule.getValue();
      }
    }
    return NO_MATCH;
  }

  /**
   * Extracts the Hudi module (the class that set the job status) when the description carries one.
   *
   * @param jobDescription the verbatim {@code spark.job.description}, possibly null.
   * @param jobGroupId     the {@code spark.jobGroup.id}, which carries the module in logs written
   *                       before HUDI-8596; may be null.
   * @return the module name, or null when neither encoding supplied one.
   */
  public static String resolveModule(String jobDescription, String jobGroupId) {
    if (jobDescription != null && matchActivity(jobDescription).getPhase() == HudiPhase.UNKNOWN) {
      int colon = jobDescription.indexOf(':');
      if (colon > 0 && colon + 1 < jobDescription.length()
          && matchActivity(jobDescription.substring(colon + 1)).getPhase() != HudiPhase.UNKNOWN) {
        return jobDescription.substring(0, colon);
      }
    }
    return jobGroupId == null || jobGroupId.isEmpty() ? null : jobGroupId;
  }

  private static Match matchActivity(String activity) {
    String lowerCase = activity.toLowerCase(Locale.ROOT);
    for (Rule rule : RULES) {
      if (lowerCase.startsWith(rule.prefixLowerCase)) {
        return new Match(rule.phase, rule.subPhase);
      }
    }

    for (CountedRule rule : COUNTED_RULES) {
      if (rule.pattern.matcher(activity).find()) {
        return new Match(rule.phase, null);
      }
    }
    return NO_MATCH;
  }
}
