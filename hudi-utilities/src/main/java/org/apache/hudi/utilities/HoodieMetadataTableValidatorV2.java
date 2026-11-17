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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.util.ArchivalProbe;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.BranchBeacon;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.DuplicateBaseFileDimension;
import org.apache.hudi.utilities.util.FileDiffReportWriter;
import org.apache.hudi.utilities.util.FsMdtDiffDimension;
import org.apache.hudi.utilities.util.HoodieMetadataValidationUtils;
import org.apache.hudi.utilities.util.Inconclusive;
import org.apache.hudi.utilities.util.InconclusiveCause;
import org.apache.hudi.utilities.util.InstantBuckets;
import org.apache.hudi.utilities.util.InstantStateIndex;
import org.apache.hudi.utilities.util.MismatchRecord;
import org.apache.hudi.utilities.util.MismatchReportWriter;
import org.apache.hudi.utilities.util.ServiceActivityDelta;
import org.apache.hudi.utilities.util.ValidationFailures;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.ValidationRunReports;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * V2 of the metadata table validator, over COW and MOR tables (log files included). Enabled from
 * {@link HoodieMetadataTableValidator} via {@code --use-validator-v2} (default off).
 *
 * <p>Runs two independent dimensions, each on by default: the FS-MDT file slice comparison
 * ({@code --validate-fs-mdt-diff}), which compares latest slices and reports every slice of a file
 * group the metadata table does not know, and duplicate base file detection
 * ({@code --validate-duplicate-base-files}). For the former the driver builds a bounded
 * metadata-table-backed view once and serves it through the embedded timeline server; executors
 * never open the metadata table, fetch expected slices through a
 * {@link RemoteHoodieTableFileSystemView}, and compare with the v1 logic
 * ({@link HoodieMetadataTableValidator#validateFileSlices}). Other v1 validations are out of scope.
 */
public class HoodieMetadataTableValidatorV2 implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataTableValidatorV2.class);

  /** Names the directory one exhaustive run's findings are written to. */
  private static final DateTimeFormatter RUN_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

  /** @see ValidationFailures#INFRA_FAILURE_MARKER */
  private static final String INFRA_FAILURE_MARKER = ValidationFailures.INFRA_FAILURE_MARKER;

  /**
   * Total attempts (not retries) at pinning the metadata snapshot. Each attempt rebuilds the
   * metadata-table view, the slowest phase of a run.
   */
  private static final int MAX_PIN_ATTEMPTS = 3;

  /**
   * Log token for a fail-fast run that validated nothing because the timeline moved under every
   * pinning attempt. Alerting keys off it, so it must not be reworded: fail-fast returns success
   * here, so counting consecutive occurrences is the only signal a table is never validated.
   */
  private static final String PINNING_SKIPPED_MARKER = "MDT_VALIDATOR_PINNING_SKIPPED";

  /** Storage types whose driver-hosted view spills instead of growing without bound. */
  private static final Set<FileSystemViewStorageType> SUPPORTED_SERVER_VIEW_STORAGE_TYPES =
      Collections.unmodifiableSet(EnumSet.of(
          FileSystemViewStorageType.SPILLABLE_DISK, FileSystemViewStorageType.EMBEDDED_KV_STORE));

  private final transient JavaSparkContext jsc;
  private final HoodieMetadataTableValidator.Config cfg;
  private final Properties props;
  private final List<Throwable> throwables = new ArrayList<>();

  /** What the last exhaustive run wrote; empty in fail-fast mode or if no writing phase ran. */
  private Option<ValidationRunReports> runReports = Option.empty();

  private transient HoodieTableMetaClient metaClient;

  public HoodieMetadataTableValidatorV2(JavaSparkContext jsc, HoodieMetadataTableValidator.Config cfg,
                                        Properties props) {
    this(jsc, cfg, props, Option.empty());
  }

  /**
   * @param props      resolved {@code --props} / {@code --hoodie-conf} overrides, seeding the
   *                   file-system-view and common configs as under the v1 validator.
   * @param metaClient meta client with the active timeline already loaded, if any; reused so the
   *                   timeline the executors carry is the one the caller saw.
   */
  public HoodieMetadataTableValidatorV2(JavaSparkContext jsc, HoodieMetadataTableValidator.Config cfg,
                                        Properties props, Option<HoodieTableMetaClient> metaClient) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = props;
    if (metaClient.isPresent()) {
      this.metaClient = metaClient.get();
      return;
    }
    try {
      this.metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).setBasePath(cfg.basePath)
          .setLoadActiveTimelineOnLoad(true)
          .build();
    } catch (TableNotFoundException tbe) {
      LOG.warn("Data table is not found. Skip current validation for: {}", cfg.basePath);
    }
  }

  /**
   * Hooks at the seams of a run, for tests that must act on the table between phases. The run keeps
   * ownership of ordering and cleanup; work done here is real table activity (archiving makes the
   * run inconclusive, committing moves {@code T_aft}). Every method is a no-op by default.
   */
  public interface PhaseObserver {

    /**
     * Called once {@code T_prev} is pinned, before phase 1 lists; a commit here reaches storage only.
     *
     * @param tPrev the pinned timeline the metadata snapshot was built against
     */
    default void afterSnapshotPinned(HoodieTimeline tPrev) {
    }

    /** Called after phase 1 completes, before {@code T_aft} is read; work here lands in the delta. */
    default void beforePostPhaseOneTimelineRead() {
    }

    /**
     * Called once {@code T_aft} is read, before phase 2; work here reaches only phase 2's own reads.
     *
     * @param tAft the timeline just read
     */
    default void afterPostPhaseOneTimelineRead(HoodieTimeline tAft) {
    }

    /**
     * Called once {@code T_prevD} is fixed, before the duplicate dimension's phase 1 lists.
     *
     * @param tPrevD the active timeline the duplicate-base-file dimension's window opens with
     */
    default void afterDuplicateSnapshotPinned(HoodieTimeline tPrevD) {
    }

    /** Called once duplicate phase 1 has completed, immediately before {@code T_aftD} is read. */
    default void beforeDuplicatePostPhaseOneTimelineRead() {
    }

    /**
     * Called immediately after {@code T_aftD} is read, before the archival check uses it.
     *
     * @param tAftD the timeline just read
     */
    default void afterDuplicatePostPhaseOneTimelineRead(HoodieTimeline tAftD) {
    }
  }

  /** Where the run stops to let a caller act on the table. Never null. */
  private transient PhaseObserver phaseObserver = new PhaseObserver() {
  };

  /**
   * Installs the observer the run calls at each seam; for tests only.
   *
   * @param observer never null
   */
  @VisibleForTesting
  public void setPhaseObserver(PhaseObserver observer) {
    ValidationUtils.checkArgument(observer != null, "the phase observer must not be null");
    this.phaseObserver = observer;
  }

  public List<Throwable> getThrowables() {
    return throwables;
  }

  public Option<ValidationRunReports> getRunReports() {
    return runReports;
  }

  public boolean hasValidationFailure() {
    for (Throwable throwable : throwables) {
      if (throwable instanceof HoodieValidationException) {
        return true;
      }
    }
    return false;
  }

  public boolean run() {
    // Config errors surface before anything is read and do not depend on the table existing.
    validateConfig();
    if (metaClient == null) {
      LOG.warn("Data table is not available to read for now, skip current validation for: {}", cfg.basePath);
      return true;
    }
    if (!HoodieMetadataValidationUtils.checkMetadataTableIsAvailable(jsc, cfg.basePath, metaClient, "v2")) {
      LOG.warn("Metadata table is not available to read for now, skip current validation for: {}", cfg.basePath);
      return true;
    }

    try {
      return validate();
    } catch (HoodieValidationException ve) {
      LOG.error("Metadata table validation (v2) failed for table: {}", cfg.basePath, ve);
      if (!cfg.ignoreFailed) {
        throw ve;
      }
      throwables.add(ve);
      return false;
    } catch (Exception e) {
      // Not collected into throwables: hasValidationFailure() must stay false so the job reports an
      // infrastructure error, not a correctness incident.
      Option<String> infraReason = matchInfraFailure(e);
      if (infraReason.isPresent()) {
        LOG.error("{} table={} reason={} - the cluster running this validation went away before it "
                + "finished. The metadata table was NOT found to be inconsistent; this is not a data "
                + "correctness issue and the next scheduled run will validate the table again.",
            INFRA_FAILURE_MARKER, cfg.basePath, infraReason.get(), e);
        throw classifyFailure(e);
      }
      throw new HoodieException("Unable to run metadata table validation (v2) in " + cfg.basePath, e);
    }
  }

  /**
   * Runs the two independent dimensions. The duplicate-base-file dimension runs first so fail-fast
   * can abort on it before the expensive FS-MDT DAG. Both switches off is rejected by
   * {@link #validateConfig}. The FS-MDT switch is v2's own {@code --validate-fs-mdt-diff} (default
   * on), not v1's {@code --validate-latest-file-slices}, which defaults to off.
   */
  private boolean validate() throws Exception {
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> allPartitions = enumeratePartitions(engineContext);
    if (allPartitions.isEmpty()) {
      LOG.warn("Neither the metadata table nor storage lists any partition, skip current "
          + "validation for: {}", cfg.basePath);
      return true;
    }

    DuplicateBaseFileDimension.Result duplicates =
        doDuplicateBaseFileDetection(engineContext, allPartitions);
    if (duplicates.isInconclusive()) {
      // Dimension A has no verdict: the archiver ran in its window, or phase 1 was recomputed.
      ValidationMode validationMode = parseValidationMode(cfg.validationMode);
      Inconclusive inconclusive = duplicates.getInconclusive().get();
      if (validationMode == ValidationMode.EXHAUSTIVE || !cfg.validateFsMdtDiff) {
        return concludeInconclusiveRun(validationMode, inconclusive,
            DuplicateBaseFileDimension.Result.none(), allPartitions.size());
      }
      // Fail-fast still runs dimension B: its window is its own and guarded by the same probes, so
      // it cannot be misled by what disturbed dimension A, and a real finding there pages this run.
      // Exhaustive does not, because a usable handle is what a mitigation job acts on, and a run
      // missing dimension A's verdict must not publish one.
      skipInconclusiveDuplicateDimension(inconclusive);
      duplicates = DuplicateBaseFileDimension.Result.none();
    }
    if (!cfg.validateFsMdtDiff) {
      LOG.info("--validate-fs-mdt-diff is off; the FS-MDT comparison is skipped for table {}",
          cfg.basePath);
      return concludeDuplicateOnlyRun(duplicates, allPartitions.size());
    }
    return doLatestFileSliceValidation(engineContext, allPartitions, duplicates);
  }

  /**
   * The union of the partitions the metadata table and storage report. Enumerating from one side
   * only would make partitions missing from the other side invisible; a partition missing from one
   * side shows up in the diff as a count mismatch.
   */
  private List<String> enumeratePartitions(HoodieSparkEngineContext engineContext) throws IOException {
    List<String> partitionsFromMetadata = FSUtils.getAllPartitionPaths(engineContext, metaClient, true);
    List<String> partitionsFromStorage = FSUtils.getAllPartitionPaths(engineContext, metaClient, false);
    Set<String> unionOfPartitions = new TreeSet<>(partitionsFromMetadata);
    unionOfPartitions.addAll(partitionsFromStorage);
    if (unionOfPartitions.size() != partitionsFromMetadata.size()
        || unionOfPartitions.size() != partitionsFromStorage.size()) {
      LOG.warn("Partition listings diverge for table {}: {} from the metadata table, {} from "
              + "storage, {} in total; every partition either side reports will be validated",
          cfg.basePath, partitionsFromMetadata.size(), partitionsFromStorage.size(),
          unionOfPartitions.size());
    }
    return new ArrayList<>(unionOfPartitions);
  }

  /**
   * Dimension A: lists each partition raw, reports every file slice with more than one base file
   * and, in exhaustive mode, adjudicates each suspect against the timeline on the executors. Opens no
   * metadata table; both timeline reads happen once on the driver so tasks cannot disagree about
   * which instants completed. Fail-fast raises from the executor, pre-empting dimension B;
   * otherwise the dimension's one failure is returned in the result.
   */
  private DuplicateBaseFileDimension.Result doDuplicateBaseFileDetection(
      HoodieSparkEngineContext engineContext, List<String> allPartitions) {
    int partitionsPerTask = Math.max(1, cfg.partitionsPerTask);
    int taskCount = (int) Math.ceil((double) allPartitions.size() / partitionsPerTask);
    LongAccumulator listingMs = jsc.sc().longAccumulator("mdtValidatorV2DuplicateListingMs");
    // Summed file count across all listings: the exhaustive document's denominator.
    LongAccumulator filesSeen = jsc.sc().longAccumulator("mdtValidatorV2DuplicateFilesSeen");

    HoodieTimeline tPrevD = metaClient.getActiveTimeline();
    String archivalAtPrevD = ArchivalProbe.signature(metaClient);
    // Read off T_prevD itself so the cutoff and the commit content executors judge by agree.
    String earliestActiveWriteCommit = InstantBuckets.earliestCompletedWriteCommit(
        tPrevD.getCommitsTimeline().filterCompletedInstants());
    // Seam 4: T_prevD is fixed; phase 1 lists after this returns.
    phaseObserver.afterDuplicateSnapshotPinned(tPrevD);

    DuplicateBaseFileDimension.Context ctx = new DuplicateBaseFileDimension.Context(
        parseValidationMode(cfg.validationMode), metaClient, earliestActiveWriteCommit,
        metaClient.getBasePath().toString(), Math.max(1, cfg.partitionThreads), cfg.ignoreFailed,
        cfg.validateDuplicateBaseFiles, listingMs, filesSeen);

    long startMs = System.currentTimeMillis();
    DuplicateBaseFileDimension.Result result = DuplicateBaseFileDimension.run(
        jsc, engineContext, allPartitions, taskCount, ctx, tPrevD, archivalAtPrevD,
        this::readTimelineAtTheDuplicateBarrier);
    LOG.info("Duplicate base file detection (v2) completed for table {} in {} ms across {} tasks "
            + "(partition listing summed {} ms)",
        cfg.basePath, System.currentTimeMillis() - startMs, taskCount, listingMs.value());

    // Non-zero in fail-fast only with --ignore-failed; the only place the table-wide total is logged.
    if (result.getSuspectCount() > 0) {
      LOG.error("Metadata table validation (v2) found {} duplicate base file suspect(s) in table {} "
              + "out of {} file(s) listed; the 'Duplicate base file' lines in the executor logs name "
              + "each file", result.getSuspectCount(), cfg.basePath, result.getFilesSeen());
    }
    if (result.getSanctionedCount() > 0) {
      LOG.info("Metadata table validation (v2) sanctioned {} duplicate base file suspect(s) as "
              + "possibly in-flight during this run for table {}", result.getSanctionedCount(),
          cfg.basePath);
    }
    if (!result.getConfirmedDangling().isEmpty()) {
      LOG.error("Metadata table validation (v2) proved {} duplicate base file(s) unreferenced in "
              + "table {}; {} were still referenced and {} could not be settled",
          result.getConfirmedDangling().size(), cfg.basePath, result.getReferencedCount(),
          result.getNotSettledCount());
    }
    return result;
  }

  /**
   * Seams 5 and 6 around the duplicate dimension's {@code T_aftD} read. Called by
   * {@link DuplicateBaseFileDimension#run} only after phase 1 has been forced to completion.
   */
  private HoodieTimeline readTimelineAtTheDuplicateBarrier() {
    phaseObserver.beforeDuplicatePostPhaseOneTimelineRead();
    HoodieTimeline tAftD = readTimelineThroughASeparateMetaClient();
    phaseObserver.afterDuplicatePostPhaseOneTimelineRead(tAftD);
    return tAftD;
  }

  /**
   * A run with {@code --validate-fs-mdt-diff=false}: dimension A alone. {@code fs_mdt_diff/} is
   * not written because nothing looked. As in {@link #doLatestFileSliceValidation},
   * {@code --ignore-failed=false} throws only after every report is on storage.
   */
  private boolean concludeDuplicateOnlyRun(DuplicateBaseFileDimension.Result duplicates,
                                           int partitionsValidated) {
    boolean finalResult = duplicates.getFailure().isEmpty();
    duplicates.getFailure().ifPresent(throwables::add);
    if (parseValidationMode(cfg.validationMode) == ValidationMode.EXHAUSTIVE) {
      writeFindings(Option.empty(), duplicates, partitionsValidated);
    }
    if (finalResult) {
      LOG.info("Metadata table validation (v2) of duplicate base files succeeded for table {}",
          cfg.basePath);
    } else {
      LOG.warn("Metadata table validation (v2) of duplicate base files failed for table {}",
          cfg.basePath);
    }
    if (!finalResult && !cfg.ignoreFailed) {
      throw firstValidationFailure();
    }
    return finalResult;
  }

  /**
   * Dimension B: the FS-MDT comparison. Reads {@code T_prev} and pins the metadata snapshot, then
   * compares each partition's metadata view with storage and reads {@code T_aft}. Dimension A's
   * result is carried in only for the exhaustive report and the combined verdict.
   *
   * @param engineContext the engine context the fan-outs run on
   * @param allPartitions every partition either side of the table reports
   * @param duplicates    what dimension A found, for the report and the run's combined verdict
   */
  private boolean doLatestFileSliceValidation(HoodieSparkEngineContext engineContext,
                                              List<String> allPartitions,
                                              DuplicateBaseFileDimension.Result duplicates)
      throws Exception {
    long validationStartMs = System.currentTimeMillis();
    ValidationMode validationMode = parseValidationMode(cfg.validationMode);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .build();

    // Must match the executors' spelling (RemoteHoodieTableFileSystemView keys on
    // metaClient.getBasePath()); FileSystemViewManager caches by raw string, so cfg.basePath would
    // build a second, cold view.
    String basePath = metaClient.getBasePath().toString();

    // Only RocksDB and the spillable map bound the driver view; see parseServerViewStorageType.
    FileSystemViewStorageType serverViewStorageType = parseServerViewStorageType(cfg.fsViewStorageType);
    FileSystemViewStorageConfig serverViewConfig = FileSystemViewStorageConfig.newBuilder()
        .fromProperties(props)
        .withStorageType(serverViewStorageType)
        .withRocksDBPath(cfg.fsViewBaseStorePath)
        .withBaseStoreDir(cfg.fsViewBaseStorePath)
        .withMaxMemoryForView(driverViewMemoryBudget())
        .build();
    // Properties first so the settings this validator owns win over --hoodie-conf.
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .withPath(basePath)
        .withMetadataConfig(metadataConfig)
        .withFileSystemViewConfig(serverViewConfig)
        .withEmbeddedTimelineServerEnabled(true)
        // Forced off so --hoodie-conf cannot enable it. A reused service keeps the first creator's
        // view storage config (possibly unbounded MEMORY), and a co-tenant on the same table shares
        // its lifecycle, so whichever calls stopForBasePath first closes the server for both.
        .withEmbeddedTimelineServerReuseEnabled(false)
        .build();

    // Hosts the metadata-backed view with reuse=true readers, so all partition requests share one
    // merged log scan, and points the write config's view storage at this driver.
    EmbeddedTimelineService timelineService =
        EmbeddedTimelineServerHelper.createEmbeddedTimelineService(engineContext, writeConfig);

    // Held outside the try so the finally stops whichever service the pinning loop ends up holding.
    MetadataSnapshot snapshot = null;
    // Also released by the finally: the one closeable this method constructs itself.
    HoodieTableFileSystemView fsViewTemplate = null;
    try {
      LOG.info("Validating latest file slices (v2) for {} partitions of table {}",
          allPartitions.size(), cfg.basePath);

      // Steps 1-3: pin the metadata snapshot to a timeline that did not move while it was built.
      // Strict by design: a busy table gives up rather than use a timeline that cannot explain it.
      snapshot = new MetadataSnapshot(engineContext, writeConfig, basePath, allPartitions,
          timelineService);
      PinResult pin = pinMetadataSnapshot(snapshot);
      if (!pin.isPinned()) {
        return concludeUnpinnedRun(validationMode, duplicates, allPartitions.size());
      }
      HoodieTimeline tPrev = pin.getTimeline();
      // Taken immediately: the window that must be free of archival is exactly [T_prev, T_aft].
      String archivalAtPrev = ArchivalProbe.signature(metaClient);
      // State of every instant on the full active timeline at T_prev, built once on the driver.
      InstantStateIndex prevIndex = InstantStateIndex.from(tPrev);

      // Seam 1. Work done here lands on the storage side of the comparison, not the metadata side.
      phaseObserver.afterSnapshotPinned(tPrev);

      // Read only now: createEmbeddedTimelineService rewrites the view storage config, so a pinning
      // retry leaves an earlier read naming a server that no longer exists.
      FileSystemViewStorageConfig remoteViewConfig = writeConfig.getViewStorageConfig();
      HoodieTableMetaClient serializableMetaClient = metaClient;
      boolean ignoreFailed = cfg.ignoreFailed;

      // Built once on the driver and serialized into the task closure, so tasks inherit the costly
      // init() timeline scans instead of repeating them per partition. It must use the same visible
      // timeline as the metadata side (completed plus compaction instants), or slices whose base
      // instant is a pending compaction would be hidden and every such MOR partition would fail.
      long fsViewBuildStartMs = System.currentTimeMillis();
      fsViewTemplate = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          engineContext, metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants());
      long fsViewBuildElapsedMs = System.currentTimeMillis() - fsViewBuildStartMs;

      // Summed per-partition storage listing time, which dominates the fan-out.
      LongAccumulator partitionValidationMsAccumulator =
          jsc.sc().longAccumulator("mdtValidatorV2PartitionValidationMs");

      // Several partitions per task amortize the per-task remote view and timeline filter costs.
      int partitionsPerTask = Math.max(1, cfg.partitionsPerTask);
      int partitionThreads = Math.max(1, cfg.partitionThreads);
      long maxFilesPerRound = Math.max(1, cfg.maxFilesPerRound);
      int taskCount = (int) Math.ceil((double) allPartitions.size() / partitionsPerTask);
      LOG.info("Validating {} partitions of table {} across {} tasks ({} partitions per task)",
          allPartitions.size(), cfg.basePath, taskCount, partitionsPerTask);

      FsMdtDiffDimension.Context diffContext = new FsMdtDiffDimension.Context(validationMode,
          serializableMetaClient, remoteViewConfig, fsViewTemplate, basePath, partitionThreads,
          maxFilesPerRound, partitionValidationMsAccumulator, props);

      boolean finalResult = duplicates.getFailure().isEmpty();
      duplicates.getFailure().ifPresent(throwables::add);

      // Steps 4-5 and phase 2: forces phase 1, reads T_aft, and adjudicates against the delta.
      FsMdtDiffDimension.Result diff = FsMdtDiffDimension.run(jsc, allPartitions, taskCount,
          diffContext, tPrev, prevIndex, this::readTimelineAtTheBarrier,
          archivalAtPrev);
      if (diff.isInconclusive()) {
        return concludeInconclusiveRun(validationMode, diff.getInconclusive().get(),
            duplicates, allPartitions.size());
      }
      ServiceActivityDelta dlt = diff.getServiceActivity();

      // Excludes the exhaustive report writes: a run's cost is what it spent looking at the table.
      LOG.info("FS-MDT comparison (v2) completed for table {} in {} ms: metadata table open "
              + "{} ms, driver warm-up of {} partitions ({} file slices) {} ms, file system view build "
              + "{} ms, phase 1 {} ms across {} tasks (partition listing summed {} ms), phase 2 {} "
              + "ms, snapshot pinned on attempt {} of {}",
          cfg.basePath, System.currentTimeMillis() - validationStartMs, snapshot.viewBuildElapsedMs,
          allPartitions.size(), snapshot.warmedSliceCount, snapshot.warmupElapsedMs,
          fsViewBuildElapsedMs, diff.getPhaseOneElapsedMs(), taskCount,
          partitionValidationMsAccumulator.value(), diff.getPhaseTwoElapsedMs(),
          pin.getAttempts(), MAX_PIN_ATTEMPTS);

      // Not a fast path: the diff is non-empty whenever any instant departed, archival included.
      LOG.info("FS-MDT diff for table {}: {} record(s) across {} partition(s), {} of which no "
              + "adjudication row named; the active timeline's newest instant moved from {} to {} "
              + "while the run was in progress (no clean or rollback activity: {}, plan or metadata "
              + "read failed: {})",
          cfg.basePath, diff.getDiffCount(), allPartitions.size(), diff.getUnmatchedStateCount(),
          newestInstantTime(prevIndex), diff.getAftNewestInstant(), dlt.noServiceActivity(),
          dlt.readFailed());

      // One failure per dimension: getThrowables() counts dimensions, not damaged partitions.
      if (diff.getFailure().isPresent()) {
        finalResult = false;
        throwables.add(diff.getFailure().get());
      }

      // Written whether or not validation passed: sanctioned findings are evidence too. Fail-fast
      // writes nothing; its suspects are logged on the executors.
      if (validationMode == ValidationMode.EXHAUSTIVE) {
        writeFindings(Option.of(diff), duplicates, allPartitions.size());
      }

      if (finalResult) {
        LOG.info("Metadata table validation (v2) of latest file slices succeeded for table {}", cfg.basePath);
      } else {
        LOG.warn("Metadata table validation (v2) of latest file slices failed for table {}", cfg.basePath);
      }
      // Exhaustive collects unconditionally, so --ignore-failed=false is honoured here, after every
      // report is on storage. Fail-fast also raises here: the FS-MDT verdict is decided on the driver.
      if (!finalResult && !cfg.ignoreFailed) {
        throw firstValidationFailure();
      }
      return finalResult;
    } finally {
      // Neither release may throw: this finally is on a validation failure's exit path, and a
      // teardown exception would replace that verdict with an infrastructure error.
      //
      // Stop whichever service the snapshot ended up holding (a pinning retry replaces it), under
      // the same base path spelling it was registered with.
      try {
        (snapshot == null ? timelineService : snapshot.currentService()).stopForBasePath(basePath);
      } catch (Exception e) {
        LOG.warn("Could not stop the embedded timeline service for table {}; the validation verdict "
            + "is unaffected", cfg.basePath, e);
      }
      // Closed only here: the view is serialized into every task closure and close() nulls its
      // state, so it must outlive the last Spark job that reads it.
      if (fsViewTemplate != null) {
        try {
          fsViewTemplate.close();
        } catch (Exception e) {
          LOG.warn("Could not close the file system view of table {}; the validation verdict is "
              + "unaffected", cfg.basePath, e);
        }
      }
    }
  }

  /**
   * Concludes a run that could not pin its metadata snapshot. Only dimension B is skipped; dimension
   * A's findings stand (see {@link #publishSkippedRunFindings}).
   *
   * <p>Fail-fast logs {@link #PINNING_SKIPPED_MARKER} and returns success, since a continuously
   * written table would otherwise fail every run for being busy; alerting counts markers and
   * {@link #getRunReports()} reports {@link ValidationRunReports#skipped}. Exhaustive throws a
   * {@link HoodieValidationException}, since that mode must never silently produce no document.
   *
   * @return on the fail-fast path, {@code true} unless dimension A failed
   */
  @VisibleForTesting
  boolean concludeUnpinnedRun(ValidationMode validationMode,
                              DuplicateBaseFileDimension.Result duplicates,
                              int partitionsValidated) {
    // Dimension A pins nothing, so its findings survive a failed pin; only B is reported skipped.
    publishSkippedRunFindings(validationMode, duplicates, partitionsValidated);
    duplicates.getFailure().ifPresent(throwables::add);
    if (validationMode == ValidationMode.EXHAUSTIVE) {
      BranchBeacon.hit(Branch.MTV_UNPINNED_EXHAUSTIVE_FAILED);
      raiseDuplicateFailureUnlessIgnored(duplicates);
      throw new HoodieValidationException(
          "Could not pin the metadata snapshot to a stable timeline for " + cfg.basePath + " in "
              + MAX_PIN_ATTEMPTS + " attempts; the FS-MDT comparison was not run and no findings "
              + "were written for it");
    }
    // The marker is the alerting contract; the beacon puts this outcome in the enumerated surface.
    BranchBeacon.hit(Branch.MTV_UNPINNED_FAIL_FAST_SKIPPED);
    LOG.warn(pinningSkippedMessage(cfg.basePath));
    return duplicateVerdictOfSkippedRun(duplicates);
  }

  /**
   * Dimension A's verdict on a fail-fast run that skipped dimension B. The skip itself passes, so
   * the run fails only when dimension A did, and then exactly as a completed run would: raised
   * unless {@code --ignore-failed}, returned as {@code false} otherwise.
   */
  private boolean duplicateVerdictOfSkippedRun(DuplicateBaseFileDimension.Result duplicates) {
    if (!duplicates.getFailure().isPresent()) {
      return true;
    }
    raiseDuplicateFailureUnlessIgnored(duplicates);
    return false;
  }

  /**
   * Raises dimension A's failure ahead of a skip's own, so an operator reads the finding rather than
   * the reason dimension B did not run.
   */
  private void raiseDuplicateFailureUnlessIgnored(DuplicateBaseFileDimension.Result duplicates) {
    if (duplicates.getFailure().isPresent() && !cfg.ignoreFailed) {
      throw duplicates.getFailure().get();
    }
  }

  /**
   * Publishes dimension A's findings for a run that skipped dimension B, written in exhaustive mode
   * before the caller throws. A clean dimension A is reported skipped rather than handed to
   * {@link #writeFindings}, whose usable clean handle would claim the whole table is clean.
   */
  private void publishSkippedRunFindings(ValidationMode validationMode,
                                         DuplicateBaseFileDimension.Result duplicates,
                                         int partitionsValidated) {
    if (validationMode == ValidationMode.EXHAUSTIVE && cfg.validateDuplicateBaseFiles
        && dimensionAFoundSomething(duplicates)) {
      writeFindings(Option.empty(), duplicates, partitionsValidated);
      return;
    }
    runReports = Option.of(ValidationRunReports.skipped(resolveRunId()));
  }

  /**
   * Whether dimension A has anything to write, by the same rule {@link #writeFindings} uses. Asked
   * before calling it, because its clean-run path publishes a usable handle.
   */
  private static boolean dimensionAFoundSomething(DuplicateBaseFileDimension.Result duplicates) {
    return !duplicates.getSuspects().isEmpty();
  }

  /**
   * Concludes a run whose timeline delta cannot be trusted (archival overlapped it, or phase 1 was
   * recomputed), with the same two answers as {@link #concludeUnpinnedRun}. No FS-MDT verdict is
   * reached; dimension A adjudicates against its own reads and stands.
   *
   * <p>Fail-fast logs the cause's {@link InconclusiveCause#marker() marker} and returns success,
   * since archival is routine. Exhaustive throws, since a document built on an incomplete delta
   * would read as a clean table. No retry: this is detected only after the whole diff.
   *
   * @return on the fail-fast path, {@code true} unless dimension A failed
   */
  @VisibleForTesting
  boolean concludeInconclusiveRun(ValidationMode validationMode, Inconclusive inconclusive,
                                  DuplicateBaseFileDimension.Result duplicates,
                                  int partitionsValidated) {
    // Dimension A's failure is already in throwables: the caller collects it before dimension B.
    publishSkippedRunFindings(validationMode, duplicates, partitionsValidated);
    InconclusiveCause cause = inconclusive.getCause();
    if (validationMode == ValidationMode.EXHAUSTIVE) {
      BranchBeacon.hit(cause.exhaustiveBeacon());
      raiseDuplicateFailureUnlessIgnored(duplicates);
      throw new HoodieValidationException(
          cause.messagePrefix() + cfg.basePath + ": " + inconclusive.getReason()
              + cause.messageTail());
    }
    BranchBeacon.hit(cause.failFastBeacon());
    LOG.warn(skippedMessage(cause, cfg.basePath, inconclusive.getReason()));
    return duplicateVerdictOfSkippedRun(duplicates);
  }

  /**
   * What a fail-fast run whose dimension A was inconclusive records before running dimension B
   * alone: the cause's marker and beacon, and a handle saying the run is skipped, since half of it
   * never looked.
   */
  private void skipInconclusiveDuplicateDimension(Inconclusive inconclusive) {
    InconclusiveCause cause = inconclusive.getCause();
    BranchBeacon.hit(cause.failFastBeacon());
    LOG.warn(duplicateDimensionSkippedMessage(cause, cfg.basePath, inconclusive.getReason()));
    runReports = Option.of(ValidationRunReports.skipped(resolveRunId()));
  }

  /**
   * The line a fail-fast run leaves when only dimension A was skipped. It carries the same marker as
   * {@link #skippedMessage}, so alerting counts it the same way.
   */
  @VisibleForTesting
  static String duplicateDimensionSkippedMessage(InconclusiveCause cause, String basePath,
                                                 String reason) {
    return cause.marker() + " table=" + basePath + " - " + reason
        + "; the duplicate base file check reached no verdict on this run, the FS-MDT comparison "
        + "still runs";
  }

  /**
   * The one line a skipped fail-fast run leaves, built in one place so the marker alerting keys on
   * can be asserted. The marker comes from the cause, so each cause is a distinct alerting fact.
   */
  @VisibleForTesting
  static String skippedMessage(InconclusiveCause cause, String basePath, String reason) {
    return cause.marker() + " table=" + basePath + " - " + reason
        + "; nothing was validated on this run";
  }

  /**
   * The id a run reports itself under: the caller's, else a UTC timestamp. Resolved once per handle
   * so a directory name and the handle never drift apart.
   */
  private String resolveRunId() {
    return StringUtils.isNullOrEmpty(cfg.validationRunId)
        ? LocalDateTime.now(ZoneOffset.UTC).format(RUN_TIMESTAMP_FORMAT)
        : cfg.validationRunId;
  }

  /**
   * The one line a skipped fail-fast run leaves behind, built here rather than formatted at the call
   * site so the marker alerting keys on has exactly one definition and can be asserted.
   */
  @VisibleForTesting
  static String pinningSkippedMessage(String basePath) {
    return PINNING_SKIPPED_MARKER + " table=" + basePath + " - the active timeline changed during "
        + "every one of the " + MAX_PIN_ATTEMPTS + " attempts to build the metadata snapshot; "
        + "nothing was validated on this run";
  }

  /** Steps 1-3 behind an instance seam, so a test can drive a real run into the unpinned branch. */
  @VisibleForTesting
  protected PinResult pinMetadataSnapshot(SnapshotAttempt attempt) throws IOException {
    return pinSnapshot(attempt);
  }

  /**
   * The three things one pinning attempt does, behind a seam so the attempt policy can be driven
   * without a table, a Spark context or a timeline server.
   */
  @VisibleForTesting
  public interface SnapshotAttempt {

    /** Steps 1 and 2: reads the active timeline and builds (and warms) the view against it. */
    HoodieTimeline buildSnapshot() throws IOException;

    /** Step 3's second read, through a meta client of its own. */
    HoodieTimeline reReadTimeline();

    /** Throws the snapshot away so the next attempt can build a fresh one. */
    void discardSnapshot() throws IOException;
  }

  /** Whether the snapshot could be pinned, the timeline it was pinned to, and what it cost. */
  @VisibleForTesting
  public static final class PinResult {

    private final HoodieTimeline timeline;
    private final int attempts;

    private PinResult(HoodieTimeline timeline, int attempts) {
      this.timeline = timeline;
      this.attempts = attempts;
    }

    public boolean isPinned() {
      return timeline != null;
    }

    /** {@code T_prev}: the timeline the accepted snapshot was built against. Null when unpinned. */
    public HoodieTimeline getTimeline() {
      return timeline;
    }

    public int getAttempts() {
      return attempts;
    }
  }

  /**
   * Steps 1-3: builds the metadata snapshot and accepts it only if the active timeline did not move
   * while it was built, compared as {@code (instantTime, action, state)} triples so a
   * requested-to-inflight transition also counts.
   *
   * <p>The snapshot is discarded only between attempts; the last attempt's timeline server belongs
   * to the caller's {@code finally}.
   */
  @VisibleForTesting
  static PinResult pinSnapshot(SnapshotAttempt attempt) throws IOException {
    for (int attemptNo = 1; attemptNo <= MAX_PIN_ATTEMPTS; attemptNo++) {
      HoodieTimeline tPrev = attempt.buildSnapshot();
      HoodieTimeline tMid = attempt.reReadTimeline();
      if (timelineState(tPrev).equals(timelineState(tMid))) {
        return new PinResult(tPrev, attemptNo);
      }
      LOG.warn("The active timeline moved while the metadata snapshot was being built (attempt {} "
          + "of {}); the snapshot cannot be reconciled against it", attemptNo, MAX_PIN_ATTEMPTS);
      if (attemptNo < MAX_PIN_ATTEMPTS) {
        attempt.discardSnapshot();
      }
    }
    return new PinResult(null, MAX_PIN_ATTEMPTS);
  }

  /**
   * The {@code (instantTime, action, state)} triples of the whole active timeline, every action
   * included, since a clean, rollback or compaction moves the snapshot as surely as a commit.
   */
  private static Set<String> timelineState(HoodieTimeline timeline) {
    Set<String> triples = new TreeSet<>();
    for (HoodieInstant instant : timeline.getInstants()) {
      triples.add(instant.requestedTime() + "|" + instant.getAction() + "|" + instant.getState());
    }
    return triples;
  }

  /**
   * Seams 2 and 3 around the {@code T_aft} read. Called by
   * {@link FsMdtDiffDimension#readAfterBarrier} only after phase 1 has been forced to completion.
   */
  private HoodieTimeline readTimelineAtTheBarrier() {
    phaseObserver.beforePostPhaseOneTimelineRead();
    HoodieTimeline tAft = readTimelineThroughASeparateMetaClient();
    phaseObserver.afterPostPhaseOneTimelineRead(tAft);
    return tAft;
  }

  /**
   * {@code T_mid} and {@code T_aft}, read through a meta client of its own. Never through
   * {@code metaClient}: its timeline rides in every task closure, and a timeline-hash mismatch
   * makes the served view re-sync and reject requests with HTTP 400 (no fallback view).
   */
  private HoodieTimeline readTimelineThroughASeparateMetaClient() {
    return HoodieTableMetaClient.builder()
        .setConf(metaClient.getStorageConf().newInstance())
        .setBasePath(metaClient.getBasePath())
        .setLoadActiveTimelineOnLoad(true)
        .build()
        .getActiveTimeline();
  }

  /** The newest instant time in {@code index}, or null when it holds none. */
  private static String newestInstantTime(InstantStateIndex index) {
    List<String> ordered = index.getOrderedInstants();
    return ordered.isEmpty() ? null : ordered.get(ordered.size() - 1);
  }

  /**
   * The metadata snapshot one pinning attempt builds: the driver-hosted, metadata-table-backed file
   * system view, served through Hudi's embedded timeline server.
   */
  private final class MetadataSnapshot implements SnapshotAttempt {

    private final HoodieSparkEngineContext engineContext;
    private final HoodieWriteConfig writeConfig;
    private final String basePath;
    private final List<String> allPartitions;

    private EmbeddedTimelineService timelineService;
    private long viewBuildElapsedMs;
    private long warmupElapsedMs;
    private long warmedSliceCount;

    MetadataSnapshot(HoodieSparkEngineContext engineContext, HoodieWriteConfig writeConfig,
                     String basePath, List<String> allPartitions,
                     EmbeddedTimelineService timelineService) {
      this.engineContext = engineContext;
      this.writeConfig = writeConfig;
      this.basePath = basePath;
      this.allPartitions = allPartitions;
      this.timelineService = timelineService;
    }

    @Override
    public HoodieTimeline buildSnapshot() {
      // Step 1, a fresh read on every attempt. Reloads THIS meta client because the executors carry
      // it and the served view is keyed on its timeline hash.
      HoodieTimeline tPrev = metaClient.reloadActiveTimeline();

      // Step 2. Warm the view on the driver so the first burst of executor requests cannot race the
      // request handler's view-sync/reset path. The first getFileSystemView call opens the metadata
      // table and merges its logs, so it is timed apart from the per-partition lookups.
      //
      // Built from our meta client, not cfg.basePath, so the server view is pinned to the same
      // timeline the executors carry; a hash mismatch fails their requests with HTTP 400.
      long viewBuildStartMs = System.currentTimeMillis();
      SyncableFileSystemView mdtView = timelineService.getViewManager().getFileSystemView(metaClient);
      viewBuildElapsedMs = System.currentTimeMillis() - viewBuildStartMs;

      long warmupStartMs = System.currentTimeMillis();
      long slices = 0;
      for (String partitionPath : allPartitions) {
        slices += mdtView.getLatestFileSlices(partitionPath).count();
      }
      warmedSliceCount = slices;
      warmupElapsedMs = System.currentTimeMillis() - warmupStartMs;
      return tPrev;
    }

    @Override
    public HoodieTimeline reReadTimeline() {
      return readTimelineThroughASeparateMetaClient();
    }

    /**
     * Replaces the timeline server: the view manager caches one view per service, so a new service
     * is the only way to build a view against a newly read timeline.
     */
    @Override
    public void discardSnapshot() throws IOException {
      timelineService.stopForBasePath(basePath);
      timelineService =
          EmbeddedTimelineServerHelper.createEmbeddedTimelineService(engineContext, writeConfig);
    }

    EmbeddedTimelineService currentService() {
      return timelineService;
    }
  }

  /**
   * Writes what exhaustive mode found, on the driver, and records the resulting handle.
   *
   * <pre>
   * &lt;root&gt;/&lt;dir&gt;/&lt;timestamp&gt;/fs_mdt_diff/diff/         every differing file, verdict and all
   *                              fs_mdt_diff/adjudicated/  the same, minus what was explained
   *                              dangling_files/           the dangling-file suspects, unadjudicated
   *                              adjudicated/              the suspects the timeline proved deletable
   * </pre>
   *
   * <p>An absent {@code diff} means dimension B did not run, so no {@code fs_mdt_diff/} document is
   * written and the handle reports {@link ValidationRunReports#isFsMdtDiffAssessed()} false. A run that found nothing writes nothing. Nothing here may change the verdict: each write
   * is guarded on its own, and a failed write makes the handle refused rather than clean.
   */
  private void writeFindings(Option<FsMdtDiffDimension.Result> diff,
                             DuplicateBaseFileDimension.Result duplicates,
                             int partitionsValidated) {
    List<MismatchRecord> danglingSuspects = duplicates.getSuspects();
    // Resolved once and reused for every handle this call can produce.
    String timestamp = resolveRunId();
    // Whether this run found anything, across both dimensions. Not the writers' own gates: they
    // count filesSeen, non-zero on every healthy table, and would write a document every run.
    boolean foundSomething = false;
    if (diff.isPresent()) {
      // Drop counts count: a run where every file was excluded from adjudication found something.
      foundSomething = diff.get().getUnmatchedStateCount() > 0
          || !diff.get().getRecords().isEmpty()
          || !diff.get().getDropCounts().isEmpty();
    }
    foundSomething |= !danglingSuspects.isEmpty();
    if (!foundSomething) {
      // A clean run: usable, and it names nothing. Without dimension B it vouches for dimension A only.
      runReports = Option.of(diff.isPresent()
          ? ValidationRunReports.of(timestamp, Option.empty(), Option.empty(), Option.empty())
          : ValidationRunReports.withoutFsMdtDiff(timestamp, Option.empty(), Option.empty()));
      return;
    }
    String outputRoot = StringUtils.isNullOrEmpty(cfg.validationOutputRoot)
        ? metaClient.getBasePath().toString() : cfg.validationOutputRoot;
    String outputDir = cfg.validationOutputDir;
    String mode = ValidationMode.EXHAUSTIVE.getFlagValue();

    // An empty directory means either nothing to write or a failed write; reconcile() must tell.
    List<String> failedWrites = new ArrayList<>();
    // Absent unless dimension B ran: an empty fs_mdt_diff/ would claim it ran and found nothing.
    Option<String> diffDir = Option.empty();
    Option<String> diffAdjudicatedDir = Option.empty();
    if (diff.isPresent()) {
      FsMdtDiffDimension.Result diffResult = diff.get();
      diffDir = persist("FS-MDT diff", failedWrites,
          () -> FileDiffReportWriter.writeDiff(metaClient.getStorage(), outputRoot, outputDir,
              timestamp, cfg.basePath, mode, partitionsValidated, diffResult.getRecords(),
              diffResult.getFilesSeen(), diffResult.getDropCounts(),
              diffResult.getUnmatchedStateCount()));
      diffAdjudicatedDir = persist("adjudicated FS-MDT diff", failedWrites,
          () -> FileDiffReportWriter.writeAdjudicated(metaClient.getStorage(), outputRoot, outputDir,
              timestamp, cfg.basePath, mode, partitionsValidated, diffResult.getRecords(),
              diffResult.getFilesSeen(), diffResult.getDropCounts(),
              diffResult.getUnmatchedStateCount()));
    }
    Option<String> danglingDir = persist("dangling-file suspects", failedWrites,
        () -> DanglingReportWriter.writeSuspects(metaClient.getStorage(), outputRoot,
            outputDir, timestamp, cfg.basePath, mode, partitionsValidated, danglingSuspects,
            duplicates.getFilesSeen()));

    if (!failedWrites.isEmpty()) {
      runReports = Option.of(ValidationRunReports.refused(timestamp));
      // Still called so it logs why it declines; the refusal above is what a consumer acts on.
      reconcile(outputRoot, outputDir, timestamp, duplicates, partitionsValidated,
          failedWrites);
      return;
    }
    Option<String> adjudicatedDir = reconcile(outputRoot, outputDir, timestamp, duplicates,
        partitionsValidated, failedWrites);
    if (!failedWrites.isEmpty()) {
      // The reconciliation threw. With no adjudicated/ directory the handle would otherwise read as
      // clean while the raw reports hold findings, so it is reported as a refusal.
      runReports = Option.of(ValidationRunReports.refused(timestamp));
      return;
    }
    // The two dimensions conclude independently, so each adjudicated document has its own slot.
    // Without dimension B the handle must say so: its empty FS-MDT slots would otherwise read as clean.
    runReports = Option.of(diff.isPresent()
        ? ValidationRunReports.of(timestamp, diffDir, diffAdjudicatedDir, danglingDir, adjudicatedDir)
        : ValidationRunReports.withoutFsMdtDiff(timestamp, danglingDir, adjudicatedDir));
  }

  /** One report write. Separate from {@link java.util.concurrent.Callable} only to name it. */
  @FunctionalInterface
  private interface ReportWrite {
    Option<String> write() throws Exception;
  }

  /**
   * Writes one report and records it by name in {@code failedWrites} if it throws, so
   * {@link #reconcile} can tell a lost report from an empty one. Never rethrows: the verdict never
   * depends on whether evidence could be saved.
   */
  private Option<String> persist(String what, List<String> failedWrites, ReportWrite write) {
    try {
      return write.write();
    } catch (Exception e) {
      failedWrites.add(what);
      LOG.error("Could not write the {} for table {}; the validation verdict is unaffected", what,
          cfg.basePath, e);
      return Option.empty();
    }
  }

  /**
   * Writes dimension A's adjudicated report from its confirmed-dangling records, which come off the
   * same executor job as the raw suspects and need no read-back. Refused when any raw report failed
   * to write, since the result could not be re-derived. Never throws or changes the verdict; its own
   * failure is added to {@code failedWrites}. Dimension A's confirmed-dangling verdict is final and
   * {@code fs_mdt_diff/} does not veto it; an empty return does not mean the table is clean.
   *
   * @param duplicates          what dimension A found; its {@code confirmedDangling} records are
   *                            this document's whole input
   * @param partitionsValidated every partition either side reported, for the document header
   * @param failedWrites in: raw reports that could not be written; out: also this pass's failure
   * @return the adjudicated report's directory, or {@link Option#empty()} if reconciliation was
   *         refused, found nothing to adjudicate, or failed
   */
  private Option<String> reconcile(String outputRoot, String outputDir, String timestamp,
                                   DuplicateBaseFileDimension.Result duplicates,
                                   int partitionsValidated, List<String> failedWrites) {
    if (!failedWrites.isEmpty()) {
      LOG.error("Not reconciling the exhaustive findings for table {}: the {} could not be written, "
              + "and an adjudicated document written from an incomplete set of raw reports is one "
              + "nobody can re-derive from what is on storage. Whatever was written stands and the "
              + "pass can be re-run against it; the validation verdict is unaffected", cfg.basePath,
          String.join(" and the ", failedWrites));
      return Option.empty();
    }
    if (duplicates.getConfirmedDangling().isEmpty()) {
      // Nothing proven unreferenced. FS-MDT findings, if any, carry their own verdicts elsewhere.
      return Option.empty();
    }
    try {
      return DanglingReportWriter.writeAdjudicated(metaClient.getStorage(), outputRoot, outputDir,
          timestamp, documentOfConfirmed(duplicates, timestamp, partitionsValidated));
    } catch (Exception e) {
      // Recorded like a raw write failure, so the caller does not read the absent adjudicated/ as
      // clean.
      failedWrites.add("adjudicated report");
      LOG.error("Could not reconcile the exhaustive findings for table {} into an adjudicated "
              + "report; the raw reports stand and the validation verdict is unaffected. This "
              + "run reached no adjudicated conclusion and must not be read as a clean one",
          cfg.basePath, e);
      return Option.empty();
    }
  }

  /**
   * The proven-unreferenced records as an adjudicated document with the raw document's header
   * ({@code filesSeen} and {@code partitionsValidated} passed through, so the two cannot disagree).
   * Sorted because the records arrive off a shuffle, so identical tables yield identical documents.
   */
  private MismatchReportWriter.Document documentOfConfirmed(
      DuplicateBaseFileDimension.Result duplicates, String timestamp, int partitionsValidated) {
    List<MismatchRecord> confirmed = new ArrayList<>(duplicates.getConfirmedDangling());
    Collections.sort(confirmed);
    return MismatchReportWriter.documentOf(cfg.basePath, timestamp,
        ValidationMode.EXHAUSTIVE.getFlagValue(), partitionsValidated, duplicates.getFilesSeen(),
        confirmed);
  }

  /**
   * The failure {@code --ignore-failed=false} raises once every report is on storage: the first
   * collected one, matching what fail-fast would throw. The fallback, for a caller that dropped a
   * collected failure, names no dimension because either may have been switched off.
   */
  private HoodieValidationException firstValidationFailure() {
    for (Throwable throwable : throwables) {
      if (throwable instanceof HoodieValidationException) {
        return (HoodieValidationException) throwable;
      }
    }
    return new HoodieValidationException(
        "Metadata table validation (v2) failed for table: " + cfg.basePath);
  }

  /**
   * Accepts only the two storage types that bound the driver-hosted view. {@code MEMORY} is
   * unbounded on the driver heap, and {@code REMOTE_FIRST}/{@code REMOTE_ONLY} are rewritten to
   * {@code MEMORY} by {@link EmbeddedTimelineService}.
   */
  private static FileSystemViewStorageType parseServerViewStorageType(String configured) {
    FileSystemViewStorageType storageType;
    try {
      storageType = FileSystemViewStorageType.valueOf(configured);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown --fs-view-storage-type '" + configured
          + "'; the v2 validator supports " + SUPPORTED_SERVER_VIEW_STORAGE_TYPES, e);
    }
    ValidationUtils.checkArgument(SUPPORTED_SERVER_VIEW_STORAGE_TYPES.contains(storageType),
        "--fs-view-storage-type " + storageType + " does not bound the driver-hosted view and can "
            + "OOM the driver on a large table; the v2 validator supports "
            + SUPPORTED_SERVER_VIEW_STORAGE_TYPES);
    return storageType;
  }

  /**
   * Parses {@code --validation-mode}. Called from {@link #validateConfig} so a typo fails the run
   * before anything is read instead of silently downgrading to fail-fast.
   *
   * @param value the raw flag value, which may be null or empty
   * @return the parsed mode, {@link ValidationMode#FAIL_FAST} when unset
   * @throws IllegalArgumentException if the value is not a known mode
   */
  @VisibleForTesting
  static ValidationMode parseValidationMode(String value) {
    return ValidationMode.fromFlagValue(value);
  }

  /**
   * Decides what a failure means, which decides whether someone is paged for corruption. In order:
   * a {@link HoodieValidationException} anywhere in the cause chain wins, since an observed
   * divergence must not be buried by a later teardown; else an infrastructure signature yields a
   * {@link HoodieException} that {@link #hasValidationFailure()} does not count; else the failure
   * passes through. The whole chain is searched because Spark wraps task failures.
   */
  @VisibleForTesting
  static RuntimeException classifyFailure(Exception e) {
    return ValidationFailures.classifyFailure(e);
  }

  /**
   * The infrastructure signature matched by {@code throwable}'s cause chain, if any.
   *
   * @return the matched fragment, for the operator to read in the log line; empty when nothing in
   *         the chain looks like the cluster going away.
   */
  @VisibleForTesting
  static Option<String> matchInfraFailure(Throwable throwable) {
    return ValidationFailures.matchInfraFailure(throwable);
  }

  /**
   * View budget = fraction of the Spark execution memory pool on the driver:
   * (max heap - 300MB reserved) x spark.memory.fraction x (1 - spark.memory.storageFraction),
   * scaled by {@code --fs-view-memory-fraction} (default 0.5). Beyond this the view
   * spills to disk.
   */
  private long driverViewMemoryBudget() {
    org.apache.spark.SparkConf sparkConf = jsc.getConf();
    double memoryFraction = sparkConf.getDouble("spark.memory.fraction", 0.6);
    double storageFraction = sparkConf.getDouble("spark.memory.storageFraction", 0.5);
    long reservedBytes = 300L * 1024 * 1024;
    long usableHeap = Math.max(Runtime.getRuntime().maxMemory() - reservedBytes, 0L);
    long executionMemory = (long) (usableHeap * memoryFraction * (1 - storageFraction));
    return (long) (executionMemory * cfg.fsViewMemoryFraction);
  }

  /**
   * Checks the reports' output directory before anything is read: hidden (a courtesy only; partition
   * discovery and the storage-side pass never read reports as table data), not inside
   * {@code .hoodie/}, and free of {@code .}, {@code ..} and colons. Checked per segment on the raw
   * string, because {@code StoragePath} normalises the evidence away. See also
   * {@link #validateOutputRoot}.
   */
  @VisibleForTesting
  static void validateOutputDir(String outputDir) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(outputDir),
        "--validation-output-dir must not be empty");
    ValidationUtils.checkArgument(outputDir.startsWith("."),
        "--validation-output-dir must be hidden (start with '.'); got " + outputDir);
    String normalized = outputDir.replace('\\', '/');
    ValidationUtils.checkArgument(normalized.indexOf(':') < 0,
        "--validation-output-dir must not contain a colon, which can be read as a URI scheme; got "
            + outputDir);
    // Per segment, so an offending segment anywhere is caught. Empty segments (trailing or doubled
    // slashes) are harmless.
    for (String segment : normalized.split("/", -1)) {
      ValidationUtils.checkArgument(!".".equals(segment) && !"..".equals(segment),
          "--validation-output-dir must name directories, not relative references; got "
              + outputDir);
      ValidationUtils.checkArgument(!HoodieTableMetaClient.METAFOLDER_NAME.equals(segment),
          "--validation-output-dir must not live under " + HoodieTableMetaClient.METAFOLDER_NAME
              + "/, which is the timeline's; got " + outputDir);
    }
  }

  /**
   * Rejects an output root inside a table's timeline, checked per segment. Otherwise unrestricted
   * (empty means the base path), but without this a root of {@code &lt;table&gt;/.hoodie} would
   * sidestep the {@code .hoodie} guard of {@link #validateOutputDir}.
   */
  @VisibleForTesting
  static void validateOutputRoot(String outputRoot) {
    if (StringUtils.isNullOrEmpty(outputRoot)) {
      return;
    }
    for (String segment : outputRoot.replace('\\', '/').split("/", -1)) {
      ValidationUtils.checkArgument(!HoodieTableMetaClient.METAFOLDER_NAME.equals(segment),
          "--validation-output-root must not be inside " + HoodieTableMetaClient.METAFOLDER_NAME
              + "/, which is a table's timeline; got " + outputRoot);
    }
  }

  /**
   * Rejects a run id that is not a single directory: {@code new StoragePath(parent, child)}
   * normalises {@code ..}, so an unchecked id would put the run's reports somewhere else entirely.
   */
  @VisibleForTesting
  static void validateRunId(String runId) {
    if (StringUtils.isNullOrEmpty(runId)) {
      return;
    }
    String normalized = runId.replace('\\', '/');
    ValidationUtils.checkArgument(normalized.indexOf('/') < 0,
        "--validation-run-id must be a single path segment; got " + runId);
    ValidationUtils.checkArgument(!".".equals(normalized) && !"..".equals(normalized),
        "--validation-run-id must name a directory, not a relative reference; got " + runId);
    ValidationUtils.checkArgument(normalized.indexOf(':') < 0,
        "--validation-run-id must not contain a colon, which can be read as a URI scheme; got "
            + runId);
  }

  /**
   * Rejects a configuration this validator cannot honour, rather than silently checking less than
   * asked. Validations v2 does not implement are refused when requested; record index flags default
   * to {@code true}, so they cannot signal an explicit request and only warn. A run with both
   * dimensions off is refused because it would report success without looking.
   */
  private void validateConfig() {
    if (cfg.continuous) {
      throw new UnsupportedOperationException(
          "The v2 validator supports single-shot runs only; drop --continuous or use the v1 validator");
    }
    ValidationUtils.checkArgument(cfg.validateFsMdtDiff || cfg.validateDuplicateBaseFiles,
        "--validate-fs-mdt-diff and --validate-duplicate-base-files are both off, so this run "
            + "would validate nothing and report success; turn at least one of them on");
    parseServerViewStorageType(cfg.fsViewStorageType);
    ValidationMode validationMode = parseValidationMode(cfg.validationMode);
    validateOutputRoot(cfg.validationOutputRoot);
    validateOutputDir(cfg.validationOutputDir);
    validateRunId(cfg.validationRunId);
    if (validationMode == ValidationMode.EXHAUSTIVE
        && StringUtils.isNullOrEmpty(cfg.validationOutputRoot)
        && !cfg.validationOutputDir.startsWith(HoodieMetadataValidationUtils.TOOLING_DIR_PREFIX)) {
      // Exhaustive only, since fail-fast writes nothing. A convention note, not a hazard: nothing
      // mistakes these reports for table data (see validateOutputDir), but an unrecognised report
      // directory under a table is usually a typo.
      LOG.warn("--validation-output-dir '{}' is written under the table and is outside the '{}' "
              + "namespace this tool's own reports use. Nothing will mistake them for table data -- "
              + "partition discovery keys on {}, and the storage-side pass inside a partition "
              + "considers only base and log files -- so this is a convention note rather than a "
              + "hazard: an unrecognised report directory under a table is usually a typo. Point "
              + "--validation-output-root away from the table, or keep the directory inside that "
              + "namespace", cfg.validationOutputDir,
          HoodieMetadataValidationUtils.TOOLING_DIR_PREFIX,
          HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
    }
    List<String> requested = new ArrayList<>();
    if (cfg.validateAllFileGroups) {
      requested.add("--validate-all-file-groups");
    }
    if (cfg.validateAllColumnStats) {
      requested.add("--validate-all-column-stats");
    }
    if (cfg.validateBloomFilters) {
      requested.add("--validate-bloom-filters");
    }
    if (cfg.validateSecondaryIndex) {
      requested.add("--validate-secondary-index");
    }
    if (cfg.validatePartitionStats) {
      requested.add("--validate-partition-stats");
    }
    if (cfg.validateLatestBaseFiles) {
      requested.add("--validate-latest-base-files");
    }
    if (cfg.validateLastNFileSlices != null) {
      requested.add("--validate-last-n-file-slices");
    }
    if (!requested.isEmpty()) {
      throw new UnsupportedOperationException(
          "The v2 validator validates latest file slices only, but this run also requested "
              + String.join(", ", requested) + "; drop those options or use the v1 validator "
              + "(omit --use-validator-v2) so they are actually performed");
    }
    if (cfg.validateRecordIndexCount || cfg.validateRecordIndexContent) {
      LOG.warn("Record index validation is enabled (it defaults to on) but the v2 validator does "
          + "not implement it; this run validates latest file slices only. Use the v1 validator "
          + "for record index validation.");
    }
  }
}
