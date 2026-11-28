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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.BranchBeacon;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.DuplicateBaseFileDimension;
import org.apache.hudi.utilities.util.FileDiffReportWriter;
import org.apache.hudi.utilities.util.MismatchReportWriter;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.archive.ArchiveFile;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The one harness the validator's functional suites share: write a real table with Spark, damage it
 * on storage, run the whole {@link HoodieMetadataTableValidator} job, and read the reports back off
 * storage exactly as a consumer would.
 *
 * <p>Extracted from {@code TestValidatorV2ExhaustiveMode} rather than copied, so a second suite
 * cannot drift away from the first about what "a table" or "a report" means — the fixtures are the
 * only thing these tests have that a Mockito suite does not, and two of them would be worth less
 * than one.
 *
 * <p>Every table here is written through the Spark datasource at an explicit
 * {@link HoodieWriteConfig#WRITE_TABLE_VERSION}, because the layouts differ in ways this validator
 * depends on: where the archive lives and whether it exists before the first archival, and what
 * instant a log file's name carries.
 */
abstract class ValidatorV2FunctionalTestBase extends HoodieSparkClientTestBase {

  /**
   * The vendor-neutral default the reports are written under. Read off the config rather than
   * restated, so a change to the default cannot leave these assertions passing against a path
   * nothing writes to any more.
   */
  protected static final String OUTPUT_DIR =
      HoodieMetadataTableValidator.Config.DEFAULT_VALIDATION_OUTPUT_DIR;

  /** What a writer that says nothing about table versions produces today. */
  protected static final int CURRENT_TABLE_VERSION = HoodieTableVersion.current().versionCode();

  /**
   * The table version the branch-coverage suites write.
   *
   * <p>Fixed at 6 on purpose. The two layouts differ in exactly the places these suites depend on --
   * whether {@code .hoodie/archived} exists before the first archival, and what instant a log file's
   * name carries -- and version 6 is the one where an un-archived table's absence of archived history
   * is <em>trustworthy</em> (that trust requires a pre-version-8 layout), which is
   * the premise the whole dangling-file family of rows rests on. A suite parameterised over both
   * versions would therefore be asserting two different decision paths under one name.
   */
  protected static final int V6 = 6;

  /** The prefix {@code DuplicateBaseFileDimension.logSuspects} writes one line per suspect under. */
  private static final String SUSPECT_LINE_PREFIX = "Duplicate base file in table";
  /** How many times {@link #resetForFreshTable} has run in this test, to keep the folders apart. */
  private int freshTables = 0;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  // ---------------------------------------------------------------- running the job

  /** Collected rather than thrown, so a failing run still reaches the driver's write. */
  protected boolean runValidator(ValidationMode mode) {
    return runValidator(mode, true);
  }

  /**
   * @param ignoreFailed {@code false} is the product default, on which a fail-fast partition throws
   *                     from the executor and the driver is never reached
   */
  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed) {
    return runValidator(mode, ignoreFailed, "");
  }

  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed, String outputRoot) {
    return runValidator(mode, ignoreFailed, outputRoot, "");
  }

  /**
   * @param validateDuplicateBaseFiles {@code true} is the product default; {@code false} is the
   *                                   off-switch, which skips the extra per-partition listing
   */
  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed,
                                 boolean validateDuplicateBaseFiles) {
    return runValidator(mode, ignoreFailed, "", "", validateDuplicateBaseFiles);
  }

  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed, String outputRoot,
                                  String runId) {
    return runValidator(mode, ignoreFailed, outputRoot, runId, true);
  }

  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed, String outputRoot,
                                  String runId, boolean validateDuplicateBaseFiles) {
    return runValidator(mode, ignoreFailed, outputRoot, runId, validateDuplicateBaseFiles, true);
  }

  /**
   * @param validateFsMdtDiff {@code false} runs the duplicate-base-file dimension alone. The two
   *                          dimensions are independent, so either one is a complete run -- which is
   *                          what lets a suite assert about one of them without the other's findings
   *                          in the same documents.
   *
   *                          <p><b>This is v2's own flag, and not {@code validateLatestFileSlices}.</b>
   *                          {@code validateLatestFileSlices} is set to {@code true} below
   *                          unconditionally, exactly as it always was, and a run with
   *                          {@code validateFsMdtDiff = false} is therefore a run in which v1's
   *                          switch says "compare the slices" and v2's says "do not". That
   *                          combination is deliberate: it is what pins down that v2 reads the flag
   *                          below rather than v1's, which defaults the other way and is consulted
   *                          by every existing caller.
   */
  protected boolean runValidator(ValidationMode mode, boolean ignoreFailed, String outputRoot,
                                  String runId, boolean validateDuplicateBaseFiles,
                                  boolean validateFsMdtDiff) {
    return new HoodieMetadataTableValidator(jsc, validatorConfig(mode, ignoreFailed, outputRoot,
        runId, validateDuplicateBaseFiles, validateFsMdtDiff)).run();
  }

  /**
   * The config every run in these suites is built from, in one place.
   *
   * <p>Extracted so {@link #observeRun} and {@link #runValidator} cannot come to disagree about what
   * "a run" means -- the view budget below in particular, which is what keeps the driver-hosted view
   * spilling instead of growing inside a shared surefire fork.
   */
  protected HoodieMetadataTableValidator.Config validatorConfig(
      ValidationMode mode, boolean ignoreFailed, String outputRoot, String runId,
      boolean validateDuplicateBaseFiles, boolean validateFsMdtDiff) {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = ignoreFailed;
    config.validationMode = mode.getFlagValue();
    config.validationOutputRoot = outputRoot;
    config.validationRunId = runId;
    config.validateDuplicateBaseFiles = validateDuplicateBaseFiles;
    config.validateFsMdtDiff = validateFsMdtDiff;
    // tests share one large surefire fork; keep the view budget small so it spills instead
    config.fsViewMemoryFraction = 0.05;
    return config;
  }

  /**
   * Runs {@code body} with the validator's own log events captured. Spark runs locally here, so an
   * executor-side log line lands in this JVM exactly as a driver-side one does — which is what lets
   * the fail-fast suspect log be asserted on at all.
   *
   * <p><b>Both loggers, and that is not belt-and-braces.</b> The driver's lines come from
   * {@link HoodieMetadataTableValidatorV2}, and the per-suspect lines a fail-fast run leaves behind
   * — its only evidence, since it writes no report — come from {@link DuplicateBaseFileDimension},
   * which is where the suspect log has lived since the two dimensions were split apart. Capturing
   * the validator's logger alone silently returns none of them, and the assertions that read them
   * are exactly the ones nothing else covers.
   */
  protected List<String> runCapturingValidatorLogs(Runnable body) {
    List<String> captured = new ArrayList<>();
    runCapturingValidatorLogs(body, captured);
    return captured;
  }

  /**
   * The sink overload, for a {@code body} that also has to hand something back — the run's verdict,
   * typically, which cannot be returned through a {@link Runnable}.
   */
  protected void runCapturingValidatorLogs(Runnable body, List<String> sink) {
    TestLogAppender appender = new TestLogAppender();
    List<Logger> loggers = Arrays.asList(
        (Logger) LogManager.getLogger(HoodieMetadataTableValidatorV2.class),
        (Logger) LogManager.getLogger(DuplicateBaseFileDimension.class),
        (Logger) LogManager.getLogger(BranchBeacon.class));
    loggers.forEach(logger -> logger.addAppender(appender));
    // Every captured logger has its level forced, not just the beacon, and none of it is inherited
    // from the surefire log4j config. That config is shared with every other module, and whether it
    // names org.apache.hudi at all decides whether these appenders see anything: under
    // log4j2-surefire.properties it does (at DEBUG) and all three loggers are captured, while under
    // log4j2-surefire-quiet.properties -- which CI selects with -Pwarn-log -- it does not, and the
    // capture silently returns beacon lines only. Every assertion reading a marker out of the log
    // then fails on CI and passes locally. Forcing the level here is what makes the capture depend
    // on this method rather than on a file no one editing these suites is looking at.
    List<Level> levelsBefore = new ArrayList<>(loggers.size());
    loggers.forEach(logger -> {
      levelsBefore.add(logger.getLevel());
      logger.setLevel(Level.DEBUG);
    });
    try {
      body.run();
    } finally {
      for (int i = 0; i < loggers.size(); i++) {
        loggers.get(i).setLevel(levelsBefore.get(i));
      }
      loggers.forEach(logger -> logger.removeAppender(appender));
    }
    sink.addAll(appender.messages());
  }

  // ------------------------------------------- observing WHICH branch a run took

  /**
   * One run of the validator, with everything a branch-coverage assertion needs about it.
   *
   * <p><b>Why a holder rather than three helpers.</b> A branch assertion is only worth anything
   * alongside the run's verdict: {@code DANGLING} reached from the wrong row, or the right row on a
   * run that passed when it should have failed, are both green under an assertion that looks at one
   * of the two. Bundling them makes the three-part claim -- this row fired, this verdict came out,
   * the run ended this way -- the unit of a test, so a case cannot accidentally assert only the
   * easy part.
   *
   * <p>A run at the product default ({@code ignoreFailed = false}) raises rather than returning, so
   * the throwable is captured too and is part of what a case may assert on.
   */
  protected static final class ObservedRun {

    private final boolean passed;
    private final Option<Throwable> thrown;
    private final List<String> logged;
    private final Set<Branch> branches;

    private ObservedRun(boolean passed, Option<Throwable> thrown, List<String> logged,
                        Set<Branch> branches) {
      this.passed = passed;
      this.thrown = thrown;
      this.logged = logged;
      this.branches = branches;
    }

    /** Whether the run reported success. Always false when {@link #thrown} is present. */
    public boolean passed() {
      return passed;
    }

    /** What the run raised, if it raised anything. */
    public Option<Throwable> thrown() {
      return thrown;
    }

    /** Every log line the validator and the beacon emitted during the run. */
    public List<String> log() {
      return Collections.unmodifiableList(logged);
    }

    /** Every adjudication row the run went through, as named by {@link BranchBeacon}. */
    public Set<Branch> branches() {
      return Collections.unmodifiableSet(branches);
    }

    /**
     * Asserts the run went through each named row.
     *
     * <p>The failure message lists what the run <em>did</em> take, because the interesting failure
     * is not "the row did not fire" but "a different row fired instead" -- which is what a fixture
     * that stopped building the shape it was named for looks like.
     */
    public ObservedRun tookBranch(Branch... expected) {
      for (Branch branch : expected) {
        if (!branches.contains(branch)) {
          fail("the run did not go through " + branch + "; it went through " + branches
              + thrown.map(throwable -> "; the run threw " + causeChain(throwable)).orElse(""));
        }
      }
      return this;
    }

    /** The throwable and every cause under it, so a wrapped failure still names its origin. */
    private static String causeChain(Throwable thrown) {
      Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
      seen.add(thrown);
      StringBuilder chain = new StringBuilder(thrown.toString());
      for (Throwable cause = thrown.getCause(); cause != null && seen.add(cause); cause = cause.getCause()) {
        chain.append(", caused by ").append(cause);
      }
      return chain.toString();
    }

    /**
     * Asserts the run went through no row whose id starts with {@code prefix}.
     *
     * <p>How an <b>absence</b> claim is expressed. A test whose point is that a run produced no
     * finding cannot name the rows that did not fire — there are dozens — and cannot assert the branch
     * set is empty either, because the two dimensions' healthy-path rows fire on any table at all
     * (a slice holding one base file is a decision, and it is beaconed). Naming the prefix of the
     * decision family under test is the honest middle: {@code "FDA_"} and {@code "SWT_"} are the
     * FS-MDT comparison's verdict rows, {@code "DUP_"} and {@code "DAJ_"} the duplicate dimension's.
     *
     * <p>This exists because a review pointed out that absence claims are invisible to the coverage
     * harvest by construction — no record means no beacon — so they need an assertion of their own
     * rather than being left to "the output directory is missing".
     */
    public ObservedRun tookNoBranchNamed(String prefix) {
      List<Branch> offending = new ArrayList<>();
      for (Branch branch : branches) {
        if (branch.name().startsWith(prefix)) {
          offending.add(branch);
        }
      }
      if (!offending.isEmpty()) {
        fail("the run was expected to take no " + prefix + "* row, and took " + offending
            + "; the full set was " + branches);
      }
      return this;
    }

    /**
     * Asserts the run went through none of the named rows.
     *
     * <p>The negative half matters as much as the positive one for the rows that license opposite
     * actions: a file may not be reported {@code DANGLING} (delete it) by one row while another row
     * calls it {@code MDT_CORRUPTION} (rebuild the metadata table).
     */
    public ObservedRun didNotTakeBranch(Branch... forbidden) {
      for (Branch branch : forbidden) {
        if (branches.contains(branch)) {
          fail("the run went through " + branch + ", which this case forbids; it took " + branches);
        }
      }
      return this;
    }
  }

  /**
   * Runs the validator and reports which adjudication rows it went through.
   *
   * <p>Equivalent to {@link #runValidator(ValidationMode, boolean)} plus the beacon capture; use it
   * wherever a case is about a branch rather than about a report.
   */
  protected ObservedRun observeRun(ValidationMode mode, boolean ignoreFailed) {
    return observeRun(mode, ignoreFailed, null);
  }

  /**
   * Runs the validator with {@code observer} installed at its three phase seams, and reports which
   * adjudication rows it went through.
   *
   * <p>The observer is how a {@code (prev, aft)} state pair becomes reachable at all: the pair is a
   * fact about what happened to the table <em>between</em> the run's two timeline reads, so most
   * cells cannot be produced by any arrangement made before the run starts. See
   * {@link HoodieMetadataTableValidatorV2.PhaseObserver}.
   *
   * @param observer may be null, for a run with no interleaved activity
   */
  protected ObservedRun observeRun(ValidationMode mode, boolean ignoreFailed,
                                   HoodieMetadataTableValidatorV2.PhaseObserver observer) {
    return observeRun(mode, ignoreFailed, true, true, observer);
  }

  /**
   * The full form: which dimensions run, and what happens between the phases.
   *
   * <p>Isolating a dimension is what makes a branch assertion attributable. A duplicate base file in
   * a latest slice is visible to both dimensions, so a run with both enabled cannot say which one
   * failed it — the assertion would keep passing with the dimension under test deleted outright.
   *
   * @param validateDuplicateBaseFiles dimension A's switch
   * @param validateFsMdtDiff          dimension B's switch; the two may not both be off, which
   *                                   {@code validateConfig} rejects before the run starts
   * @param observer                   may be null, for a run with no interleaved activity
   */
  protected ObservedRun observeRun(ValidationMode mode, boolean ignoreFailed,
                                   boolean validateDuplicateBaseFiles, boolean validateFsMdtDiff,
                                   HoodieMetadataTableValidatorV2.PhaseObserver observer) {
    HoodieMetadataTableValidator.Config config = validatorConfig(mode, ignoreFailed, "", "",
        validateDuplicateBaseFiles, validateFsMdtDiff);
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    if (observer != null) {
      validator.setV2PhaseObserver(observer);
    }
    List<String> logged = new ArrayList<>();
    boolean[] passed = {false};
    Throwable[] raised = {null};
    runCapturingValidatorLogs(() -> {
      try {
        passed[0] = validator.run();
      } catch (Throwable t) {
        raised[0] = t;
      }
    }, logged);
    return new ObservedRun(raised[0] == null && passed[0],
        raised[0] == null ? Option.empty() : Option.of(raised[0]), logged, branchesIn(logged));
  }

  /**
   * The adjudication rows a captured log names.
   *
   * <p>Parses the {@link BranchBeacon#PREFIX} contract rather than matching free text, and refuses
   * an id the enum does not know: a beacon whose id no longer parses would otherwise silently
   * reduce every {@code tookBranch} assertion to a no-op, which is the one failure mode a coverage
   * mechanism must not have.
   */
  protected static Set<Branch> branchesIn(List<String> logged) {
    Set<Branch> branches = EnumSet.noneOf(Branch.class);
    for (String line : logged) {
      int at = line.indexOf(BranchBeacon.PREFIX);
      if (at < 0) {
        continue;
      }
      String id = line.substring(at + BranchBeacon.PREFIX.length()).trim();
      try {
        branches.add(Branch.valueOf(id));
      } catch (IllegalArgumentException e) {
        fail("the log carries a branch beacon the Branch enum does not name: '" + id + "'. Either a "
            + "constant was removed without removing its BranchBeacon.hit call, or the beacon line "
            + "format changed and every branch assertion is now vacuous. Line: " + line);
      }
    }
    return branches;
  }

  /**
   * The fail-fast suspect lines out of a captured log, filtered on the prefix
   * {@code DuplicateBaseFileDimension.logSuspects} writes.
   *
   * <p>Here rather than once per suite. Four copies of this filter meant that a change to that
   * prefix would leave three of them collecting nothing at all while every
   * {@code assertEquals(emptyList(), suspectLinesIn(logged))} they feed went on passing — the
   * filter's own failure mode is indistinguishable from the condition those assertions exist to
   * check.
   */
  protected static List<String> suspectLinesIn(List<String> logged) {
    List<String> suspectLines = new ArrayList<>();
    for (String line : logged) {
      if (line.startsWith(SUSPECT_LINE_PREFIX)) {
        suspectLines.add(line);
      }
    }
    return suspectLines;
  }

  /**
   * Whether any of {@code suspectLines} names {@code fileName}.
   *
   * <p><b>The trailing space is load-bearing.</b> The line reads {@code ... file=<name> reason=...},
   * so a bare {@code contains("file=" + fileName)} is also satisfied by a line naming a longer file
   * whose name this one is a prefix of — and base file names in one file group differ only by a
   * suffix, which is exactly the population these assertions are asked to tell apart.
   */
  protected static boolean namesAFile(List<String> suspectLines, String fileName) {
    for (String line : suspectLines) {
      if (line.contains("file=" + fileName + " ")) {
        return true;
      }
    }
    return false;
  }

  /** Collects the validator's log events, the way the unit suites capture a logger's output. */
  private static final class TestLogAppender extends AbstractAppender {

    private final List<String> messages = new ArrayList<>();

    private TestLogAppender() {
      super(UUID.randomUUID().toString(), null, null, false, null);
      start();
    }

    @Override
    public synchronized void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    private synchronized List<String> messages() {
      return new ArrayList<>(messages);
    }
  }

  // ---------------------------------------------------------------- writing tables

  protected Map<String, String> writeOptions(String tableType) {
    return writeOptions(tableType, CURRENT_TABLE_VERSION);
  }

  protected Map<String, String> writeOptions(String tableType, int tableVersion) {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "exhaustive_mode_table");
    writeOptions.put("hoodie.table.name", "exhaustive_mode_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    writeOptions.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion));
    return writeOptions;
  }

  /**
   * Resets the fixture for a case whose second half needs a table of its own: a fresh base path under
   * the same temporary directory, and a fresh key space for the records written to it.
   *
   * <p>Recreating the table at the path the first one used is not equivalent. The metadata table's
   * bootstrap files carry fixed names, and the JVM-wide HFile block cache keys blocks by path, offset
   * and size, so reads of the new table can be served the old table's listings when the new blocks
   * happen to land at the same offsets with the same sizes. Keeping the first table's keys is not
   * equivalent either: an
   * update generated for a key the new table never held lands as an insert, so a round of updates can
   * supersede nothing, and the cleans and metadata deltacommits {@link #archiveTimeline} relies on
   * stop coming.
   */
  protected void resetForFreshTable() {
    initPath("dataset-" + (++freshTables));
    initTestDataGenerator();
  }

  protected void writeTable(String tableType) throws IOException {
    writeTable(tableType, CURRENT_TABLE_VERSION);
  }

  protected void writeTable(String tableType, int tableVersion) throws IOException {
    Map<String, String> writeOptions = writeOptions(tableType, tableVersion);
    makeInsertDf("000", 5).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);
    if ("MERGE_ON_READ".equals(tableType)) {
      // Upserts on a MOR table are what put log files in the latest slices.
      upsert(writeOptions);
    }
  }

  /**
   * A COW table whose file groups have two slices, so that the older slice's base file is one the
   * latest-file-slice comparison never looks at. Cleaning has not run, so it is still on storage.
   */
  protected void writeCowTableWithASupersededSlice() throws IOException {
    writeCowTableWithASupersededSlice(CURRENT_TABLE_VERSION);
  }

  protected void writeCowTableWithASupersededSlice(int tableVersion) throws IOException {
    writeTable("COPY_ON_WRITE", tableVersion);
    // On COW an upsert rewrites the file group into a NEW base file under a new instant and leaves
    // the old one behind, which is exactly the superseded slice these tests need.
    upsert(writeOptions("COPY_ON_WRITE", tableVersion));
  }

  /**
   * Writes {@code n} <b>new</b> records, leaving every existing file group untouched.
   *
   * <p>The counterpart of {@link #upsert}, and the difference matters wherever a fixture needs the
   * timeline to move on without the table's existing slices moving with it — archival, above all.
   *
   * @param instantSeed distinguishes one round's generated keys from another's
   */
  protected void insertMore(Map<String, String> writeOptions, String instantSeed, int n)
      throws IOException {
    makeInsertDf(instantSeed, n).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append)
        .save(basePath);
  }

  protected void upsert(Map<String, String> writeOptions) throws IOException {
    makeUpdateDf("001", 5).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append)
        .save(basePath);
  }

  /**
   * Runs a real Hudi clean, inline, so the timeline carries a completed clean instant whose metadata
   * names the files it deleted and storage really lost them.
   *
   * <p>A hand-planted plan ({@link #plantPendingClean}) proves that {@code PendingServiceState} is
   * wired in. This is the other half: the writer's own cleaner, its own plan encoding, and its own
   * clean metadata.
   *
   * <p><b>Both halves of the fixture are asserted here rather than by the caller</b> — a completed
   * clean instant on the timeline, and at least one data file that existed before and does not exist
   * after. A cleaner that found nothing to do leaves a table indistinguishable from one that was
   * never cleaned, and every assertion a caller then makes about "what a clean did" would hold
   * against a table nothing happened to.
   *
   * @param commitsRetained how many commits the cleaner keeps; 1 makes it delete on a two-commit
   *                        table
   */
  protected void runClean(Map<String, String> writeOptions, int commitsRetained) throws IOException {
    List<String> before = dataFileNames();
    Map<String, String> cleaning = new HashMap<>(writeOptions);
    cleaning.put("hoodie.clean.automatic", "true");
    cleaning.put("hoodie.clean.async", "false");
    cleaning.put("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS");
    cleaning.put("hoodie.cleaner.commits.retained", String.valueOf(commitsRetained));
    upsert(cleaning);

    // Through the cleaner timeline rather than by globbing for "*.clean": the instant file's name
    // and directory are both table-version dependent, and this is the reader the product uses.
    assertTrue(metaClient().getActiveTimeline().getCleanerTimeline().filterCompletedInstants()
            .countInstants() > 0,
        "precondition: the cleaner has to have actually completed, or nothing this fixture is "
            + "named for has happened");
    List<String> after = dataFileNames();
    List<String> deleted = new ArrayList<>(before);
    deleted.removeAll(after);
    assertFalse(deleted.isEmpty(),
        "precondition: the clean has to have actually deleted a data file. It kept all " + before
            + " and a caller asserting what a clean explains would be asserting it about nothing");
  }

  /** Every non-metadata data file under the table, by path, for a before/after comparison. */
  private List<String> dataFileNames() throws IOException {
    List<String> names = new ArrayList<>();
    for (StoragePathInfo info : storage.listFiles(new StoragePath(basePath))) {
      if (!info.getPath().toString().contains("/.hoodie/")) {
        names.add(info.getPath().toString());
      }
    }
    return names;
  }

  /**
   * Removes one file from a completed commit's write stats <b>for {@code partition}</b>, leaving a
   * table whose metadata table names a file neither storage nor that partition's stats have. This is
   * MDT corruption manufactured rather than waited for: the shape arises in production when the
   * metadata table and the timeline drift, and no amount of ordinary writing produces it.
   *
   * <p><b>Scoped to the one partition, and only claimed for it.</b> Both the removal loop and the
   * read-back below look at {@code getPartitionToWriteStats().get(partition)} and nowhere else, so a
   * stray stat naming the same file under some other partition key would survive and go unreported.
   * The two halves agree, which is what a caller needs; what this does not do is promise that the
   * commit as a whole stops naming the file.
   *
   * <p>Read and written back through the meta client's own {@code CommitMetadataSerDe}, exactly as
   * {@code TestValidatorV2Reconciliation.nameFileInCommit} does the inverse — <b>not</b> through a
   * JSON object mapper. Version 6 writes commit metadata as JSON and version 8 and above as Avro, so
   * a fixture that picked one encoding would produce an unreadable commit on the other table and the
   * run would refuse instead of reaching the verdict the caller meant to assert.
   *
   * <p>Fails loudly when that partition's stats do not name the file: a silent no-op would leave the
   * caller's damage half-applied and its test asserting against a shape it did not build.
   */
  protected void dropFileFromCommitMetadata(String instantTime, String partition, String fileName)
      throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant commit = client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no completed commit at " + instantTime + " on " + basePath));
    HoodieCommitMetadata metadata = client.getActiveTimeline().readCommitMetadata(commit);
    List<HoodieWriteStat> stats = metadata.getPartitionToWriteStats().get(partition);
    boolean removed = false;
    if (stats != null) {
      Iterator<HoodieWriteStat> iterator = stats.iterator();
      while (iterator.hasNext()) {
        String path = iterator.next().getPath();
        if (path != null && path.endsWith(fileName)) {
          iterator.remove();
          removed = true;
        }
      }
    }
    if (!removed) {
      throw new IllegalStateException("commit " + instantTime + " of " + basePath + " names no "
          + fileName + " in partition [" + partition + "]; it holds "
          + metadata.getPartitionToWriteStats());
    }
    StoragePath instantFile = new StoragePath(client.getTimelinePath(),
        client.getInstantFileNameGenerator().getFileName(commit));
    try (OutputStream out = storage.create(instantFile, true)) {
      client.getCommitMetadataSerDe().getInstantWriter(metadata).get().writeToStream(out);
    }

    // Read back through a fresh meta client, because the edit is only worth anything if the commit
    // is still READABLE afterwards. A serde mismatch would leave an unparseable instant file, and
    // every caller would then be asserting against a run that refused to read the commit rather
    // than against one that read it and found the file missing -- two very different tests, one of
    // which passes for the wrong reason.
    List<HoodieWriteStat> after = metaClient().getActiveTimeline().readCommitMetadata(commit)
        .getPartitionToWriteStats().get(partition);
    if (after != null) {
      for (HoodieWriteStat stat : after) {
        assertTrue(stat.getPath() == null || !stat.getPath().endsWith(fileName),
            "the commit still names " + fileName + " after the edit: " + stat.getPath());
      }
    }
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) throws IOException {
    List<String> records = dataGen.generateUpdates(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  // ---------------------------------------------------------------- finding files to damage

  /**
   * Every base file under the table, by file group, each group ordered oldest slice first.
   * Instants are fixed-width timestamps, so lexicographic order is chronological order.
   */
  protected Map<String, List<StoragePath>> baseFilesByFileGroup() throws IOException {
    Map<String, List<StoragePath>> byFileGroup = new LinkedHashMap<>();
    for (StoragePathInfo info : storage.listFiles(new StoragePath(basePath))) {
      String name = info.getPath().getName();
      if (info.getPath().toString().contains("/.hoodie/") || !name.endsWith(".parquet")
          || FSUtils.isLogFile(name)) {
        continue;
      }
      byFileGroup.computeIfAbsent(name.split("_")[0], ignored -> new ArrayList<>())
          .add(info.getPath());
    }
    for (List<StoragePath> fileGroup : byFileGroup.values()) {
      fileGroup.sort(Comparator.comparing(path -> path.getName().split("_")[2]));
    }
    return byFileGroup;
  }

  /**
   * A base file belonging to a slice that is <b>not</b> the latest of its file group — so the
   * latest-file-slice comparison, which is all this validator performs, never looks at it.
   */
  protected StoragePath anySupersededBaseFile() throws IOException {
    for (List<StoragePath> fileGroup : baseFilesByFileGroup().values()) {
      if (fileGroup.size() > 1) {
        return fileGroup.get(0);
      }
    }
    throw new IllegalStateException("no file group with more than one slice under " + basePath);
  }

  /**
   * The current base file of some file group other than {@code fileId}'s — a file the comparison
   * <em>does</em> look at, in a file group the caller has not already interfered with.
   */
  /**
   * A base file in some file group other than {@code fileId}, in a group holding <b>exactly one</b>
   * base file — so deleting it removes the whole group from the storage listing.
   *
   * <p>The single-slice requirement is what makes the caller's expected pattern deterministic, and it
   * is not cosmetic. {@code writeCowTableWithASupersededSlice} upserts through
   * {@code dataGen.generateUpdates}, which picks its records <b>at random</b>, so only some groups
   * come back with a second base file and which ones varies per run. Returning the first other group
   * regardless of its slice count would make the finding
   * {@code MDT_EXCLUSIVE_FILE_GROUP} when that group happened to get no update and
   * {@code MDT_EXCLUSIVE_FILE_SLICE} when it did. That flakiness reached CI as a red
   * {@code exhaustiveAtTheProductDefaultStillWritesItsReportsBeforeFailing} that passed on every
   * local run.
   *
   * <p>Throws rather than falling back to a multi-slice group: a fallback would restore exactly the
   * ambiguity this exists to remove, and silently.
   */
  protected StoragePath latestBaseFileOutsideFileGroup(String fileId) throws IOException {
    Map<String, List<StoragePath>> byFileGroup = baseFilesByFileGroup();
    for (Map.Entry<String, List<StoragePath>> fileGroup : byFileGroup.entrySet()) {
      if (!fileGroup.getKey().equals(fileId) && fileGroup.getValue().size() == 1) {
        return fileGroup.getValue().get(0);
      }
    }
    throw new IllegalStateException("no file group under " + basePath + " other than " + fileId
        + " holds exactly one base file, so deleting one cannot make a whole file group "
        + "MDT-exclusive; layout: " + byFileGroup);
  }

  /**
   * {@code logPath} renamed to log version {@code version}, keeping its file group, delta commit,
   * write token and naming format (inline {@code .<fg>_<instant>.log.<v>_<token>} or native
   * {@code <fg>_<token>_<instant>_<v>.log.<format>}).
   */
  protected static StoragePath withLogVersion(StoragePath logPath, int version) {
    HoodieLogFile logFile = new HoodieLogFile(logPath);
    String name = logFile.isNativeLogFile()
        ? FSUtils.makeNativeLogFileName(logFile.getFileId(), logFile.getLogWriteToken(),
            logFile.getDeltaCommitTime(), version, logFile.getFileExtension(),
            HoodieFileFormat.fromFileExtension("." + logFile.getSuffix()))
        : FSUtils.makeInlineLogFileName(logFile.getFileId(), "." + logFile.getFileExtension(),
            logFile.getDeltaCommitTime(), version, logFile.getLogWriteToken());
    return new StoragePath(logPath.getParent(), name);
  }

  /** The same as {@link #anyFile}, restricted to one partition directory. */
  protected StoragePath anyFileIn(StoragePath partition, String marker, boolean logFile)
      throws IOException {
    for (StoragePathInfo info : storage.listDirectEntries(partition)) {
      String name = info.getPath().getName();
      if (name.contains(marker) && logFile == FSUtils.isLogFile(name)) {
        return info.getPath();
      }
    }
    throw new IllegalStateException("no file containing " + marker + " under " + partition);
  }

  /**
   * @param logFile whether to return a log file; log files are the hidden ones, base files are not
   */
  protected StoragePath anyFile(String marker, boolean logFile) throws IOException {
    List<StoragePathInfo> everything = storage.listFiles(new StoragePath(basePath));
    for (StoragePathInfo info : everything) {
      String name = info.getPath().getName();
      if (info.getPath().toString().contains("/.hoodie/")) {
        continue;
      }
      if (name.contains(marker) && logFile == FSUtils.isLogFile(name)) {
        return info.getPath();
      }
    }
    throw new IllegalStateException("no file containing " + marker + " under " + basePath);
  }

  // ---------------------------------------------------------------- damaging them

  /**
   * A second base file for a slice that already has one: same file id, same instant, a new write
   * token. This is the Spark-retry shape — the commit records only the first of them.
   */
  protected StoragePath duplicateOf(StoragePath original) throws IOException {
    return duplicateOf(original, "9-9-9");
  }

  /**
   * The same, at a caller-chosen write token, so a slice can be given more than one extra copy.
   *
   * <p>The token is what makes two copies of one slice two distinct files, and it is the attempt id
   * a Spark retry varies ({@code FSUtils.makeWriteToken}). A slice holding three base files is not a
   * hypothetical: a task retried twice produces exactly it, and the detector's cardinality rule
   * ("two or more") has to name all three rather than a pair of them.
   *
   * @param writeToken the token to give the copy; must differ from the original's and from any
   *                   other copy's
   */
  protected StoragePath duplicateOf(StoragePath original, String writeToken) throws IOException {
    String[] parts = original.getName().split("_");
    assertFalse(writeToken.equals(parts[1]),
        "the copy's write token has to differ from the original's, or this overwrites it");
    StoragePath duplicate =
        new StoragePath(original.getParent(), parts[0] + "_" + writeToken + "_" + parts[2]);
    assertFalse(storage.exists(duplicate),
        "a copy already exists at " + duplicate + "; pick a write token no other copy uses");
    copyFile(original, duplicate);
    return duplicate;
  }

  /**
   * A file group of its own, holding {@code count} base files under an instant with <b>no file
   * anywhere on the timeline naming it</b> — requested, inflight, completed or archived.
   *
   * <p>This is the case {@link org.apache.hudi.utilities.util.DuplicateInstantWindow} does not
   * sanction: absent from {@code T_prevD} in every state, and never appearing at {@code T_aftD}
   * either, since nothing ever writes it. Two base files sharing one file-slice identity with nothing
   * accounting for either one is not what a retried Spark task's in-flight write looks like — a
   * genuinely in-flight write is present at {@code T_prevD} as requested or inflight, which this
   * fixture deliberately does not build.
   *
   * <p>The instant is minted strictly newer than everything on the timeline and no instant file is
   * written for it, so it is absent rather than pending. Copies are made from {@code seed} so the
   * files are readable parquet.
   *
   * @param seed  an existing base file to copy the bytes of
   * @param count how many copies to plant in the new file group
   * @return the planted files, in the order they were written
   */
  protected List<StoragePath> plantUncommittedBaseFiles(StoragePath seed, int count)
      throws IOException {
    String uncommitted = instantAfterEverything();
    String fileId = UUID.randomUUID().toString();
    List<StoragePath> planted = new ArrayList<>();
    for (int copy = 0; copy < count; copy++) {
      StoragePath path = new StoragePath(seed.getParent(),
          fileId + "_" + copy + "-0-0_" + uncommitted + ".parquet");
      copyFile(seed, path);
      planted.add(path);
    }
    assertFalse(metaClient().getActiveTimeline().filterCompletedInstants()
            .containsOrBeforeTimelineStarts(uncommitted),
        "precondition: the instant these files name must be one the completed timeline cannot "
            + "account for, or they are ordinary committed files and this fixture proves nothing");
    return planted;
  }

  /** Removes a file from storage and nothing else, leaving whatever named it still naming it. */
  protected void deleteFromStorage(StoragePath path) throws IOException {
    assertTrue(storage.exists(path), "nothing to delete at " + path);
    assertTrue(storage.deleteFile(path), "could not delete " + path);
    assertFalse(storage.exists(path), path + " survived its deletion");
  }

  /**
   * Runs real archival until the archived timeline holds something, and asserts that it does.
   *
   * <p>Through the writer's own archiver rather than by placing files, because what these suites ask
   * of the archive is whether the <em>loader</em> can read it back — a hand-built archive would be
   * testing this fixture's guess at the format, on a code path whose two layouts encode commit
   * metadata under different fields entirely.
   *
   * <p>Archival cannot move an instant the cleaner still needs, so cleaning is turned on with the
   * tightest retention alongside it; {@code keep.min}/{@code keep.max} are the archiver's own bounds.
   * Several upserts follow because the archiver only acts once the active timeline exceeds
   * {@code keep.max.commits}.
   *
   * <p>Asserts the outcome rather than the attempt: an archival that moved nothing leaves a table
   * indistinguishable from one that was never archived, and every assertion a caller then makes
   * about "what the archive says" would hold against an empty archive.
   *
   * @param writeOptions the table's write options, so the archive lands in its own layout
   * @return the instant times the archive now holds, newest last
   */
  protected List<String> archiveTimeline(Map<String, String> writeOptions) throws IOException {
    Map<String, String> archiving = new HashMap<>(writeOptions);
    archiving.put("hoodie.archive.automatic", "true");
    archiving.put("hoodie.archive.async", "false");
    archiving.put("hoodie.keep.min.commits", "2");
    archiving.put("hoodie.keep.max.commits", "3");
    archiving.put("hoodie.clean.automatic", "true");
    archiving.put("hoodie.clean.async", "false");
    archiving.put("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS");
    archiving.put("hoodie.cleaner.commits.retained", "1");
    // Small-file handling off, so an insert round writes a NEW file group rather than appending into
    // an existing one.
    archiving.put("hoodie.parquet.small.file.limit", "0");
    // No embedded timeline server for this writer. It needs none — archival and cleaning read the
    // timeline directly — and when this fixture is driven from a PhaseObserver it runs *inside* a
    // validation, which already hosts one. EmbeddedTimelineService keys a static cache of running
    // services on (host, port, marker type, metadata enabled, early conflict detection) and its
    // basePaths is a Set, so a co-tenant holding the same table keeps it at size one: whoever calls
    // stopForBasePath first closes the server for BOTH. The validator's own code comments name this
    // hazard, and it surfaced here as a later test failing its setup with "Failed to create marker
    // file" — a remote call to a server another actor had stopped.
    archiving.put("hoodie.embed.timeline.server", "false");
    // One insert round FIRST, then updates. Both halves are load-bearing; the first alone archives
    // nothing at all:
    //
    //   * the updates are what let the cleaner act. KEEP_LATEST_COMMITS deletes SUPERSEDED file
    //     slices, and an insert supersedes nothing -- so an inserts-only fixture leaves the cleaner
    //     with no work, produces no clean instant, and archival never advances behind it. Measured:
    //     seven completed commits, zero clean instants, no archive directory at all. The archiver
    //     also archives nothing until the metadata table has compacted, and the cleans' own
    //     deltacommits are what carry the metadata table to its compaction threshold;
    //   * the one insert round is what leaves a file group behind. dataGen.generateUpdates picks
    //     among the records that exist when it runs, so a group inserted here and never inserted into
    //     again keeps whatever slice it was given.
    insertMore(archiving, "quiet", 2);
    for (int round = 0; round < 5; round++) {
      upsert(archiving);
    }
    List<String> archived = new ArrayList<>();
    metaClient().getArchivedTimeline().getInstantsAsStream()
        .forEach(instant -> archived.add(instant.requestedTime()));
    assertFalse(archived.isEmpty(),
        "precondition: archival has to have actually moved an instant off the active timeline. The "
            + "archived timeline of " + basePath + " is empty, so every assertion about what the "
            + "archive says would hold against an archive that was never written. The cleaner "
            + "completed " + metaClient().getActiveTimeline().getCleanerTimeline()
            .filterCompletedInstants().countInstants() + " time(s) and "
            + metaClient().getActiveTimeline().filterCompletedInstants().countInstants()
            + " commits are still active -- archival cannot advance ahead of the cleaner, so zero "
            + "cleans is the usual reason for an empty archive");
    return archived;
  }

  /**
   * Whether the archive directory holds anything at all.
   *
   * <p>An empty or absent archive is what makes an archived absence trustworthy: the resolver
   * short-circuits on the directory's shape before it searches, so a case that means to assert what
   * a <em>search</em> concluded has to establish there was something to search.
   */
  protected boolean archiveHasContent() throws IOException {
    StoragePath archivePath = metaClient().getArchivePath();
    return storage.exists(archivePath) && !storage.listDirectEntries(archivePath).isEmpty();
  }

  /**
   * Makes an archive file's first block read back as a {@code HoodieCorruptBlock} while leaving the
   * rest intact, by disagreeing the block size in its header with the one in its footer -- the
   * mismatch {@code HoodieLogFileReader.isBlockCorrupted} looks for.
   *
   * <p>Damages bytes {@link #archiveTimeline} had Hudi's own {@code TimelineArchiverV1} write.
   * {@code TestHoodieArchiveInstantSearch#corruptFirstBlockHeader} damages synthetic bytes the same
   * way, and doing it identically here is the point: what a functional case adds over that one is
   * that a real archiver's output, damaged the same way, still reaches a real resolver as
   * {@code INDETERMINATE} rather than as a proven absence.
   */
  protected static void corruptFirstBlockHeader(ArchiveFile file) throws IOException {
    Path onDisk = Paths.get(file.path().toUri().getPath());
    byte[] bytes = Files.readAllBytes(onDisk);
    int blockSizeOffset = HoodieLogFormat.MAGIC.length;
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putLong(blockSizeOffset, buffer.getLong(blockSizeOffset) + Long.BYTES);
    Files.write(onDisk, bytes);
  }

  protected void copyFile(StoragePath from, StoragePath to) throws IOException {
    byte[] bytes = readAll(from);
    try (OutputStream out = storage.create(to, true)) {
      out.write(bytes);
    }
  }

  protected void appendBytes(StoragePath path, int count) throws IOException {
    byte[] existing = readAll(path);
    try (OutputStream out = storage.create(path, true)) {
      out.write(existing);
      out.write(new byte[count]);
    }
  }

  protected byte[] readAll(StoragePath path) throws IOException {
    byte[] bytes = new byte[(int) storage.getPathInfo(path).getLength()];
    try (InputStream in = storage.open(path)) {
      int read = 0;
      while (read < bytes.length) {
        int justRead = in.read(bytes, read, bytes.length - read);
        if (justRead < 0) {
          break;
        }
        read += justRead;
      }
    }
    return bytes;
  }

  // ---------------------------------------------------------------- planting timeline state

  protected HoodieTableMetaClient metaClient() {
    return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build();
  }

  /**
   * An instant strictly newer than every instant on the table, at the width the writer uses, so the
   * timeline's lexicographic comparison reads it as the newest.
   *
   * <p><b>Date arithmetic, not integer arithmetic.</b> An instant is {@code yyyyMMddHHmmssSSS}, so
   * adding 1000 to it as a number advances a second by carrying into a field that is base 60 — and
   * mints {@code ss=60} whenever the newest instant lands in the last second of a minute. That is
   * not a parseable timestamp;
   * {@code HoodieMetadataTableValidator.metadataTableInstantFor:1490-1499} catches the
   * {@code ParseException} and warns, so today it costs a confusing log line rather than a failure,
   * but it is a one-run-in-sixty hazard across every fixture here that mints an instant.
   *
   * <p>The arithmetic goes through {@link HoodieInstantTimeGenerator#instantTimePlusMillis} rather
   * than a locally built {@code DateTimeFormatter}: a formatter from
   * {@code ofPattern("yyyyMMddHHmmssSSS")} cannot parse its own output on Java 8
   * (<a href="https://bugs.openjdk.java.net/browse/JDK-8031085">JDK-8031085</a> — adjacent value
   * parsing does not extend to the fraction field), which the generator works around at
   * {@code HoodieInstantTimeGenerator:51-53}. Tests here run on Java 8 in CI, so the hand-rolled
   * formatter threw {@code DateTimeParseException} there while passing on a newer local JDK. It
   * also absorbs a second-granularity instant, returning millisecond width either way.
   */
  protected String instantAfterEverything() throws IOException {
    String newest = null;
    for (StoragePathInfo info : storage.listDirectEntries(metaClient().getTimelinePath())) {
      String name = info.getPath().getName();
      int dot = name.indexOf('.');
      if (dot <= 0) {
        continue;
      }
      String stamp = name.substring(0, dot).split("_")[0];
      if (!stamp.matches("\\d{14}|\\d{17}")) {
        continue;
      }
      if (newest == null || stamp.compareTo(newest) > 0) {
        newest = stamp;
      }
    }
    if (newest == null) {
      throw new IllegalStateException("no instant on the timeline of " + basePath);
    }
    return HoodieInstantTimeGenerator.instantTimePlusMillis(newest, 1000);
  }

  /** A rollback of {@code instantToRollback} left requested, planning to delete one named file. */
  protected void plantPendingRollback(String instantToRollback, String partition, String fileName)
      throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieRollbackRequest request = HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId(fileName.split("_")[0])
        .setLatestBaseInstant(instantToRollback)
        .setFilesToBeDeleted(Collections.singletonList(fileName))
        .build();
    HoodieRollbackPlan plan = HoodieRollbackPlan.newBuilder()
        .setInstantToRollback(new HoodieInstantInfo(instantToRollback, HoodieTimeline.COMMIT_ACTION))
        .setRollbackRequests(Collections.singletonList(request))
        .setVersion(1)
        .build();
    client.getActiveTimeline().saveToRollbackRequested(
        client.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED,
            HoodieTimeline.ROLLBACK_ACTION, instantAfterEverything()), plan);
  }

  /**
   * A rollback of {@code instantToRollback} that already completed during the run, having deleted
   * one named file. Unlike {@link #plantPendingRollback}, this is discoverable only through
   * {@link ServiceActivityDelta}'s completed-rollback loop, not {@link PendingServiceState} -- the
   * shape a rollback caught by the run's own {@code T_aft} read, rather than by a plan still pending
   * at either read, actually has.
   */
  protected void plantCompletedRollback(String instantToRollback, String partition, String fileName)
      throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieRollbackRequest request = HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId(fileName.split("_")[0])
        .setLatestBaseInstant(instantToRollback)
        .setFilesToBeDeleted(Collections.singletonList(fileName))
        .build();
    HoodieRollbackPlan plan = HoodieRollbackPlan.newBuilder()
        .setInstantToRollback(new HoodieInstantInfo(instantToRollback, HoodieTimeline.COMMIT_ACTION))
        .setRollbackRequests(Collections.singletonList(request))
        .setVersion(1)
        .build();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED,
        HoodieTimeline.ROLLBACK_ACTION, instantAfterEverything());
    client.getActiveTimeline().saveToRollbackRequested(requested, plan);
    HoodieInstant inflight = client.getActiveTimeline().transitionRollbackRequestedToInflight(requested);

    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.singletonList(fileName))
        .setFailedDeleteFiles(Collections.emptyList())
        .build();
    HoodieRollbackMetadata metadata = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime(inflight.requestedTime())
        .setTimeTakenInMillis(0L)
        .setTotalFilesDeleted(1)
        .setCommitsRollback(Collections.singletonList(instantToRollback))
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(1)
        .build();
    client.getActiveTimeline().transitionRollbackInflightToComplete(false, inflight, metadata);
  }

  /** A clean left requested, planning to delete one named file. */
  protected void plantPendingClean(String partition, String fileName) throws IOException {
    HoodieTableMetaClient client = metaClient();
    Map<String, List<HoodieCleanFileInfo>> perPartition = Collections.singletonMap(partition,
        Collections.singletonList(new HoodieCleanFileInfo(fileName, false)));
    HoodieCleanerPlan plan = HoodieCleanerPlan.newBuilder()
        .setPolicy("KEEP_LATEST_COMMITS")
        .setFilePathsToBeDeletedPerPartition(perPartition)
        .setVersion(2)
        .build();
    client.getActiveTimeline().saveToCleanRequested(
        client.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED,
            HoodieTimeline.CLEAN_ACTION, instantAfterEverything()), Option.of(plan));
  }

  /**
   * The file's partition path relative to the table root, which is the form a clean or rollback
   * plan records and the form {@code FileDiffRecord} carries.
   *
   * <p>Through {@link FSUtils#getRelativePartitionPath} rather than string arithmetic on
   * {@code basePath}: a path that came back from a storage listing carries the {@code file:} scheme
   * and {@code basePath} does not, so a {@code startsWith} test silently fails and the plan is
   * written under a key nothing looks up — which is a plan that explains nothing, reported as a
   * table that is broken.
   */
  protected String relativePartitionOf(StoragePath file) {
    return FSUtils.getRelativePartitionPath(new StoragePath(basePath), file.getParent());
  }

  // ---------------------------------------------------------------- reading the reports back

  /** The one run directory this table's validation left behind. */
  protected StoragePath runDir() throws IOException {
    List<StoragePathInfo> runs =
        storage.listDirectEntries(new StoragePath(basePath, OUTPUT_DIR));
    assertEquals(1, runs.size(), "expected exactly one run directory, got: " + runs);
    return runs.get(0).getPath();
  }

  protected JsonNode readSuspects() throws IOException {
    return readDocument(new StoragePath(runDir(), DanglingReportWriter.SUSPECTS_DIR),
        DanglingReportWriter.SUSPECTS_FILE);
  }

  protected JsonNode readAdjudicated() throws IOException {
    return readDocument(new StoragePath(runDir(), DanglingReportWriter.ADJUDICATED_DIR),
        DanglingReportWriter.ADJUDICATED_FILE);
  }

  /** The FS-MDT dimension's full population, {@code fs_mdt_diff/diff/}. */
  protected JsonNode readDiff() throws IOException {
    return readDocument(diffDir(FileDiffReportWriter.DIFF_DIR), FileDiffReportWriter.DIFF_FILE);
  }

  /** The FS-MDT dimension's conclusion, {@code fs_mdt_diff/adjudicated/}. */
  protected JsonNode readDiffAdjudicated() throws IOException {
    return readDocument(diffDir(FileDiffReportWriter.ADJUDICATED_DIR),
        FileDiffReportWriter.ADJUDICATED_FILE);
  }

  /** One leaf of the FS-MDT dimension's directory; both sit under one {@code fs_mdt_diff/}. */
  protected StoragePath diffDir(String leafDir) throws IOException {
    return new StoragePath(new StoragePath(runDir(), FileDiffReportWriter.REPORT_DIR), leafDir);
  }

  protected JsonNode readDocument(StoragePath dir, String fileName) throws IOException {
    assertTrue(storage.exists(new StoragePath(dir, MismatchReportWriter.COMPLETE_MARKER)),
        "without the marker a consumer cannot tell a finished run from a killed one: " + dir);
    try (InputStream in = storage.open(new StoragePath(dir, fileName))) {
      return JsonUtils.getObjectMapper().readTree(in);
    }
  }

  protected static JsonNode recordFor(JsonNode document, String fileName) {
    JsonNode record = recordOrNull(document, fileName);
    if (record == null) {
      throw new AssertionError("no record for " + fileName + " in " + document);
    }
    return record;
  }

  /**
   * The same lookup, for an assertion that a document does <b>not</b> name a file. Kept apart from
   * {@link #recordFor} rather than folded into it with a nullable return, so a test that meant to
   * find a record still fails with the document in the message.
   */
  protected static JsonNode recordOrNull(JsonNode document, String fileName) {
    for (JsonNode record : document.get("records")) {
      if (fileName.equals(record.get("fileName").asText())) {
        return record;
      }
    }
    return null;
  }

  /** How many records of {@code document} carry {@code verdict}; the FS-MDT dimension's counter. */
  protected static int countOfVerdict(JsonNode document, String verdict) {
    int count = 0;
    for (JsonNode record : document.get("records")) {
      JsonNode found = record.get("verdict");
      if (found != null && verdict.equals(found.asText())) {
        count++;
      }
    }
    return count;
  }
}
