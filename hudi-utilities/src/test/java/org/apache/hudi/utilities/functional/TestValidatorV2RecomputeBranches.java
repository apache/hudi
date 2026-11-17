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

import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.ValidationMode;

import org.apache.spark.rdd.RDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a run does when Spark re-runs its phase-1 listing after the barrier that was supposed to fix
 * it.
 *
 * <p>The condition is forced rather than waited for: in production it needs a lost executor or an
 * evicted block, so a test that hoped for one would be a test that never ran. Unpersisting the
 * cached phase-1 RDD at the barrier seam produces exactly the state a lost block leaves behind — the
 * next action finds no block and recomputes the partition, listing storage a second time, after the
 * timeline the run pins to.
 */
@Tag("functional")
public class TestValidatorV2RecomputeBranches extends ValidatorV2FunctionalTestBase {

  /**
   * Drops every cached block at the seam the dimension reads its post-barrier timeline at, which is
   * the moment the invariant starts to matter.
   */
  private HoodieMetadataTableValidatorV2.PhaseObserver dropsTheCacheBeforeTheDuplicateBarrierRead() {
    return new HoodieMetadataTableValidatorV2.PhaseObserver() {
      @Override
      public void beforeDuplicatePostPhaseOneTimelineRead() {
        unpersistEveryCachedRdd();
      }
    };
  }

  private HoodieMetadataTableValidatorV2.PhaseObserver dropsTheCacheBeforeTheDiffBarrierRead() {
    return new HoodieMetadataTableValidatorV2.PhaseObserver() {
      @Override
      public void beforePostPhaseOneTimelineRead() {
        unpersistEveryCachedRdd();
      }
    };
  }

  /**
   * Blocking, because the next action must find the blocks already gone: an asynchronous release
   * would race the recompute this test depends on.
   */
  private void unpersistEveryCachedRdd() {
    scala.collection.Iterator<RDD<?>> cached =
        jsc.sc().getPersistentRDDs().valuesIterator();
    while (cached.hasNext()) {
      cached.next().unpersist(true);
    }
  }

  /**
   * Exhaustive refuses. The mode exists to produce a findings document a mitigation job acts on, and
   * a document holding one finding listed after the window that is supposed to explain it is worse
   * than no document: downstream, a wrong finding reads as a checked table.
   */
  @Test
  void exhaustiveRefusesWhenTheDuplicateDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");

    // ignoreFailed=false: with it true, HoodieMetadataTableValidatorV2.run() swallows the
    // HoodieValidationException concludeInconclusiveRun raises (logs it, adds it to throwables,
    // returns false) exactly as it does for every other validation failure -- see
    // runTheArchiverOverlappedReachesNoVerdictInEitherMode, which asserts only passed() for that
    // reason. Asserting the exception itself, as this case does, needs it to actually propagate.
    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, false, true, false,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertFalse(run.passed(), "a run that cannot attribute its listings must not report success");
    assertTrue(run.thrown().isPresent(), "exhaustive refuses rather than returning");
    assertTrue(run.thrown().get() instanceof HoodieValidationException,
        String.valueOf(run.thrown().get()));
    assertTrue(run.thrown().get().getMessage().contains("was recomputed after the barrier"),
        run.thrown().get().getMessage());
    assertTrue(run.branches().contains(Branch.DUP_RECOMPUTED_INCONCLUSIVE), run.branches().toString());
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_EXHAUSTIVE_FAILED),
        run.branches().toString());
  }

  /**
   * Fail-fast sanctions. A lost cache block is a fact about the cluster, not a finding about the
   * table, and failing every run that loses one would report a healthy table as broken on the
   * strength of an executor going away.
   */
  @Test
  void failFastSanctionsWhenTheDuplicateDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false, true, false,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertTrue(run.passed(), "a sanctioned run reports success and leaves the marker");
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
    assertTrue(run.log().stream().anyMatch(l -> l.contains("MDT_VALIDATOR_RECOMPUTE_SKIPPED")),
        "the marker alerting counts consecutive skips on must be in the log");
    assertFalse(run.log().stream().anyMatch(l -> l.contains("MDT_VALIDATOR_ARCHIVAL_SKIPPED")),
        "a recomputed run must not be counted against the archival threshold");
  }

  /**
   * The case the whole catch-path check exists for: a real duplicate on the table, fail-fast, and a
   * recompute in the same action that raised. The finding may rest on a listing taken after the
   * window closed, so it is unattributable — and an unattributable finding must not page anyone.
   */
  @Test
  void failFastDiscardsAFindingItCannotAttribute() throws IOException {
    writeCowTableWithASupersededSlice();

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false, true, false,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertTrue(run.passed(), "the finding is unattributable, so the run sanctions rather than fails");
    assertFalse(run.thrown().isPresent(),
        "the executor's validation failure must be discarded, not propagated");
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
  }

  /**
   * The negative that keeps the other three honest: a run nobody interferes with must never report
   * a recompute. A baseline snapshotted before the barrier, or a counter bumped somewhere other than
   * the listing, would make every run above pass for the wrong reason.
   */
  @Test
  void anUndisturbedRunNeverReportsARecompute() throws IOException {
    writeTable("COPY_ON_WRITE");

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true, true, true, null);

    assertFalse(run.branches().contains(Branch.DUP_RECOMPUTED_INCONCLUSIVE),
        run.branches().toString());
    assertFalse(run.branches().contains(Branch.FSM_RECOMPUTED_INCONCLUSIVE),
        run.branches().toString());
    assertFalse(run.log().stream().anyMatch(l -> l.contains("MDT_VALIDATOR_RECOMPUTE_SKIPPED")),
        "an undisturbed run has nothing to sanction");
  }

  /** The FS-MDT diff's counterpart: same hazard, same barrier, same refusal. */
  @Test
  void exhaustiveRefusesWhenTheDiffDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, false, false, true,
        dropsTheCacheBeforeTheDiffBarrierRead());

    assertFalse(run.passed(), "a run that cannot attribute its listings must not report success");
    assertTrue(run.thrown().isPresent(), "exhaustive refuses rather than returning");
    assertTrue(run.thrown().get().getMessage().contains("was recomputed after the barrier"),
        run.thrown().get().getMessage());
    assertTrue(run.branches().contains(Branch.FSM_RECOMPUTED_INCONCLUSIVE), run.branches().toString());
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_EXHAUSTIVE_FAILED),
        run.branches().toString());
  }

  @Test
  void failFastSanctionsWhenTheDiffDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false, false, true,
        dropsTheCacheBeforeTheDiffBarrierRead());

    assertTrue(run.passed(), "a sanctioned run reports success and leaves the marker");
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
    assertTrue(run.log().stream().anyMatch(l -> l.contains("MDT_VALIDATOR_RECOMPUTE_SKIPPED")),
        "the marker alerting counts consecutive skips on must be in the log");
  }

  /**
   * The FS-MDT catch-path case: a planted orphan that fail-fast raises a fatal verdict on, and a
   * recompute in the same action that raised. The orphan is real, but the run can no longer say
   * whether it was listed inside the window that would explain it, so it must not fail the run.
   *
   * <p>Without a recompute the same fixture raises — that is
   * {@code TestValidatorV2FsMdtBranches#failFastFailsOnAnOrphanAndPersistsNothing}. This case is
   * that one with the cache dropped, and the difference between them is the whole point.
   */
  @Test
  void failFastDiscardsADiffFindingItCannotAttribute() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    String[] parts = original.getName().split("_");
    StoragePath stranger = new StoragePath(original.getParent(),
        "00000000-0000-0000-0000-0000000000ff-0_" + parts[1] + "_" + parts[2]);
    copyFile(original, stranger);

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false, false, true,
        dropsTheCacheBeforeTheDiffBarrierRead());

    assertTrue(run.passed(), "the finding is unattributable, so the run sanctions rather than fails");
    assertFalse(run.thrown().isPresent(),
        "the executor's validation failure must be discarded, not propagated");
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
  }

  /**
   * Fail-fast still runs the FS-MDT comparison when only dimension A recomputed: dimension B's
   * window is its own, so a real finding there fails this run rather than waiting for the next one.
   */
  @Test
  void failFastStillReportsADiffFindingWhenTheDuplicateDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");
    StoragePath victim = anyFile(".parquet", false);
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, true, true, true,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertFalse(run.passed(), "dimension B's outbound deletion must fail the run");
    assertFalse(run.thrown().isPresent(), "--ignore-failed reports the failure by return value");
    assertTrue(run.branches().contains(Branch.DUP_RECOMPUTED_INCONCLUSIVE), run.branches().toString());
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
    assertTrue(run.branches().contains(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION),
        run.branches().toString());
    assertTrue(run.log().stream().anyMatch(l -> l.contains("MDT_VALIDATOR_RECOMPUTE_SKIPPED")
            && l.contains("the FS-MDT comparison still runs")),
        "dimension A's skip still leaves the marker alerting counts");
  }

  /** The clean counterpart: dimension A's skip alone does not fail a fail-fast run. */
  @Test
  void failFastPassesWhenOnlyTheDuplicateDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, true, true, true,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertTrue(run.passed(), "" + run.thrown());
    assertTrue(run.branches().contains(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED),
        run.branches().toString());
    assertTrue(run.log().stream().anyMatch(l -> l.contains("FS-MDT comparison (v2) completed")),
        "dimension B must have run");
  }

  /**
   * Exhaustive does not run dimension B once dimension A recomputed: the run has no verdict for
   * dimension A, so it must not publish a handle a mitigation job would act on.
   */
  @Test
  void exhaustiveDoesNotRunTheDiffWhenTheDuplicateDimensionRecomputed() throws IOException {
    writeTable("COPY_ON_WRITE");
    deleteFromStorage(anyFile(".parquet", false));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, false, true, true,
        dropsTheCacheBeforeTheDuplicateBarrierRead());

    assertTrue(run.thrown().isPresent(), "exhaustive refuses rather than returning");
    assertTrue(run.thrown().get().getMessage().contains("was recomputed after the barrier"),
        run.thrown().get().getMessage());
    assertFalse(run.branches().contains(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION),
        run.branches().toString());
  }
}
