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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolver;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolvers;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * Phase 2 of the FS-MDT diff over one shuffle partition (batch): whether the archive is read once for
 * the whole task by BOTH readers that consume it -- {@link InstantMembershipResolver} and
 * {@link FsMdtDiffDimension.ArchiveStateLookup} -- whatever the batch's shape, whether an instant no
 * worse than {@code T_aft}'s earliest write commit ever reaches it, and whether a v2-layout table
 * refuses the archive bucket without ever constructing a resolver. Mirrors {@code
 * TestDuplicateBaseFileDimension}'s equivalent section for dimension A.
 */
class TestFsMdtDiffDimension {

  private static final String PARTITION = "2024/01/01";
  private static final String EARLIEST_ACTIVE_WRITE_COMMIT = "2000";

  /**
   * <b>One archived pass for the whole task, whatever the batch's shape.</b>
   *
   * <p>The batch spans two file groups and three instants, all older than {@code T_aft}'s earliest
   * write commit. A "call once per file group" or "call once with a slice of the batch"
   * implementation would still satisfy a bare call count, so the single call is asserted to have been
   * handed <em>every</em> target instant, not just a plausible-looking count.
   */
  @Test
  void fsMdtArchiveIsOpenedOncePerTaskRegardlessOfFileGroupCount(@TempDir Path dir) throws Exception {
    List<FileDiffRecord> batch = Arrays.asList(
        record("fg1", "1001", "fg1_0-1-0_1001.parquet"),
        record("fg1", "1002", "fg1_0-1-0_1002.parquet"),
        record("fg2", "1003", "fg2_0-1-0_1003.parquet"));
    AtomicInteger passes = new AtomicInteger();
    List<Set<String>> asked = new ArrayList<>();
    ArchiveTimelineInstantBatchResolver<ArchivedCommitPaths> resolver = targets -> {
      passes.incrementAndGet();
      asked.add(new HashSet<>(targets));
      return ArchivedInstantWorkingSet.allNotFound(targets);
    };

    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()))
          .thenReturn(true);
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()))
          .thenReturn(resolver);

      adjudicate(batch, dir, HoodieTableVersion.SIX);

      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers
          .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()));
    }

    assertEquals(1, passes.get(), "the whole task reads the archive once, not once per file group");
    assertEquals(Collections.singletonList(
        new HashSet<>(Arrays.asList("1001", "1002", "1003"))), asked,
        "and that one pass is asked about every instant of the batch");
  }

  /**
   * <b>Both archive readers -- {@link InstantMembershipResolver} and
   * {@link FsMdtDiffDimension.ArchiveStateLookup} -- are served by the SAME single pass.</b>
   *
   * <p>The one record's {@code prev}/{@code aft} state comes from the archive
   * ({@code ArchiveStateLookup}, through {@code FileDiffAdjudicator#resolvePrev}/{@code resolveAft})
   * and, once both resolve {@code COMPLETED}, its membership answer comes from the archive too
   * ({@code InstantMembershipResolver}, through {@code lookUpMembership}). Both are drawn from the ONE
   * mocked pass below: this table is freshly initialised and holds no real archive, so a real,
   * independent read by either reader would find nothing and could not have produced
   * {@code COMPLETED} or {@code COMMITTED} on its own. A second archive pass opened directly by
   * {@code ArchiveStateLookup} would take a code path this mock never touches, so a call-count
   * assertion on {@code ArchiveTimelineInstantBatchResolvers} alone would not detect it.
   */
  @Test
  void fsMdtArchiveIsStillOpenedOncePerTaskWithBothReaders(@TempDir Path dir) throws Exception {
    String instant = "1001";
    String fileGroupId = "fg1";
    String fileName = "fg1_0-1-0_1001.parquet";
    FileDiffRecord record = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileGroupId(fileGroupId)
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant(instant, MismatchRecord.InstantSource.FILE_NAME)
        .build();

    AtomicInteger passes = new AtomicInteger();
    ArchiveTimelineInstantBatchResolver<ArchivedCommitPaths> resolver = targets -> {
      passes.incrementAndGet();
      Map<HoodieFileGroupId, Set<String>> content = Collections.singletonMap(
          new HoodieFileGroupId(PARTITION, fileGroupId), Collections.singleton(fileName));
      return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(targets)
          .found(instant, ArchivedCommitPaths.namesOnly(content))
          .build();
    };

    AtomicReference<List<FileDiffRecord>> decidedRef = new AtomicReference<>();
    Set<Branch> taken;
    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()))
          .thenReturn(true);
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()))
          .thenReturn(resolver);

      taken = BranchBeacons.takenBy(() -> {
        try {
          decidedRef.set(adjudicate(Collections.singletonList(record), dir, HoodieTableVersion.SIX));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }

    assertEquals(1, passes.get(), "one shared pass serves both archive readers, not one apiece");
    List<FileDiffRecord> decided = decidedRef.get();
    assertEquals(1, decided.size());
    assertEquals(Verdict.MDT_CORRUPTION, decided.get(0).getVerdict(),
        "the commit completed before T_prev and names this file, so the metadata table lost it");
    assertTrue(taken.contains(Branch.ASL_FOUND_COMPLETED),
        "ArchiveStateLookup placed the instant as COMPLETED from the shared pass");
    assertTrue(taken.contains(Branch.IMR_ARCHIVE_NAMED),
        "InstantMembershipResolver answered membership from the SAME shared pass");
    assertTrue(taken.contains(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED),
        "the adjudicator's own row fired, tying both readers' answers to one verdict");
  }

  /**
   * <b>An instant at or after {@code T_aft}'s earliest write commit never pokes the archive.</b>
   *
   * <p>{@link InstantBuckets#pokesArchive} draws the line the archive bucket is built from; a record
   * on the wrong side of it must never contribute to the archive targets, and neither
   * {@code readsV1Archive} nor {@code forTable} may even be consulted for a batch that is empty of
   * them.
   */
  @Test
  void instantAboveTheAftEarliestNeverReachesTheArchive(@TempDir Path dir) throws Exception {
    List<FileDiffRecord> batch = Collections.singletonList(
        record("fg1", EARLIEST_ACTIVE_WRITE_COMMIT, "fg1_0-1-0_2000.parquet"));

    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      adjudicate(batch, dir, HoodieTableVersion.SIX);

      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()),
          Mockito.never());
      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()),
          Mockito.never());
    }
  }

  /**
   * <b>Pin: an in-window instant is refused, never condemned, even with the archive warm for a
   * sibling in the SAME batch.</b>
   *
   * <p>The batch pairs one archive-bucket suspect ({@code archiveInstant}, below {@code
   * EARLIEST_ACTIVE_WRITE_COMMIT}) with one in-window suspect (AT {@code
   * EARLIEST_ACTIVE_WRITE_COMMIT} itself -- {@link InstantBuckets#pokesArchive}'s boundary is
   * exclusive, so equal counts as in-window, still on the "never poked" side). The first suspect
   * warms the shared pass up; the second's own instant is never a member of the target set that
   * warm-up was built from, so {@link FsMdtDiffDimension.ArchiveStateLookup} refuses it --
   * {@link Branch#ASL_ARCHIVE_INSTANT_NOT_A_TARGET} -- without the mocked pass ever being asked
   * about it, whatever it would have answered, even on a never-archived table (see
   * {@code ArchiveStateLookup}'s class Javadoc). This pins the narrowing so a future change cannot
   * silently widen the target bucket to cover it. The sibling proves the pass really was warm: it
   * settles off the SAME mocked absence the in-window suspect is refused in front of, so the
   * refusal is not merely an artifact of an archive pass that never ran.
   */
  @Test
  void inWindowSuspectIsRefusedEvenWithTheArchiveWarmForASibling(@TempDir Path dir) throws Exception {
    String archiveInstant = "1001";
    FileDiffRecord archiveBucketRecord = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileGroupId("fg1")
        .withFileName("fg1_0-1-0_" + archiveInstant + ".parquet")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant(archiveInstant, MismatchRecord.InstantSource.FILE_NAME)
        .build();
    FileDiffRecord inWindowRecord = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileGroupId("fg2")
        .withFileName("fg2_0-1-0_" + EARLIEST_ACTIVE_WRITE_COMMIT + ".parquet")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant(EARLIEST_ACTIVE_WRITE_COMMIT, MismatchRecord.InstantSource.FILE_NAME)
        .build();

    ArchiveTimelineInstantBatchResolver<ArchivedCommitPaths> resolver =
        ArchivedInstantWorkingSet::allNotFound;

    AtomicReference<List<FileDiffRecord>> decidedRef = new AtomicReference<>();
    Set<Branch> taken;
    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()))
          .thenReturn(true);
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()))
          .thenReturn(resolver);

      taken = BranchBeacons.takenBy(() -> {
        try {
          decidedRef.set(adjudicate(
              Arrays.asList(archiveBucketRecord, inWindowRecord), dir, HoodieTableVersion.SIX));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers
          .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()));
    }

    List<FileDiffRecord> decided = decidedRef.get();
    assertEquals(2, decided.size());
    FileDiffRecord archiveBucketDecided = decided.stream()
        .filter(r -> "fg1".equals(r.getFileGroupId())).findFirst()
        .orElseThrow(() -> new AssertionError("fg1 missing from " + decided));
    FileDiffRecord inWindowDecided = decided.stream()
        .filter(r -> "fg2".equals(r.getFileGroupId())).findFirst()
        .orElseThrow(() -> new AssertionError("fg2 missing from " + decided));

    assertEquals(Verdict.DANGLING, archiveBucketDecided.getVerdict(),
        "the archive-bucket sibling proves the pass really ran and really answered: absence trusted");
    assertEquals(Verdict.NOT_SETTLED, inWindowDecided.getVerdict(),
        "the in-window suspect must be refused, never condemned, even though the pass answered for "
            + "its sibling in the SAME batch");
    assertTrue(taken.contains(Branch.ASL_ARCHIVE_INSTANT_NOT_A_TARGET),
        "refused because its own instant was never a member of the warmed-up target set");
    assertTrue(taken.contains(Branch.ASL_ABSENCE_TRUSTED_NULL),
        "and the sibling shows the SAME pass really did answer NOT_FOUND for what it WAS asked");
    assertTrue(taken.contains(Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE),
        "the in-window suspect's refusal reaches the adjudicator's own unresolved-arm row");
  }

  /**
   * <b>A v2-layout table constructs no resolver at all, and the archive bucket is refused.</b>
   *
   * <p>{@code V2ArchiveTimelineInstantBatchResolver.warmUp} throws {@code
   * UnsupportedOperationException} by design, so an implementation that built one and called it would
   * fail this task outright rather than refuse politely. {@code forTable} is left unstubbed here on
   * purpose: if production code ever reached it despite {@code readsV1Archive} returning false, the
   * unstubbed static mock returns {@code null} and the subsequent {@code warmUp} call throws a
   * {@code NullPointerException} the test does not catch, failing loudly rather than passing by luck.
   */
  @Test
  void v2TableRefusesTheArchiveBucketWithoutConstructingAResolver(@TempDir Path dir) throws Exception {
    List<FileDiffRecord> batch = Collections.singletonList(
        record("fg1", "1001", "fg1_0-1-0_1001.parquet"));

    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()))
          .thenReturn(false);

      adjudicate(batch, dir, HoodieTableVersion.EIGHT);

      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()));
      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()),
          Mockito.never());
    }
  }

  // ---------------------------------------------------------------------------------------------
  //  Fixtures
  // ---------------------------------------------------------------------------------------------

  private static FileDiffRecord record(String fileGroupId, String writingInstant, String fileName) {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileGroupId(fileGroupId)
        .withFileName(fileName)
        .withWritingInstant(writingInstant, MismatchRecord.InstantSource.FILE_NAME)
        .build();
  }

  /** Runs {@code adjudicateBatch} over {@code batch} against a real table of {@code version}. */
  private static List<FileDiffRecord> adjudicate(List<FileDiffRecord> batch, Path dir,
                                                  HoodieTableVersion version) throws IOException {
    List<FileDiffRecord> decided = new ArrayList<>();
    FsMdtDiffDimension.adjudicateBatch(batch.iterator(), adjudicationContext(dir, version),
        new LongAccumulator()).forEachRemaining(decided::add);
    return decided;
  }

  /**
   * A phase-2 context over a real, empty table of {@code version} -- real because the executor builds
   * its own storage-only meta client from it -- whose {@code T_aft} holds nothing completed, and
   * whose {@code earliestActiveWriteCommit} is {@link #EARLIEST_ACTIVE_WRITE_COMMIT}, so every record
   * older than that lands in the archive bucket.
   */
  private static FsMdtDiffDimension.AdjudicationContext adjudicationContext(
      Path dir, HoodieTableVersion version) throws IOException {
    HoodieTableMetaClient metaClient;
    try {
      metaClient = HoodieTestUtils.init(
          dir.toAbsolutePath().toString(), HoodieTableType.COPY_ON_WRITE, version);
    } catch (Exception e) {
      throw new IOException("could not initialise the fixture table", e);
    }
    HoodieTimeline emptyInstants = Mockito.mock(HoodieTimeline.class);
    Mockito.when(emptyInstants.getInstants()).thenReturn(Collections.emptyList());
    InstantStateIndex prevIndex = InstantStateIndex.from(emptyInstants);
    InstantStateIndex aftIndex = InstantStateIndex.from(emptyInstants);
    HoodieTimeline noCompletedCommits = Mockito.mock(HoodieTimeline.class);
    Mockito.when(noCompletedCommits.getInstants()).thenReturn(Collections.emptyList());
    HoodieTimeline commits = Mockito.mock(HoodieTimeline.class);
    Mockito.when(commits.filterCompletedInstants()).thenReturn(noCompletedCommits);
    HoodieTimeline tAft = Mockito.mock(HoodieTimeline.class);
    Mockito.when(tAft.getCommitsTimeline()).thenReturn(commits);
    return new FsMdtDiffDimension.AdjudicationContext(prevIndex, aftIndex,
        ServiceActivityDelta.forTesting().build(), PendingServiceState.empty(),
        ExecutorMetaClientFactory.from(metaClient), metaClient.getBasePath().toString(), tAft, false,
        EARLIEST_ACTIVE_WRITE_COMMIT);
  }
}
