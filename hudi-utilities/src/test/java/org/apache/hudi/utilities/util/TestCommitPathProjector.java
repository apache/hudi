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

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCommitMetadata;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.avro.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.avro.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>The projection is keyed by file group, and that is a safety property rather than a shape.</b>
 * A consumer asks this map whether one commit referenced one file; answered out of a flat set
 * unioned across the batch, a commit that wrote some other group hands back names that do not
 * contain the file, which downstream is a delete authorisation. Every assertion below therefore
 * reads one group's entry, and {@link #namesOnlyTheOtherGroupsCommitRecordedAreNotAscribedToThisOne}
 * pins the scoping itself.
 */
class TestCommitPathProjector {

  private static final StoragePath BASE = new StoragePath("file:/tmp/table");
  private static final String PARTITION = "2024/01/01";
  private static final String FILE_ID = "fg-1";
  private static final HoodieFileGroupId GROUP = new HoodieFileGroupId(PARTITION, FILE_ID);
  private static final HoodieFileGroupId OTHER_GROUP = new HoodieFileGroupId(PARTITION, "fg-9");

  private static CommitPathProjector projector() {
    return new CommitPathProjector(BASE, Collections.singleton(GROUP));
  }

  /** What the projection ascribes to {@code group}, absent and empty read alike. */
  private static Set<String> namesOf(Map<HoodieFileGroupId, Set<String>> projected,
                                     HoodieFileGroupId group) {
    Set<String> names = projected.get(group);
    return names == null ? Collections.emptySet() : names;
  }

  /** The one-group projection an assertion compares against. */
  private static Map<HoodieFileGroupId, Set<String>> onlyGroup(String... names) {
    Map<HoodieFileGroupId, Set<String>> expected = new LinkedHashMap<>();
    expected.put(GROUP, new HashSet<>(Arrays.asList(names)));
    return expected;
  }

  private static HoodieWriteStat stat(String fileId, String path) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    return stat;
  }

  private static GenericRecord commit(String partitionKey, HoodieWriteStat... stats) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setPartitionToWriteStats(
        Collections.singletonMap(partitionKey, Arrays.asList(stats)));
    return metadata;
  }

  @Test
  void projectsTheBareNameOfAWriteStatPath() throws IOException {
    GenericRecord record =
        commit(PARTITION, stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"));

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(record).namesByGroup());
  }

  @Test
  void projectsLogFilesAlongsideThePath() throws IOException {
    // getLogFiles() is a superset seeded from the pre-existing file slice, so consulting it can
    // only ever turn a would-be DANGLING into a REFERENCED.
    HoodieWriteStat writeStat = stat(FILE_ID, PARTITION + "/.fg-1_20240101010000.log.1_0-1-0");
    writeStat.setLogFiles(Collections.singletonList(PARTITION + "/.fg-1_20231201000000.log.1_0-9-0"));

    Set<String> projected = namesOf(projector().project(commit(PARTITION, writeStat)).namesByGroup(), GROUP);

    assertTrue(projected.contains(".fg-1_20240101010000.log.1_0-1-0"));
    assertTrue(projected.contains(".fg-1_20231201000000.log.1_0-9-0"));
  }

  @Test
  void projectsCdcFilesWhichAreNamedNowhereElse() throws IOException {
    HoodieWriteStat writeStat = stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet");
    writeStat.setCdcStats(Collections.singletonMap(PARTITION + "/.fg-1_20240101010000.cdc", 42L));

    Set<String> projected = namesOf(projector().project(commit(PARTITION, writeStat)).namesByGroup(), GROUP);

    assertTrue(projected.contains(".fg-1_20240101010000.cdc"));
  }

  @Test
  void matchesAPartitionTheCommitSpellsAbsolutely() throws IOException {
    // The raw-string comparison this replaces answered "not found" for every file in the partition.
    GenericRecord record = commit("file:/tmp/table/" + PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"));

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(record).namesByGroup(),
        "and the key is the group id the CALLER supplied, not the record's spelling of it");
  }

  @Test
  void ignoresOtherFileGroupsAndOtherPartitions() throws IOException {
    GenericRecord record = commit(PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"),
        stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet"));

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(record).namesByGroup());
  }

  @Test
  void spansEveryWantedFileGroupInOnePass() throws IOException {
    CommitPathProjector multi =
        new CommitPathProjector(BASE, new HashSet<>(Arrays.asList(GROUP, OTHER_GROUP)));
    GenericRecord record = commit(PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"),
        stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet"));

    Map<HoodieFileGroupId, Set<String>> projected = multi.project(record).namesByGroup();

    assertEquals(Collections.singleton("fg-1_0-1-0_20240101010000.parquet"),
        namesOf(projected, GROUP));
    assertEquals(Collections.singleton("fg-9_0-2-0_20240101010000.parquet"),
        namesOf(projected, OTHER_GROUP),
        "one pass covers every wanted group, and each keeps its own names");
  }

  /**
   * <b>The scoping itself: names a commit recorded for one group are never ascribed to another.</b>
   *
   * <p>Both groups are wanted -- which is what a batch spanning two file groups produces -- and the
   * commit records stats for {@code fg-9} alone. Unioned into one flat set, {@code fg-1} would come
   * back with a non-empty name set that does not contain its file, and its consumer would read that
   * as "the commit spoke about this group and declined to claim the file" and authorise a delete.
   * Worse, whether that happened would depend on whether {@code fg-9} landed in the same shuffle
   * partition. Keyed, {@code fg-1}'s entry is absent, which refuses.
   */
  @Test
  void namesOnlyTheOtherGroupsCommitRecordedAreNotAscribedToThisOne() throws IOException {
    CommitPathProjector multi =
        new CommitPathProjector(BASE, new HashSet<>(Arrays.asList(GROUP, OTHER_GROUP)));
    GenericRecord record =
        commit(PARTITION, stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet"));

    Map<HoodieFileGroupId, Set<String>> projected = multi.project(record).namesByGroup();

    assertTrue(namesOf(projected, GROUP).isEmpty(),
        "a group the commit said nothing about must be absent or empty, never another group's names");
    assertEquals(Collections.singleton("fg-9_0-2-0_20240101010000.parquet"),
        namesOf(projected, OTHER_GROUP),
        "and the group it did record still carries exactly its own");
  }

  @Test
  void writeStatWhoseSchemaPredatesLogFilesAndCdcStatsStillProjects() throws IOException {
    // Archived records decode against the writer schema they were written with. A table archived
    // under an older Hudi has write stats with no logFiles/cdcStats field at all, and reading those
    // strictly would make every such instant INDETERMINATE -- disabling the archive path on exactly
    // the tables with the most archived history.
    Schema statSchema = SchemaBuilder.record("HoodieWriteStat").fields()
        .name("fileId").type().stringType().noDefault()
        .name("path").type().stringType().noDefault()
        .endRecord();
    Schema commitSchema = SchemaBuilder.record("HoodieCommitMetadata").fields()
        .name("partitionToWriteStats").type().map().values().array().items(statSchema).noDefault()
        .endRecord();

    GenericRecord oldStat = new GenericData.Record(statSchema);
    oldStat.put("fileId", FILE_ID);
    oldStat.put("path", PARTITION + "/fg-1_0-1-0_20240101010000.parquet");
    GenericRecord oldCommit = new GenericData.Record(commitSchema);
    oldCommit.put("partitionToWriteStats",
        Collections.singletonMap(PARTITION, Collections.singletonList(oldStat)));

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(oldCommit).namesByGroup());
  }

  @Test
  void commitThatNamesNothingForTheGroupProjectsEmpty() throws IOException {
    GenericRecord record = commit(PARTITION, stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet"));

    assertTrue(projector().project(record).namesByGroup().isEmpty());
  }

  @Test
  void recordWithNoPartitionToWriteStatsFieldIsRefused() {
    // "I could not understand this" must never become "the commit records nothing here".
    GenericRecord notCommitMetadata = new HoodieWriteStat();

    assertThrows(IOException.class, () -> projector().project(notCommitMetadata));
  }

  /**
   * Merge unions the key sets AND, for a key both carry, the two name sets -- an entry that merely
   * overwrote would drop half of an instant that arrived as several records, which is the ordinary
   * v6 shape. Neither argument may be mutated: the pass is multi-threaded and either may be visible
   * to another thread.
   */
  @Test
  void mergeIsOrderIndependentAndUnionsPerFileGroup() {
    Map<HoodieFileGroupId, Set<String>> first = new LinkedHashMap<>();
    first.put(GROUP, new HashSet<>(Collections.singletonList("a.parquet")));
    Map<HoodieFileGroupId, Set<String>> second = new LinkedHashMap<>();
    second.put(GROUP, new HashSet<>(Collections.singletonList("b.parquet")));
    second.put(OTHER_GROUP, new HashSet<>(Collections.singletonList("c.parquet")));

    Map<HoodieFileGroupId, Set<String>> expected = new LinkedHashMap<>();
    expected.put(GROUP, new HashSet<>(Arrays.asList("a.parquet", "b.parquet")));
    expected.put(OTHER_GROUP, new HashSet<>(Collections.singletonList("c.parquet")));

    ArchivedCommitPaths firstPaths = ArchivedCommitPaths.namesOnly(first);
    ArchivedCommitPaths secondPaths = ArchivedCommitPaths.namesOnly(second);
    assertEquals(expected, projector().merge(firstPaths, secondPaths).namesByGroup());
    assertEquals(expected, projector().merge(secondPaths, firstPaths).namesByGroup());
    assertEquals(Collections.singleton("a.parquet"), first.get(GROUP), "first was not mutated");
    assertEquals(Collections.singleton("b.parquet"), second.get(GROUP), "second was not mutated");
  }

  // -- Envelope shapes: this is the production contract. HoodieArchiveFileScanner opens every v6
  // archive file with HoodieArchivedMetaEntry.getClassSchema(), so every real archived record
  // arrives wrapped in one of these envelopes, never as bare commit metadata.

  @Test
  void v6EnvelopeUnderHoodieCommitMetadataProjects() throws IOException {
    HoodieCommitMetadata commitMetadata = (HoodieCommitMetadata) commit(PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"));
    HoodieArchivedMetaEntry envelope = new HoodieArchivedMetaEntry();
    envelope.setHoodieCommitMetadata(commitMetadata);

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(envelope).namesByGroup());
  }

  @Test
  void v6EnvelopeUnderHoodieReplaceCommitMetadataProjects() throws IOException {
    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    replaceCommitMetadata.setPartitionToWriteStats(Collections.singletonMap(PARTITION,
        Collections.singletonList(stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"))));
    HoodieArchivedMetaEntry envelope = new HoodieArchivedMetaEntry();
    envelope.setHoodieReplaceCommitMetadata(replaceCommitMetadata);

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(envelope).namesByGroup());
  }

  @Test
  void v6EnvelopeWithBothPayloadsNullProjectsEmpty() throws IOException {
    HoodieArchivedMetaEntry envelope = new HoodieArchivedMetaEntry();
    // hoodieCommitMetadata and hoodieReplaceCommitMetadata both left at their null default: an
    // action recorded on the archive that wrote no write stats, not an undecodable record.

    assertTrue(projector().project(envelope).namesByGroup().isEmpty());
  }

  @Test
  void lsmEnvelopeWithSerialisedMetadataProjects() throws IOException {
    GenericRecord commitMetadata =
        commit(PARTITION, stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"));
    HoodieLSMTimelineInstant envelope = new HoodieLSMTimelineInstant();
    envelope.setMetadata(serialise(commitMetadata));

    assertEquals(onlyGroup("fg-1_0-1-0_20240101010000.parquet"), projector().project(envelope).namesByGroup());
  }

  @Test
  void lsmEnvelopeWithNullMetadataProjectsEmpty() throws IOException {
    HoodieLSMTimelineInstant envelope = new HoodieLSMTimelineInstant();
    // metadata left at its null default: a completed instant whose meta file was empty.

    assertTrue(projector().project(envelope).namesByGroup().isEmpty());
  }

  @Test
  void recordCarryingNoneOfTheThreeShapesIsRefused() {
    // Neither 'metadata', nor 'hoodieCommitMetadata'/'hoodieReplaceCommitMetadata', nor
    // 'partitionToWriteStats' -- this decoder must refuse rather than guess.
    GenericRecord unrecognisable = new HoodieWriteStat();

    assertThrows(IOException.class, () -> projector().project(unrecognisable));
  }

  @Test
  void logFilesPresentButWrongTypedIsRefused() {
    Schema statSchema = SchemaBuilder.record("HoodieWriteStat").fields()
        .name("fileId").type().stringType().noDefault()
        .name("path").type().stringType().noDefault()
        .name("logFiles").type().stringType().noDefault()
        .endRecord();
    Schema commitSchema = SchemaBuilder.record("HoodieCommitMetadata").fields()
        .name("partitionToWriteStats").type().map().values().array().items(statSchema).noDefault()
        .endRecord();

    GenericRecord badStat = new GenericData.Record(statSchema);
    badStat.put("fileId", FILE_ID);
    badStat.put("path", PARTITION + "/fg-1_0-1-0_20240101010000.parquet");
    badStat.put("logFiles", "not-a-collection");
    GenericRecord badCommit = new GenericData.Record(commitSchema);
    badCommit.put("partitionToWriteStats",
        Collections.singletonMap(PARTITION, Collections.singletonList(badStat)));

    assertThrows(IOException.class, () -> projector().project(badCommit));
  }

  // -- Completed-commit facts: what lets an absent group be read as proof.

  @Test
  void completedReplaceCommitRecordingAnotherGroupInThePartitionNamesNothingForThisOne()
      throws IOException {
    ArchivedCommitPaths projected = projector().project(
        completedReplaceEntry(PARTITION, stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet")));

    assertTrue(projected.namesOf(GROUP).isEmpty());
    assertTrue(projected.isCompletedWithWriteStats());
  }

  @Test
  void completedCommitInAnotherPartitionOnlyNamesNothingForTheGroup() throws IOException {
    ArchivedCommitPaths projected = projector().project(
        completedReplaceEntry("2024/01/02", stat(FILE_ID, "2024/01/02/fg-1_0-2-0_20240101010000.parquet")));

    assertTrue(projected.namesOf(GROUP).isEmpty(),
        "the same file id under another partition is another file group");
    assertTrue(projected.isCompletedWithWriteStats());
  }

  @Test
  void absolutePartitionSpellingStillNamesTheGroup() throws IOException {
    ArchivedCommitPaths projected = projector().project(completedReplaceEntry(BASE + "/" + PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet")));

    assertEquals(Collections.singleton("fg-1_0-1-0_20240101010000.parquet"), projected.namesOf(GROUP));
    assertTrue(projected.isCompletedWithWriteStats());
  }

  @Test
  void completedCommitNamingTheGroupRecordsIt() throws IOException {
    ArchivedCommitPaths projected = projector().project(
        completedReplaceEntry(PARTITION, stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet")));

    assertEquals(Collections.singleton("fg-1_0-1-0_20240101010000.parquet"), projected.namesOf(GROUP));
    assertTrue(projected.isCompletedWithWriteStats());
  }

  @Test
  void pendingEntryProjectsNamesButNoCompletedFacts() throws IOException {
    HoodieArchivedMetaEntry inflight = completedReplaceEntry(PARTITION,
        stat(FILE_ID, PARTITION + "/fg-1_0-1-0_20240101010000.parquet"));
    inflight.setActionState("INFLIGHT");

    ArchivedCommitPaths projected = projector().project(inflight);

    assertEquals(Collections.singleton("fg-1_0-1-0_20240101010000.parquet"), projected.namesOf(GROUP));
    assertFalse(projected.isCompletedWithWriteStats());
  }

  @Test
  void completedCommitWithNoWriteStatsRecordsNothing() throws IOException {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    metadata.setPartitionToWriteStats(Collections.emptyMap());
    HoodieArchivedMetaEntry envelope = new HoodieArchivedMetaEntry();
    envelope.setHoodieReplaceCommitMetadata(metadata);
    envelope.setActionState("COMPLETED");

    ArchivedCommitPaths projected = projector().project(envelope);

    assertFalse(projected.isCompletedWithWriteStats());
    assertTrue(projected.namesOf(GROUP).isEmpty());
  }

  @Test
  void mergingAPendingEntryKeepsTheCompletedFacts() throws IOException {
    HoodieArchivedMetaEntry requested = new HoodieArchivedMetaEntry();
    requested.setActionState("REQUESTED");
    ArchivedCommitPaths completed = projector().project(
        completedReplaceEntry(PARTITION, stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet")));
    ArchivedCommitPaths pending = projector().project(requested);

    for (ArchivedCommitPaths merged : Arrays.asList(projector().merge(completed, pending),
        projector().merge(pending, completed))) {
      assertTrue(merged.isCompletedWithWriteStats());
      assertTrue(merged.namesOf(GROUP).isEmpty());
    }
  }

  @Test
  void lsmRecordIsCompleted() throws IOException {
    HoodieLSMTimelineInstant envelope = new HoodieLSMTimelineInstant();
    envelope.setMetadata(serialise(commit(PARTITION, stat("fg-9", PARTITION + "/fg-9_0-2-0_20240101010000.parquet"))));

    ArchivedCommitPaths projected = projector().project(envelope);

    assertTrue(projected.isCompletedWithWriteStats());
    assertTrue(projected.namesOf(GROUP).isEmpty());
  }

  private static HoodieArchivedMetaEntry completedReplaceEntry(String partitionKey, HoodieWriteStat stat) {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    metadata.setPartitionToWriteStats(
        Collections.singletonMap(partitionKey, Collections.singletonList(stat)));
    HoodieArchivedMetaEntry envelope = new HoodieArchivedMetaEntry();
    envelope.setHoodieReplaceCommitMetadata(metadata);
    envelope.setActionState("COMPLETED");
    return envelope;
  }

  /** Serialises a record as the self-describing avro data file {@code pathsFromSerialisedMetadata}
   *  reads back -- the same mechanism the v9 archive uses for {@code HoodieLSMTimelineInstant}'s
   *  {@code metadata} field. */
  private static ByteBuffer serialise(GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(record.getSchema(), out);
      writer.append(record);
    }
    return ByteBuffer.wrap(out.toByteArray());
  }
}
