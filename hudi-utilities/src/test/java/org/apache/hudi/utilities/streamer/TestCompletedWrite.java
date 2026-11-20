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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.client.timeline.LSMTimelineWriter;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.ArchivedTimelineV1;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;

/** {@link CompletedWrite}: whether a data-write instant completed, in the active timeline or the archive (LSM and legacy). */
class TestCompletedWrite {
  private static final String BEFORE = "20261112080000000";
  private static final String FIRST = "20261112090000000";
  private static final String SECOND = "20261112100000000";
  private static final String THIRD = "20261112110000000";
  private static final String AFTER = "20261112120000000";

  @TempDir
  Path temp;

  /**
   * The archiver archives only completed instants, so an instant still requested or inflight in the active timeline (a skipped
   * empty commit, a failed write not rolled back yet) is answered without reading the archive. An instant absent from the active
   * timeline (archived inside or before the window, rolled back, or unknown) reads it.
   */
  @Test
  void archiveIsReadOnlyForInstantsAbsentFromTheActiveTimeline() throws Exception {
    HoodieTableMetaClient meta = table();
    commit(meta, FIRST);
    commit(meta, SECOND);
    commit(meta, THIRD);
    archive(meta, SECOND);
    meta.reloadActiveTimeline().createNewInstant(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, AFTER));

    Observed pending = observe(meta);
    assertFalse(CompletedWrite.exists(pending.meta, AFTER), "a pending latest instant is not completed");
    assertEquals(0, mockingDetails(pending.loader).getInvocations().size(), "a pending instant never reads the archive");

    Observed active = observe(meta);
    assertTrue(CompletedWrite.exists(active.meta, THIRD));
    assertEquals(0, mockingDetails(active.loader).getInvocations().size(), "an active completed commit never reads the archive");

    Observed hole = observe(meta);
    assertTrue(CompletedWrite.exists(hole.meta, SECOND), "the instant archived inside the active window is found in the archive");
    assertTrue(mockingDetails(hole.loader).getInvocations().size() > 0, "an instant inside the active window reads the archive");

    Observed before = observe(meta);
    assertFalse(CompletedWrite.exists(before.meta, BEFORE));
    assertTrue(mockingDetails(before.loader).getInvocations().size() > 0, "an instant before the active window reads the archive");
  }

  /**
   * Archive beyond savepoint can archive every commit that is not savepointed, so an archived commit can be newer than every
   * active one; deleting the savepoint afterwards changes nothing: the archived commit is still found.
   */
  @Test
  void commitArchivedAfterADeletedSavepointIsStillACompletedWrite() throws Exception {
    HoodieTableMetaClient meta = table();
    commit(meta, FIRST);
    commit(meta, SECOND);
    commit(meta, THIRD);
    savepoint(meta, SECOND);
    archive(meta, THIRD);
    assertTrue(CompletedWrite.exists(meta, THIRD), "the commit archived after the savepointed newest active commit");
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.reloadActiveTimeline().getSavePointTimeline().filterCompletedInstants().firstInstant().get());
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, SECOND));
    HoodieTimeline active = meta.reloadActiveTimeline();
    assertTrue(active.getSavePointTimeline().empty());
    assertEquals(SECOND, active.getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime());
    assertTrue(CompletedWrite.exists(meta, THIRD), "the archived commit after the deleted savepoint is still found");
  }

  /**
   * A skipped empty commit (the commit returns before it completes the instant) leaves the round's instant pending, as does a
   * write that failed and is not rolled back yet: neither is a completed write, and neither reads the archive.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieInstant.State.class, names = {"REQUESTED", "INFLIGHT"})
  void pendingInstantIsNotACompletedWriteAndReadsNoArchive(HoodieInstant.State state) throws Exception {
    HoodieTableMetaClient meta = table();
    commit(meta, FIRST);
    commit(meta, SECOND);
    archive(meta, FIRST);
    meta.reloadActiveTimeline().createNewInstant(meta.getInstantGenerator().createNewInstant(state, HoodieTimeline.COMMIT_ACTION, THIRD));

    Observed pending = observe(meta);
    assertFalse(CompletedWrite.exists(pending.meta, THIRD), "a " + state + " instant is not a completed write");
    assertEquals(0, mockingDetails(pending.loader).getInvocations().size(), "a " + state + " instant never reads the archive");

    Observed archived = observe(meta);
    assertTrue(CompletedWrite.exists(archived.meta, FIRST), "the archived commit of the same table is found");
    assertTrue(mockingDetails(archived.loader).getInvocations().size() > 0);
  }

  /**
   * A legacy (table version 6) archive keeps every state of an archived instant: only its COMPLETED commit record proves the write,
   * and an instant archived with no completed record is not a completed write.
   */
  @Test
  void legacyArchivedCommitIsACompletedWriteOnlyThroughItsCompletedRecord() throws Exception {
    HoodieTableMetaClient meta = table(HoodieTableVersion.SIX);
    assertEquals(1, meta.getTimelineLayoutVersion().getVersion(), "the legacy timeline layout");
    commit(meta, FIRST);
    commit(meta, SECOND);
    commit(meta, THIRD);
    archiveLegacy(meta, FIRST, HoodieInstant.State.REQUESTED, HoodieInstant.State.INFLIGHT, HoodieInstant.State.COMPLETED);
    archiveLegacy(meta, BEFORE, HoodieInstant.State.REQUESTED, HoodieInstant.State.INFLIGHT);

    assertFalse(meta.reloadActiveTimeline().containsInstant(FIRST), "the commit left the active timeline");
    assertTrue(CompletedWrite.exists(meta, FIRST), "the legacy archived commit is a completed write");
    assertTrue(CompletedWrite.exists(meta, THIRD), "the active commit is a completed write");
    assertFalse(CompletedWrite.exists(meta, BEFORE), "pending records alone prove no completed write");
    assertFalse(CompletedWrite.exists(meta, AFTER), "an unknown instant is not a completed write");
  }

  private HoodieTableMetaClient table() throws Exception {
    return table(HoodieTableVersion.NINE);
  }

  private HoodieTableMetaClient table(HoodieTableVersion version) throws Exception {
    StorageConfiguration<?> conf = HoodieTestUtils.getDefaultStorageConf();
    return HoodieTableMetaClient.newTableBuilder().setTableVersion(version).setTableName("completed_write")
        .setTableType(HoodieTableType.COPY_ON_WRITE).initTable(conf, temp.resolve("table").toUri().toString());
  }

  /**
   * Moves the commit {@code instant} to a legacy archive file the way TimelineArchiverV1 does, one record per given state, and
   * deletes its active instant files.
   */
  private static void archiveLegacy(HoodieTableMetaClient meta, String instant, HoodieInstant.State... states) throws Exception {
    List<HoodieRecord> records = Stream.of(states).map(state -> {
      HoodieArchivedMetaEntry entry = new HoodieArchivedMetaEntry();
      entry.setCommitTime(instant);
      entry.setActionState(state.name());
      entry.setActionType(ActionType.commit.name());
      entry.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build());
      return (HoodieRecord) new HoodieAvroIndexedRecord(entry);
    }).collect(Collectors.toList());
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieArchivedMetaEntry.getClassSchema().toString());
    StoragePath archiveFile = ArchivedTimelineV1.getArchiveLogPath(meta.getArchivePath());
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(archiveFile.getParent())
        .withLogFileId(archiveFile.getName())
        .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
        .withStorage(meta.getStorage())
        .withInstantTime("")
        .build()) {
      writer.appendBlock(new HoodieAvroDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
    }
    for (HoodieInstant.State state : HoodieInstant.State.values()) {
      if (state != HoodieInstant.State.NIL) {
        meta.getActiveTimeline().deleteInstantFileIfExists(
            meta.getInstantGenerator().createNewInstant(state, HoodieTimeline.COMMIT_ACTION, instant));
      }
    }
  }

  private static void commit(HoodieTableMetaClient meta, String instant) {
    HoodieInstant inflight = meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant);
    meta.reloadActiveTimeline().createNewInstant(inflight);
    meta.getActiveTimeline().saveAsComplete(inflight, Option.of(new HoodieCommitMetadata()));
  }

  private static void savepoint(HoodieTableMetaClient meta, String instant) {
    HoodieInstant inflight = meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, instant);
    meta.reloadActiveTimeline().createNewInstant(inflight);
    meta.getActiveTimeline().saveAsComplete(inflight,
        Option.of(TimelineMetadataUtils.convertSavepointMetadata("test", "test", Collections.emptyMap())));
  }

  /** Moves the completed commit {@code instant} to the archive the way the archiver does, leaving the rest active. */
  private static void archive(HoodieTableMetaClient meta, String instant) {
    HoodieInstant completed = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(candidate -> candidate.requestedTime().equals(instant)).findFirst().get();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(meta.getBasePath().toString()).forTable("completed_write").build();
    LSMTimelineWriter.getInstance(config, new LocalTaskContextSupplier(), meta).write(
        Collections.singletonList(ActiveAction.fromInstants(Collections.singletonList(completed))), Option.empty(),
        Option.of(failure -> {
          throw new IllegalStateException("cannot prepare archive fixture", failure);
        }));
    meta.getActiveTimeline().deleteInstantFileIfExists(completed);
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant));
  }

  /** A fresh meta client of {@code meta}'s table whose archived-timeline loader records every call. */
  private static Observed observe(HoodieTableMetaClient meta) {
    HoodieTableMetaClient fresh = spy(HoodieTableMetaClient.builder().setBasePath(meta.getBasePath().toString())
        .setConf(meta.getStorageConf()).build());
    HoodieTableFormat format = spy(fresh.getTableFormat());
    TimelineFactory factory = spy(format.getTimelineFactory());
    ArchivedTimelineLoader loader = spy(factory.createArchivedTimelineLoader());
    doReturn(loader).when(factory).createArchivedTimelineLoader();
    doReturn(factory).when(format).getTimelineFactory();
    doReturn(format).when(fresh).getTableFormat();
    return new Observed(fresh, loader);
  }

  private static final class Observed {
    private final HoodieTableMetaClient meta;
    private final ArchivedTimelineLoader loader;

    private Observed(HoodieTableMetaClient meta, ArchivedTimelineLoader loader) {
      this.meta = meta;
      this.loader = loader;
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"REQUESTED", "INFLIGHT", "COMPLETED"})
  void legacyArchiveProofRequiresCompletion(String state) {
    GenericRecord record = new GenericData.Record(HoodieArchivedMetaEntry.getClassSchema());
    record.put("actionType", "commit");
    record.put("actionState", state);
    assertEquals("COMPLETED".equals(state), assertDoesNotThrow(() -> CompletedWrite.isCompletedArchiveRecord(record)));
  }

  @Test
  void legacyArchiveWithoutStateUsesItsDocumentedCompletedDefault() {
    GenericRecord record = new GenericData.Record(HoodieArchivedMetaEntry.getClassSchema());
    record.put("actionType", "commit");
    assertTrue(assertDoesNotThrow(() -> CompletedWrite.isCompletedArchiveRecord(record)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"commit", "deltacommit", "replacecommit"})
  void lsmCompletedDataWritesRemainRecognized(String action) {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put("action", action);
    assertTrue(CompletedWrite.isCompletedArchiveRecord(record));
  }

  @Test
  void tableServiceRecordIsNotProofOfADataWrite() {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put("action", "clean");
    assertFalse(CompletedWrite.isCompletedArchiveRecord(record));
  }
}
