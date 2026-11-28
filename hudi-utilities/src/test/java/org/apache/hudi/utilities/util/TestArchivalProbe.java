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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * The archiver is the only actor that removes instants from the active timeline without leaving
 * anything on it to say so, which is the one change {@code ServiceActivityDelta} cannot model. These cases
 * are about the probe that detects it having run.
 *
 * <p>The pre-version-8 half is driven through a mocked listing rather than a real table because the
 * property under test is about <em>lengths</em>: the v1 archiver appends log blocks to the current
 * {@code .commits_.archive.N} until it rolls over, so an archival can change no file name at all,
 * and producing that state on a real table means driving a real archiver to a rollover boundary.
 */
class TestArchivalProbe {

  private static final StoragePath ARCHIVE = new StoragePath("file:/tmp/tbl/.hoodie/archived");

  @Test
  void twoReadingsOfAnUnchangedLegacyArchiveAgree() {
    HoodieTableMetaClient metaClient = legacyMetaClientListing(
        entry(".commits_.archive.1", 4096L), entry(".commits_.archive.2", 128L));

    assertEquals(ArchivalProbe.signature(metaClient), ArchivalProbe.signature(metaClient));
    assertFalse(ArchivalProbe.archivalRan(
        ArchivalProbe.signature(metaClient), ArchivalProbe.signature(metaClient)));
  }

  /**
   * The case a name-only digest would miss, and the reason lengths are in the signature: the v1
   * archiver appends to the current archive file until it rolls over, so a real archival can leave
   * the listing's file names untouched.
   */
  @Test
  void appendToTheCurrentArchiveFileIsDetected() {
    String before = ArchivalProbe.signature(
        legacyMetaClientListing(entry(".commits_.archive.1", 4096L)));
    String after = ArchivalProbe.signature(
        legacyMetaClientListing(entry(".commits_.archive.1", 8192L)));

    assertNotEquals(before, after);
    assertTrue(ArchivalProbe.archivalRan(before, after),
        "an archival that appends changes no file name; only the length says it happened");
  }

  @Test
  void rolloverToANewArchiveFileIsDetected() {
    String before = ArchivalProbe.signature(
        legacyMetaClientListing(entry(".commits_.archive.1", 4096L)));
    String after = ArchivalProbe.signature(legacyMetaClientListing(
        entry(".commits_.archive.1", 4096L), entry(".commits_.archive.2", 64L)));

    assertTrue(ArchivalProbe.archivalRan(before, after));
  }

  /** A listing that comes back in a different order is the same archive, not a changed one. */
  @Test
  void listingOrderDoesNotAffectTheReading() {
    String oneOrder = ArchivalProbe.signature(legacyMetaClientListing(
        entry(".commits_.archive.1", 4096L), entry(".commits_.archive.2", 64L)));
    String otherOrder = ArchivalProbe.signature(legacyMetaClientListing(
        entry(".commits_.archive.2", 64L), entry(".commits_.archive.1", 4096L)));

    assertFalse(ArchivalProbe.archivalRan(oneOrder, otherOrder));
  }

  /**
   * Below version 8 the archive directory is created by the first archival, so its absence is the
   * ordinary never-archived state. It is a real answer and must compare equal to itself -- reading
   * it as unknown would make every exhaustive run on a young table fail.
   */
  @Test
  void absentLegacyArchiveIsAnAnswerAndNotAnUnknown() throws IOException {
    HoodieStorage storage = Mockito.mock(HoodieStorage.class);
    Mockito.when(storage.listDirectEntries(any(StoragePath.class)))
        .thenThrow(new FileNotFoundException("no archive yet"));
    String reading = ArchivalProbe.signature(legacyMetaClient(storage));

    assertNotEquals(ArchivalProbe.UNKNOWN, reading);
    assertFalse(ArchivalProbe.archivalRan(reading, reading));
  }

  /**
   * An archive that cannot be read is never "nothing was archived".
   *
   * <p>The governing rule of this validator: a question that could not be answered must not be
   * recorded as having been answered favourably. It costs a run, which is the intended price.
   */
  @Test
  void unreadableArchiveIsTreatedAsArchivalHavingPossiblyRun() throws IOException {
    HoodieStorage storage = Mockito.mock(HoodieStorage.class);
    Mockito.when(storage.listDirectEntries(any(StoragePath.class)))
        .thenThrow(new IOException("permission denied"));

    String reading = ArchivalProbe.signature(legacyMetaClient(storage));

    assertEquals(ArchivalProbe.UNKNOWN, reading);
    assertTrue(ArchivalProbe.archivalRan(reading, reading),
        "two unknown readings must not cancel out into agreement");
    assertTrue(ArchivalProbe.archivalRan("legacy:[]", reading));
    assertTrue(ArchivalProbe.archivalRan(reading, "legacy:[]"));
  }

  /** The two causes read very differently to an operator, so the message tells them apart. */
  @Test
  void theDescriptionSeparatesAChangedArchiveFromAnUnreadableOne() {
    assertTrue(ArchivalProbe.describe("lsm:4", "lsm:5").contains("changed from lsm:4 to lsm:5"));
    assertTrue(ArchivalProbe.describe("lsm:4", ArchivalProbe.UNKNOWN).contains("could not be read"));
    assertTrue(ArchivalProbe.describe(ArchivalProbe.UNKNOWN, "lsm:4").contains("could not be read"));
  }

  /**
   * The version-8-and-up half, which is the layout the shipping table version uses.
   *
   * <p>{@code LSMTimeline.latestSnapshotVersion} falls back to the manifest listing when the
   * {@code _version_} file cannot be read, and that fallback is what this drives: it is the branch
   * an operator's table actually takes when the version file is mid-write, and it returns the same
   * monotonic number. {@code LSMTimelineWriter.createManifestFile} computes
   * {@code newVersion = currentVersion + 1} on every archival write and every LSM compaction, so
   * any mutation of the archive moves this reading.
   */
  @Test
  void lsmSnapshotVersionMovesWhenTheArchiverWrites() {
    String before = ArchivalProbe.signature(lsmMetaClientWithManifests(4));
    String after = ArchivalProbe.signature(lsmMetaClientWithManifests(4, 5));

    assertEquals("lsm:4", before);
    assertEquals("lsm:5", after);
    assertTrue(ArchivalProbe.archivalRan(before, after));
    assertFalse(ArchivalProbe.archivalRan(before, before));
  }

  /**
   * A version-8 table's archive directory exists from table creation and is empty until the first
   * archival, so a never-archived table reads {@code -1} -- an answer, stably, not an unknown.
   */
  @Test
  void neverArchivedLsmTableIsAStableAnswer() {
    String reading = ArchivalProbe.signature(lsmMetaClientWithManifests());

    assertEquals("lsm:-1", reading);
    assertFalse(ArchivalProbe.archivalRan(reading, reading));
  }

  private static HoodieTableMetaClient lsmMetaClientWithManifests(int... versions) {
    try {
      HoodieStorage storage = Mockito.mock(HoodieStorage.class);
      // The version file is unreadable, so latestSnapshotVersion falls back to listing manifests.
      Mockito.when(storage.open(any(StoragePath.class))).thenThrow(new IOException("mid-write"));
      List<StoragePathInfo> manifests = new java.util.ArrayList<>();
      for (int version : versions) {
        manifests.add(new StoragePathInfo(
            new StoragePath(ARCHIVE, "manifest_" + version), 32L, false, (short) 0, 0L, 0L));
      }
      Mockito.when(storage.listDirectEntries(any(StoragePath.class), any())).thenReturn(manifests);
      HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
      Mockito.when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
      HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
      Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
      Mockito.when(metaClient.getStorage()).thenReturn(storage);
      Mockito.when(metaClient.getArchivePath()).thenReturn(ARCHIVE);
      Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("file:/tmp/tbl"));
      return metaClient;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static StoragePathInfo entry(String name, long length) {
    return new StoragePathInfo(new StoragePath(ARCHIVE, name), length, false, (short) 0, 0L, 0L);
  }

  private static HoodieTableMetaClient legacyMetaClientListing(StoragePathInfo... entries) {
    try {
      HoodieStorage storage = Mockito.mock(HoodieStorage.class);
      List<StoragePathInfo> listing = Arrays.asList(entries);
      Mockito.when(storage.listDirectEntries(any(StoragePath.class))).thenReturn(listing);
      return legacyMetaClient(storage);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static HoodieTableMetaClient legacyMetaClient(HoodieStorage storage) {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Mockito.when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(metaClient.getStorage()).thenReturn(storage);
    Mockito.when(metaClient.getArchivePath()).thenReturn(ARCHIVE);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("file:/tmp/tbl"));
    return metaClient;
  }

  /** Sanity: an empty archive listing is still an answer, and a stable one. */
  @Test
  void emptyLegacyArchiveIsAStableAnswer() {
    String reading = ArchivalProbe.signature(legacyMetaClientListing());

    assertNotEquals(ArchivalProbe.UNKNOWN, reading);
    assertFalse(ArchivalProbe.archivalRan(reading, reading));
    assertEquals(Collections.emptyList().toString(), reading.substring("legacy:".length()));
  }
}
