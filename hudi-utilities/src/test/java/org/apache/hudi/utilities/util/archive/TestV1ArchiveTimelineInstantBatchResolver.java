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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.utilities.util.archive.FakeArchive.Fixture;
import static org.apache.hudi.utilities.util.archive.FakeArchive.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How {@link V1ArchiveTimelineInstantBatchResolver} resolves a whole batch of instants in one v1
 * archived-timeline pass: what it can vouch for from the archive directory's shape alone, and what
 * it must search for, and how a decode failure or search failure degrades only what it must.
 */
class TestV1ArchiveTimelineInstantBatchResolver {

  private static final StoragePath ARCHIVE_PATH = new StoragePath("/tmp/table/.hoodie/archive");

  /**
   * Builds a resolver directly from a prepared searcher plus the storage/archive-path inputs
   * {@code decideFromArchiveDirectory} needs, bypassing a real table.
   */
  private static <T> V1ArchiveTimelineInstantBatchResolver<T> resolver(
      HoodieStorage storage, ArchiveFileCatalog catalog, ArchiveInstantSearcher searcher,
      ArchivedRecordProjector<T> projector) {
    return new V1ArchiveTimelineInstantBatchResolver<>(searcher, catalog, storage, ARCHIVE_PATH, projector);
  }

  private static HoodieStorage storageWithDirectory(boolean exists, StoragePathInfo... entries)
      throws IOException {
    HoodieStorage storage = Mockito.mock(HoodieStorage.class);
    Mockito.when(storage.exists(ARCHIVE_PATH)).thenReturn(exists);
    Mockito.when(storage.listDirectEntries(ARCHIVE_PATH))
        .thenReturn(Arrays.asList(entries));
    return storage;
  }

  private static StoragePathInfo nonArchiveEntry(String name) {
    return new StoragePathInfo(new StoragePath(ARCHIVE_PATH, name), 10L, false, (short) 1, 1L, 0L);
  }

  private static ArchiveInstantSearcher searcherOver(Fixture fixture) {
    return new ArchiveInstantSearcher(fixture, fixture, 1, 8L * FakeArchive.GB);
  }

  /** Records the commit time the fixture's single-field schema carries, projecting into a Set. */
  private static final class StubProjector implements ArchivedRecordProjector<Set<String>> {

    private final Set<String> failFor;

    StubProjector() {
      this(Collections.<String>emptySet());
    }

    StubProjector(Set<String> failFor) {
      this.failFor = failFor;
    }

    @Override
    public Set<String> project(GenericRecord record) throws IOException {
      String instant = record.get(HoodieTableMetaClient.COMMIT_TIME_KEY).toString();
      if (failFor.contains(instant)) {
        throw new IOException("stub decode failure for " + instant);
      }
      return Collections.singleton("named-by-" + instant);
    }

    @Override
    public Set<String> merge(Set<String> first, Set<String> second) {
      Set<String> merged = new LinkedHashSet<>(first);
      merged.addAll(second);
      return merged;
    }
  }

  /**
   * Projects a DIFFERENT value on every invocation, unlike {@link StubProjector}. Only a projector
   * whose repeat calls are distinguishable can prove that {@code merge} combined every hit rather
   * than the map silently keeping just one of them.
   */
  private static final class VaryingStubProjector implements ArchivedRecordProjector<Set<String>> {

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public Set<String> project(GenericRecord record) throws IOException {
      String instant = record.get(HoodieTableMetaClient.COMMIT_TIME_KEY).toString();
      return Collections.singleton("named-by-" + instant + "-" + counter.incrementAndGet());
    }

    @Override
    public Set<String> merge(Set<String> first, Set<String> second) {
      Set<String> merged = new LinkedHashSet<>(first);
      merged.addAll(second);
      return merged;
    }
  }

  @Test
  void anEmptyTargetSetReadsNothing() throws IOException {
    HoodieStorage storage = Mockito.mock(HoodieStorage.class);
    ArchiveFileCatalog catalog = Mockito.mock(ArchiveFileCatalog.class);
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector())
            .warmUp(Collections.<String>emptySet());

    assertTrue(resolved.targets().isEmpty());
    Mockito.verifyNoInteractions(storage);
    Mockito.verifyNoInteractions(catalog);
  }

  @Test
  void anAbsentArchiveDirectoryVouchesForEveryAbsence() throws IOException {
    HoodieStorage storage = storageWithDirectory(false);
    ArchiveFileCatalog catalog = Mockito.mock(ArchiveFileCatalog.class);
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));
    Set<String> targets = new HashSet<>(Arrays.asList("100", "200"));

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector()).warmUp(targets);

    assertEquals(InstantSearchState.NOT_FOUND, resolved.stateOf("100"));
    assertEquals(InstantSearchState.NOT_FOUND, resolved.stateOf("200"));
    assertEquals(0, fixture.probeCount("f0"), "an absent directory must never trigger a search");
    Mockito.verifyNoInteractions(catalog);
  }

  @Test
  void anEmptyArchiveDirectoryVouchesForEveryAbsence() throws IOException {
    HoodieStorage storage = storageWithDirectory(true);
    ArchiveFileCatalog catalog = Mockito.mock(ArchiveFileCatalog.class);
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));
    Set<String> targets = Collections.singleton("100");

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector()).warmUp(targets);

    assertEquals(InstantSearchState.NOT_FOUND, resolved.stateOf("100"));
    assertEquals(0, fixture.probeCount("f0"));
    Mockito.verifyNoInteractions(catalog);
  }

  @Test
  void directoryHoldingNoArchiveFileRefuses() throws IOException {
    // The directory exists and holds an entry, but that entry is not an archive file: the
    // catalogue built from the SAME storage would find nothing to list. If the two directory
    // checks ran after search(), ArchiveInstantSearcher's empty-file-list path would report this
    // as a clean NOT_FOUND -- a false absence from content nobody ever read. This test fails under
    // that reordering and passes only when the pre-search check catches it first.
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("README.txt"));
    ArchiveFileCatalog catalog = Mockito.mock(ArchiveFileCatalog.class);
    Mockito.when(catalog.list()).thenReturn(Collections.<ArchiveFile>emptyList());
    // A searcher whose own catalog is also empty, mirroring what a real HoodieArchiveFileCatalog
    // would report for this same directory -- so a NOT_FOUND here can only come from the reorder bug.
    Fixture emptyFixture = new Fixture();

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(emptyFixture), new StubProjector())
            .warmUp(Collections.singleton("100"));

    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("100"));
  }

  @Test
  void anUnlistableCatalogueRefuses() throws IOException {
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("some-entry"));
    ArchiveFileCatalog catalog = Mockito.mock(ArchiveFileCatalog.class);
    Mockito.when(catalog.list()).thenThrow(new IOException("cannot list archive"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector())
            .warmUp(Collections.singleton("100"));

    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("100"));
    assertEquals(0, fixture.probeCount("f0"), "an unreadable catalogue must never trigger a search");
  }

  @Test
  void searchThatThrewRefusesEveryTarget() throws IOException {
    // The pre-search directory checks must both pass -- the resolver's OWN catalog reference below
    // returns the fixture's raw file list, bypassing failingList() entirely -- so the failure can
    // only be observed once warmUp reaches searcher.search(...), whose SEARCHER-internal catalog
    // (the same fixture, wired through failingList()) throws when the searcher calls catalog.list().
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"))
        .failingList(new IOException("archive directory is gone"));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();
    Set<String> targets = new HashSet<>(Arrays.asList("100", "200"));

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector()).warmUp(targets);

    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("100"));
    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("200"));
  }

  @Test
  void foundInstantCarriesItsProjection() throws IOException {
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector())
            .warmUp(Collections.singleton("100"));

    assertEquals(InstantSearchState.FOUND, resolved.stateOf("100"));
    assertEquals(Collections.singleton("named-by-100"), resolved.contentOf("100").get());
  }

  @Test
  void projectorThatThrewMakesOnlyItsOwnInstantIndeterminate() throws IOException {
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100", "200"));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();
    StubProjector projector = new StubProjector(Collections.singleton("100"));
    Set<String> targets = new HashSet<>(Arrays.asList("100", "200"));

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), projector).warmUp(targets);

    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("100"));
    assertEquals(InstantSearchState.FOUND, resolved.stateOf("200"));
    assertEquals(Collections.singleton("named-by-200"), resolved.contentOf("200").get());
  }

  @Test
  void cleanlyAbsentInstantIsNotFound() throws IOException {
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("100"));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector())
            .warmUp(Collections.singleton("999"));

    assertEquals(InstantSearchState.NOT_FOUND, resolved.stateOf("999"));
  }

  @Test
  void damagedRangeMakesItsInstantIndeterminate() throws IOException {
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(
        spec("damaged", FakeArchive.GB).holding("10", "30").damaged("a corrupt block"));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new StubProjector())
            .warmUp(Collections.singleton("20"));

    assertEquals(InstantSearchState.INDETERMINATE, resolved.stateOf("20"));
  }

  @Test
  void severalRecordsOfOneInstantAreMerged() throws IOException {
    // VaryingStubProjector, not StubProjector: its three hits project three DISTINCT values, so the
    // final content can only hold all three if every hit went through projector.merge. A resolver
    // that replaced projected.merge(...) with a plain put(...), or dropped two of the three hits,
    // would leave content of size 1 here -- this is what makes the assertion below load-bearing.
    HoodieStorage storage = storageWithDirectory(true, nonArchiveEntry("marker"));
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holdingRepeated("200", 3));
    ArchiveFileCatalog catalog = () -> fixture.archiveFiles();

    ArchivedInstantWorkingSet<Set<String>> resolved =
        resolver(storage, catalog, searcherOver(fixture), new VaryingStubProjector())
            .warmUp(Collections.singleton("200"));

    assertEquals(InstantSearchState.FOUND, resolved.stateOf("200"));
    assertEquals(3, resolved.contentOf("200").get().size(),
        "all three records of the instant must be merged, not overwritten by the last one");
  }
}
