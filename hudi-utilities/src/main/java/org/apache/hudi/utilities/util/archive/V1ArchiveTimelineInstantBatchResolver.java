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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.BranchBeacon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a whole batch of instants against a table's v1 (pre table version 8) archived timeline in
 * ONE pass: one directory check, one archive-file listing, and one multi-threaded walk over the
 * archive files, however many instants are asked for.
 *
 * <p><b>The two directory checks run before the search, and that order is load-bearing.</b>
 * {@link ArchiveInstantSearcher#search} returns a clean {@link InstantSearchState#NOT_FOUND} for
 * every target when its file list is empty -- that is the correct answer when the archive genuinely
 * holds nothing, but it would also be the answer for a directory full of entries that are not archive
 * files, which the searcher never gets to see. Reading that as absence would let a proven absence
 * come from content this class never read, and downstream a proven absence licenses a delete.
 *
 * @param <T> the projection each found instant carries
 */
public final class V1ArchiveTimelineInstantBatchResolver<T> implements ArchiveTimelineInstantBatchResolver<T> {

  private static final Logger LOG = LoggerFactory.getLogger(V1ArchiveTimelineInstantBatchResolver.class);

  private final ArchiveInstantSearcher searcher;
  private final ArchiveFileCatalog catalog;
  private final HoodieStorage storage;
  private final StoragePath archivePath;
  private final ArchivedRecordProjector<T> projector;
  private final String basePathForLogging;

  public V1ArchiveTimelineInstantBatchResolver(HoodieTableMetaClient metaClient,
                                               ArchivedRecordProjector<T> projector) {
    this(new ArchiveInstantSearcher(new HoodieArchiveFileCatalog(metaClient),
            new HoodieArchiveFileScanner(metaClient)),
        new HoodieArchiveFileCatalog(metaClient),
        metaClient.getStorage(),
        metaClient.getArchivePath(),
        projector,
        safeBasePath(metaClient));
  }

  @VisibleForTesting
  V1ArchiveTimelineInstantBatchResolver(ArchiveInstantSearcher searcher, ArchiveFileCatalog catalog,
                                        HoodieStorage storage, StoragePath archivePath,
                                        ArchivedRecordProjector<T> projector) {
    this(searcher, catalog, storage, archivePath, projector, String.valueOf(archivePath));
  }

  private V1ArchiveTimelineInstantBatchResolver(ArchiveInstantSearcher searcher, ArchiveFileCatalog catalog,
                                                HoodieStorage storage, StoragePath archivePath,
                                                ArchivedRecordProjector<T> projector,
                                                String basePathForLogging) {
    this.searcher = Objects.requireNonNull(searcher, "searcher");
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.storage = Objects.requireNonNull(storage, "storage");
    this.archivePath = Objects.requireNonNull(archivePath, "archivePath");
    this.projector = Objects.requireNonNull(projector, "projector");
    this.basePathForLogging = basePathForLogging;
  }

  @Override
  public ArchivedInstantWorkingSet<T> warmUp(Set<String> targetInstants) {
    if (targetInstants == null || targetInstants.isEmpty()) {
      return ArchivedInstantWorkingSet.<T>builder(Collections.<String>emptySet()).build();
    }
    // Both directory checks run BEFORE the search. ArchiveInstantSearcher returns a clean NOT_FOUND
    // for an empty file list, so a directory full of non-archive entries would otherwise become a
    // false absence -- and downstream an absence licenses a delete.
    Option<ArchivedInstantWorkingSet<T>> shortCircuit = decideFromArchiveDirectory(targetInstants);
    if (shortCircuit.isPresent()) {
      return shortCircuit.get();
    }

    // Concurrent: the searcher calls onHit on worker threads. computeIfAbsent + merge under the
    // map's per-key lock is what makes ArchivedRecordProjector.merge race-free for one instant.
    Map<String, T> projected = new ConcurrentHashMap<>();
    Set<String> undecodable = ConcurrentHashMap.newKeySet();

    Map<String, InstantLocation> located;
    try {
      located = searcher.search(targetInstants, (instantTime, record) -> {
        try {
          T value = projector.project(record);
          projected.merge(instantTime, value, projector::merge);
        } catch (IOException | RuntimeException e) {
          // The record arrived but could not be understood. The instant demonstrably EXISTS, so this
          // is "I could not read it", never "it records nothing here" -- which would read as absence.
          LOG.warn("Could not project the archived record of instant {}; reporting it unanswered",
              instantTime, e);
          BranchBeacon.hit(Branch.V1ABR_UNDECODABLE);
          undecodable.add(instantTime);
        }
      });
    } catch (Exception e) {
      LOG.warn("The archived search over {} instant(s) of {} failed; every one of them is reported "
          + "unanswered", targetInstants.size(), basePathForLogging, e);
      BranchBeacon.hit(Branch.V1ABR_SEARCH_FAILED);
      return ArchivedInstantWorkingSet.allIndeterminate(targetInstants);
    }

    ArchivedInstantWorkingSet.Builder<T> builder = ArchivedInstantWorkingSet.builder(targetInstants);
    for (String instantTime : targetInstants) {
      if (undecodable.contains(instantTime)) {
        builder.indeterminate(instantTime);
        continue;
      }
      InstantLocation location = located.get(instantTime);
      InstantSearchState state = location == null ? InstantSearchState.INDETERMINATE : location.state();
      switch (state) {
        case FOUND:
          T value = projected.get(instantTime);
          // FOUND with nothing projected means the searcher located the instant but the projector
          // was never handed a record for it. That is not "the commit names nothing" -- it is a gap
          // between the two, so it refuses.
          if (value == null) {
            BranchBeacon.hit(Branch.V1ABR_UNDECODABLE);
            builder.indeterminate(instantTime);
          } else {
            BranchBeacon.hit(Branch.V1ABR_FOUND);
            builder.found(instantTime, value);
          }
          break;
        case NOT_FOUND:
          BranchBeacon.hit(Branch.V1ABR_NOT_FOUND);
          builder.notFound(instantTime);
          break;
        default:
          BranchBeacon.hit(Branch.V1ABR_INDETERMINATE);
          builder.indeterminate(instantTime);
          break;
      }
    }
    return builder.build();
  }

  /**
   * Two shortcuts, with their reasoning intact, for deciding whether an archived absence can be
   * trusted from the archive directory's shape alone, without a search.
   *
   * @return {@link Option#empty()} when neither shortcut applies and the caller must search
   */
  private Option<ArchivedInstantWorkingSet<T>> decideFromArchiveDirectory(Set<String> targets) {
    try {
      // Absent or empty is the never-archived state on this layout, which creates the directory at
      // first archival. It vouches: refusing here would make the tool useless on exactly the young
      // tables it has least to do on.
      if (!storage.exists(archivePath) || storage.listDirectEntries(archivePath).isEmpty()) {
        BranchBeacon.hit(Branch.V1ABR_NO_ARCHIVE_DIRECTORY);
        return Option.of(ArchivedInstantWorkingSet.<T>allNotFound(targets));
      }
      // Entries but no archive file among them refuses: the searcher would find an empty catalogue
      // and infer a clean absence from content it never read.
      if (catalog.list().isEmpty()) {
        BranchBeacon.hit(Branch.V1ABR_NO_ARCHIVE_FILES);
        return Option.of(ArchivedInstantWorkingSet.<T>allIndeterminate(targets));
      }
      return Option.empty();
    } catch (Exception e) {
      LOG.warn("Could not establish the archive state of {}; every instant is reported unanswered",
          basePathForLogging, e);
      BranchBeacon.hit(Branch.V1ABR_CATALOGUE_UNREADABLE);
      return Option.of(ArchivedInstantWorkingSet.<T>allIndeterminate(targets));
    }
  }

  private static String safeBasePath(HoodieTableMetaClient metaClient) {
    try {
      return String.valueOf(metaClient.getBasePath());
    } catch (Exception e) {
      return "<unknown>";
    }
  }
}
