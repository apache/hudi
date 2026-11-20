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

import java.util.Set;

/**
 * The v2 (LSM, table version 8+) archived timeline has no batch instant resolver yet, and this
 * class exists to make that refusal explicit rather than to guess.
 *
 * <p><b>Why guessing is unsafe here.</b> The v1 archive is a flat set of
 * {@code .commits_.archive.N} log files that {@link ArchiveInstantSearcher} reads directly, so an
 * empty file list and a genuinely-searched-but-empty archive are distinguishable before the search
 * even starts. The v2 (LSM) archive has no equivalent: {@code LSMTimeline.latestSnapshotManifest}
 * falls back to {@code HoodieLSMTimelineManifest.EMPTY} when the snapshot version resolves to -1, so
 * a loader-based read reports "the instant is not there" and "the manifest resolved to nothing"
 * identically. Downstream, a proven absence authorizes deleting a file; reporting either of those
 * two indistinguishable states as a proven absence would be a data-loss bug, not a refusal.
 *
 * <p>Building a real LSM searcher that can tell the two apart is deliberately out of scope for this
 * whole effort. Until one exists, {@link #warmUp} throws rather than answer, and both call sites
 * gate on {@link ArchiveTimelineInstantBatchResolvers#readsV1Archive} first -- so a v2 table refuses
 * before this class is ever constructed, and this exception exists as a defense against a caller
 * that skips the gate rather than as an expected runtime path.
 *
 * @param <T> the projection each found instant carries; never produced by this implementation
 */
public final class V2ArchiveTimelineInstantBatchResolver<T> implements ArchiveTimelineInstantBatchResolver<T> {

  /**
   * Always throws. See the class Javadoc for why guessing at an answer here would be unsafe.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public ArchivedInstantWorkingSet<T> warmUp(Set<String> targetInstants) {
    // The LSM archive needs a search that can distinguish "not there" from "the manifest resolved to
    // nothing": LSMTimeline.latestSnapshotManifest falls back to HoodieLSMTimelineManifest.EMPTY when
    // the snapshot version resolves to -1, so a loader-based read reports those two identically.
    // Until a searcher exists for that layout, callers must gate on
    // ArchiveTimelineInstantBatchResolvers.readsV1Archive and refuse rather than reach this.
    throw new UnsupportedOperationException(
        "the v2 (LSM) archived timeline layout has no batch instant resolver yet; callers must gate "
            + "on ArchiveTimelineInstantBatchResolvers.readsV1Archive");
  }
}
