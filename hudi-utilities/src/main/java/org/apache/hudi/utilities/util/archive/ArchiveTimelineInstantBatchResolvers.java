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
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Picks the {@link ArchiveTimelineInstantBatchResolver} for a table by its archived timeline
 * layout, and answers the question both dimensions of the validator ask before building one at
 * all: can this table's archive be read.
 *
 * <p>Every caller that needs an archived-timeline read is expected to call
 * {@link #readsV1Archive} first and refuse when it is {@code false}, rather than call
 * {@link #forTable} and rely on {@link V2ArchiveTimelineInstantBatchResolver#warmUp} to throw. The
 * throw exists as a defense for a caller that skips the gate, not as the intended refusal path.
 */
public final class ArchiveTimelineInstantBatchResolvers {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTimelineInstantBatchResolvers.class);

  private ArchiveTimelineInstantBatchResolvers() {
  }

  /**
   * @return the resolver for {@code metaClient}'s archived timeline layout: v1 (pre table version 8)
   *         tables get {@link V1ArchiveTimelineInstantBatchResolver}, everything else gets
   *         {@link V2ArchiveTimelineInstantBatchResolver}, whose {@code warmUp} throws. Callers
   *         should check {@link #readsV1Archive} before calling this rather than rely on that throw.
   */
  public static <T> ArchiveTimelineInstantBatchResolver<T> forTable(HoodieTableMetaClient metaClient,
                                                                     ArchivedRecordProjector<T> projector) {
    if (readsV1Archive(metaClient)) {
      return new V1ArchiveTimelineInstantBatchResolver<>(metaClient, projector);
    }
    return new V2ArchiveTimelineInstantBatchResolver<>();
  }

  /**
   * Whether {@code metaClient}'s archived timeline is on the v1 (pre table version 8) layout that
   * {@link V1ArchiveTimelineInstantBatchResolver} can read.
   *
   * <p>Keyed on {@link TimelineLayoutVersion} rather than on a table-version number, because that is
   * what {@code TimelineLayout} itself switches on when handing out {@code ArchivedTimelineLoaderV1}
   * versus {@code ArchivedTimelineLoaderV2} -- so a table version added later lands on the correct
   * side on its own.
   *
   * @return {@code false}, not a guess, when the layout version cannot be read: not knowing the
   *         layout means not knowing whether the archive can be read at all, which is a reason to
   *         refuse rather than to search the wrong thing.
   */
  public static boolean readsV1Archive(HoodieTableMetaClient metaClient) {
    try {
      return metaClient.getTableConfig().getTableVersion().getTimelineLayoutVersion()
          .compareTo(TimelineLayoutVersion.LAYOUT_VERSION_2) < 0;
    } catch (Exception ex) {
      LOG.warn("Could not read the timeline layout version of {}; the archived search is disabled "
          + "and every out-of-window absence stays unproven", safeBasePath(metaClient), ex);
      return false;
    }
  }

  private static String safeBasePath(HoodieTableMetaClient metaClient) {
    try {
      return String.valueOf(metaClient.getBasePath());
    } catch (Exception ex) {
      return "<unknown>";
    }
  }
}
