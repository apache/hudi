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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;

import java.util.concurrent.atomic.AtomicBoolean;

/** Proves that a data-write instant completed, including an instant already archived. */
public final class CompletedWrite {
  /** The state field of a legacy archive record; LSM records hold completed instants only and have none. */
  private static final String ARCHIVE_STATE_FIELD = "actionState";
  /** The action field of an LSM archive record. */
  private static final String ARCHIVE_ACTION_FIELD = "action";
  /** The action field of a legacy archive record. */
  private static final String LEGACY_ARCHIVE_ACTION_FIELD = "actionType";

  private CompletedWrite() {
  }

  /**
   * Whether {@code instant} is a completed commit, delta commit or replace commit of the table, in the active timeline or the
   * archive. A skipped empty commit and an instant that never completed are not.
   *
   * <p>The archive is read only for an instant absent from the reloaded active timeline. The archiver moves only completed
   * instants, and it deletes an archived instant's requested and inflight files before its completed file
   * (TimelineArchiverV2#deleteArchivedActions, TimelineArchiverV1#deleteArchivedInstants), so an instant still requested or
   * inflight in the active timeline was never archived. Such an instant is a skipped empty commit
   * (BaseHoodieWriteClient#commitStats returns before it completes the instant), a write that failed and is not rolled back
   * yet, or a write still in progress, such as a concurrent writer's; all are answered without an archive read. An instant archived anywhere, inside the active window included (archive
   * beyond savepoint leaves holes), is absent and so is found in the archive.
   */
  public static boolean exists(HoodieTableMetaClient meta, String instant) {
    HoodieTimeline commits = meta.reloadActiveTimeline().getCommitsTimeline();
    if (commits.filterCompletedInstants().containsInstant(instant)) {
      return true;
    }
    if (commits.filterInflightsAndRequested().containsInstant(instant)) {
      return false;
    }
    AtomicBoolean found = new AtomicBoolean();
    ArchivedTimelineLoader loader = meta.getTableFormat().getTimelineFactory().createArchivedTimelineLoader();
    loader.loadInstants(meta, new HoodieArchivedTimeline.ClosedClosedTimeRangeFilter(instant, instant),
        HoodieArchivedTimeline.LoadMode.ACTION, CompletedWrite::isCompletedArchiveRecord,
        (time, record) -> found.set(true), Option.of(1));
    return found.get();
  }

  /** Whether an archive record (legacy or LSM layout) is a completed data write. */
  static boolean isCompletedArchiveRecord(GenericRecord record) {
    String state = field(record, ARCHIVE_STATE_FIELD);
    if (state != null && !HoodieInstant.State.COMPLETED.name().equals(state)) {
      return false;
    }
    String action = field(record, ARCHIVE_ACTION_FIELD);
    if (action == null) {
      action = field(record, LEGACY_ARCHIVE_ACTION_FIELD);
    }
    return HoodieTimeline.COMMIT_ACTION.equals(action) || HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)
        || HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action);
  }

  /** The string value of {@code name} in {@code record}, or null when its schema has no such field or the value is null. */
  private static String field(GenericRecord record, String name) {
    if (record.getSchema().getField(name) == null) {
      return null;
    }
    Object value = record.get(name);
    return value == null ? null : value.toString();
  }
}
