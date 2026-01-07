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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface ArchivedTimelineLoader extends Serializable {

  /**
   * Loads the instants from the timeline.
   *
   * @param metaClient          The meta client.
   * @param fileTimeRangeFilter The time range filter where the target instant belongs to. This will only be applied at the file level.
   *                            If strict filtering is required for the records, the commitsFilter should be used as well.
   * @param loadMode            The load mode.
   * @param commitsFilter       Filter of the instant type.
   * @param recordConsumer      Consumer of the instant record payload.
   *
   * @return The last instant time loaded or empty if no instant is loaded.
   */
  Option<String> loadInstants(
      HoodieTableMetaClient metaClient,
      @Nullable HoodieArchivedTimeline.TimeRangeFilter fileTimeRangeFilter,
      HoodieArchivedTimeline.LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter,
      BiConsumer<String, GenericRecord> recordConsumer);

  /**
   * Loads the instants from the timeline with optional limit for early termination.
   *
   * @param metaClient          The meta client.
   * @param fileTimeRangeFilter The time range filter where the target instant belongs to. This will only be applied at the file level.
   *                            If strict filtering is required for the records, the commitsFilter should be used as well.
   * @param loadMode            The load mode.
   * @param commitsFilter       Filter of the instant type.
   * @param recordConsumer      Consumer of the instant record payload.
   * @param limit               Optional maximum number of instants to load. Empty for no limit.
   *
   * @return the last instant time loaded or empty if no instant is loaded.
   */
  default Option<String> loadInstants(
      HoodieTableMetaClient metaClient,
      @Nullable HoodieArchivedTimeline.TimeRangeFilter fileTimeRangeFilter,
      HoodieArchivedTimeline.LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter,
      BiConsumer<String, GenericRecord> recordConsumer,
      Option<Integer> limit) {
    // Default implementation calls the method without limit for backward compatibility
    return loadInstants(metaClient, fileTimeRangeFilter, loadMode, commitsFilter, recordConsumer);
  }
}
