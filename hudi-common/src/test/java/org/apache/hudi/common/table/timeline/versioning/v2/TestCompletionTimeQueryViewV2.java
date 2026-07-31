/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.ACTION_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.COMPLETION_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.INSTANT_TIME_ARCHIVED_META_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCompletionTimeQueryViewV2 {

  /**
   * The {@code completionTime} field of {@code HoodieLSMTimelineInstant} is declared
   * {@code ["null","string"]} with a null default, and instants archived before the field existed carry
   * no value for it. Reading such an instant must fall back to the instant time, as
   * {@code setCompletionTime} already documents, rather than throwing.
   *
   * <p>See HUDI-9655: upgrading a table written by 0.x produced
   * {@code NullPointerException: Cannot invoke "Object.toString()" because the return value of
   * "org.apache.avro.generic.GenericRecord.get(String)" is null} from this path.
   */
  @Test
  void readCompletionTimeFallsBackWhenTheArchivedRecordHasNoCompletionTime() {
    try (CompletionTimeQueryViewV2 view = new CompletionTimeQueryViewV2(mockMetaClientWithEmptyTimeline())) {
      GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
      record.put(INSTANT_TIME_ARCHIVED_META_FIELD, "00000001");
      record.put(ACTION_ARCHIVED_META_FIELD, "commit");
      // completionTime deliberately left unset

      view.readCompletionTime("00000001", record);

      assertEquals(Option.of("00000001"), view.getCompletionTime("00000001"),
          "An archived instant without a completion time should fall back to its instant time");
    }
  }

  @Test
  void readCompletionTimeUsesTheArchivedCompletionTimeWhenPresent() {
    try (CompletionTimeQueryViewV2 view = new CompletionTimeQueryViewV2(mockMetaClientWithEmptyTimeline())) {
      GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
      record.put(INSTANT_TIME_ARCHIVED_META_FIELD, "00000001");
      record.put(ACTION_ARCHIVED_META_FIELD, "commit");
      record.put(COMPLETION_TIME_ARCHIVED_META_FIELD, "00001001");

      view.readCompletionTime("00000001", record);

      assertEquals(Option.of("00001001"), view.getCompletionTime("00000001"));
    }
  }

  private static HoodieTableMetaClient mockMetaClientWithEmptyTimeline() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline writeTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.firstInstant()).thenReturn(Option.empty());
    when(activeTimeline.getWriteTimeline()).thenReturn(writeTimeline);
    when(writeTimeline.getFirstNonSavepointCommit()).thenReturn(Option.empty());
    when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.empty());
    return metaClient;
  }
}
