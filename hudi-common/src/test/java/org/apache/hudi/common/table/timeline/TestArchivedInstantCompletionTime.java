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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.function.BooleanSupplier;

import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.ACTION_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.COMPLETION_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.INSTANT_TIME_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.METADATA_ARCHIVED_META_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The {@code completionTime} field of {@code HoodieLSMTimelineInstant} is declared
 * {@code ["null","string"]} with a null default, and carries no value for instants archived before the
 * field existed. Every read of it therefore has to be null-safe; dereferencing it produced
 * {@code NullPointerException: Cannot invoke "Object.toString()" because the return value of
 * "GenericRecord.get(String)" is null} on a table upgraded from 0.x (HUDI-9655).
 *
 * <p>This covers the two readers that build a completed {@link HoodieInstant} from such a record, both of
 * which fall back to the instant time.
 */
class TestArchivedInstantCompletionTime {

  private static final String INSTANT_TIME = "00000001";

  @Test
  void completionTimeFallsBackToTheInstantTimeWhenAbsent() {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    // completionTime deliberately left unset, as it is for an instant archived before the field existed

    assertEquals(INSTANT_TIME, ArchivedTimelineV2.completionTimeOrInstantTime(record, INSTANT_TIME),
        "An archived instant without a completion time should fall back to its instant time");
  }

  @Test
  void completionTimeIsUsedWhenPresent() {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put(COMPLETION_TIME_ARCHIVED_META_FIELD, "00001001");

    assertEquals("00001001", ArchivedTimelineV2.completionTimeOrInstantTime(record, INSTANT_TIME),
        "A present completion time should be used as-is");
  }

  /**
   * The same field read on the way to a {@code HoodieArchivedMetaEntry}, which is the path a CLI or
   * metadata-conversion caller takes rather than the query view.
   */
  @Test
  void createMetaWrapperFallsBackToTheInstantTimeWhenCompletionTimeIsAbsent() throws IOException {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put(INSTANT_TIME_ARCHIVED_META_FIELD, INSTANT_TIME);
    record.put(ACTION_ARCHIVED_META_FIELD, HoodieTimeline.COMMIT_ACTION);
    record.put(METADATA_ARCHIVED_META_FIELD, ByteBuffer.wrap(new byte[0]));
    // completionTime deliberately left unset

    HoodieArchivedMetaEntry entry =
        MetadataConversionUtils.createMetaWrapper(mockMetaClientReturningEmptyCommitMetadata(), record);

    assertEquals(INSTANT_TIME, entry.getStateTransitionTime(),
        "The archived entry should carry the instant time when the record has no completion time");
    assertEquals(INSTANT_TIME, entry.getCommitTime());
  }

  private static HoodieTableMetaClient mockMetaClientReturningEmptyCommitMetadata() throws IOException {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
    when(metaClient.getInstantGenerator()).thenReturn(new InstantGeneratorV2());

    CommitMetadataSerDe serDe = mock(CommitMetadataSerDe.class);
    when(serDe.<HoodieCommitMetadata>deserialize(any(HoodieInstant.class), any(InputStream.class),
        any(BooleanSupplier.class), eq(HoodieCommitMetadata.class)))
        .thenReturn(new HoodieCommitMetadata());
    when(metaClient.getCommitMetadataSerDe()).thenReturn(serDe);
    return metaClient;
  }
}
