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

package org.apache.hudi.client;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestWriteStatus {

  @Test
  public void testFailureFraction() {
    WriteStatus status = new WriteStatus(true, 0.1);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markFailure(mock(HoodieRecord.class), t, null);
    }
    assertTrue(status.getFailedRecords().size() > 0);
    assertTrue(status.getFailedRecords().size() < 150); // 150 instead of 100, to prevent flaky test
    assertTrue(status.hasErrors());
  }

  @Test
  public void testSuccessRecordTracking() {
    WriteStatus status = new WriteStatus(false, 1.0);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(mock(HoodieRecord.class), Option.empty());
      status.markFailure(mock(HoodieRecord.class), t, Option.empty());
    }
    assertEquals(1000, status.getFailedRecords().size());
    assertTrue(status.hasErrors());
    assertTrue(status.getIndexStats().getWrittenRecordDelegates().isEmpty());
    assertEquals(2000, status.getTotalRecords());
  }

  @Test
  public void testSuccessWithEventTime() {
    // test with empty eventTime
    WriteStatus status = new WriteStatus(false, 1.0);
    status.setStat(new HoodieWriteStat());
    for (int i = 0; i < 1000; i++) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY, "");
      status.markSuccess(mock(HoodieRecord.class), Option.of(metadata));
    }
    assertEquals(1000, status.getTotalRecords());
    assertFalse(status.hasErrors());
    assertNull(status.getStat().getMaxEventTime());
    assertNull(status.getStat().getMinEventTime());

    // test with null eventTime
    status = new WriteStatus(false, 1.0);
    status.setStat(new HoodieWriteStat());
    for (int i = 0; i < 1000; i++) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY, null);
      status.markSuccess(mock(HoodieRecord.class), Option.of(metadata));
    }
    assertEquals(1000, status.getTotalRecords());
    assertFalse(status.hasErrors());
    assertNull(status.getStat().getMaxEventTime());
    assertNull(status.getStat().getMinEventTime());

    // test with seconds eventTime
    status = new WriteStatus(false, 1.0);
    status.setStat(new HoodieWriteStat());
    long minSeconds = 0L;
    long maxSeconds = 0L;
    for (int i = 0; i < 1000; i++) {
      Map<String, String> metadata = new HashMap<>();
      long eventTime = System.currentTimeMillis() / 1000;
      if (i == 0) {
        minSeconds = eventTime;
      } else if (i == 999) {
        maxSeconds = eventTime;
      }
      metadata.put(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY, String.valueOf(eventTime));
      status.markSuccess(mock(HoodieRecord.class), Option.of(metadata));
    }
    assertEquals(1000, status.getTotalRecords());
    assertFalse(status.hasErrors());
    assertEquals(maxSeconds * 1000L, status.getStat().getMaxEventTime());
    assertEquals(minSeconds * 1000L, status.getStat().getMinEventTime());

    // test with millis eventTime
    status = new WriteStatus(false, 1.0);
    status.setStat(new HoodieWriteStat());
    minSeconds = 0L;
    maxSeconds = 0L;
    for (int i = 0; i < 1000; i++) {
      Map<String, String> metadata = new HashMap<>();
      long eventTime = System.currentTimeMillis();
      if (i == 0) {
        minSeconds = eventTime;
      } else if (i == 999) {
        maxSeconds = eventTime;
      }
      metadata.put(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY, String.valueOf(eventTime));
      status.markSuccess(mock(HoodieRecord.class), Option.of(metadata));
    }
    assertEquals(1000, status.getTotalRecords());
    assertFalse(status.hasErrors());
    assertEquals(maxSeconds, status.getStat().getMaxEventTime());
    assertEquals(minSeconds, status.getStat().getMinEventTime());

    // test with error format eventTime
    status = new WriteStatus(false, 1.0);
    status.setStat(new HoodieWriteStat());
    for (int i = 0; i < 1000; i++) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY, String.valueOf(i));
      status.markSuccess(mock(HoodieRecord.class), Option.of(metadata));
    }
    assertEquals(1000, status.getTotalRecords());
    assertFalse(status.hasErrors());
    assertNull(status.getStat().getMaxEventTime());
    assertNull(status.getStat().getMinEventTime());

  }

  @Test
  public void testFailureFractionExtended() {
    WriteStatus status = new WriteStatus(true, 0.1);
    String fileId = UUID.randomUUID().toString();
    String partitionPath = UUID.randomUUID().toString();
    status.setFileId(fileId);
    status.setPartitionPath(partitionPath);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markFailure(mock(HoodieRecord.class), t, Option.empty());
    }
    // verification
    assertEquals(fileId, status.getFileId());
    assertEquals(partitionPath, status.getPartitionPath());
    assertTrue(status.getFailedRecords().size() > 0);
    assertTrue(status.getFailedRecords().size() < 150); // 150 instead of 100, to prevent flaky test
    assertTrue(status.hasErrors());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSuccessRecordTrackingExtended(boolean trackSuccess) {
    WriteStatus status = new WriteStatus(trackSuccess, 1.0);
    String fileId = UUID.randomUUID().toString();
    status.setFileId(fileId);
    String partitionPath = UUID.randomUUID().toString();
    status.setPartitionPath(partitionPath);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(mock(HoodieRecord.class), Option.empty());
      status.markFailure(mock(HoodieRecord.class), t, Option.empty());
    }
    // verification
    assertEquals(fileId, status.getFileId());
    assertEquals(partitionPath, status.getPartitionPath());
    assertEquals(1000, status.getFailedRecords().size());
    assertTrue(status.hasErrors());
    if (trackSuccess) {
      assertEquals(1000, status.getIndexStats().getWrittenRecordDelegates().size());
    } else {
      assertTrue(status.getIndexStats().getWrittenRecordDelegates().isEmpty());
    }
    assertEquals(2000, status.getTotalRecords());
  }

  @Test
  public void testGlobalError() {
    WriteStatus status = new WriteStatus(true, 0.1);
    Throwable t = new Exception("some error in writing");
    status.setGlobalError(t);
    assertEquals(t, status.getGlobalError());
  }

  @Test
  public void testRemoveMetadataStats() {
    WriteStatus status = new WriteStatus(true, 0.1);
    status.markSuccess(HoodieRecordDelegate.create(new HoodieKey("key", "partition")), Option.empty());
    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = new HashMap<>();
    stats.put("field1", HoodieColumnRangeMetadata.<Comparable>create("f1", "field1", 1, 2, 0, 2, 5, 10, HoodieColumnRangeMetadata.NoneMetadata.INSTANCE));
    status.setStat(new HoodieWriteStat());
    status.getStat().putRecordsStats(stats);
    assertEquals(1, status.getIndexStats().getWrittenRecordDelegates().size());
    assertEquals(1, status.getStat().getColumnStats().get().size());

    // Remove metadata stats
    status.removeMetadataStats();
    assertNull(status.getIndexStats());
  }
}
