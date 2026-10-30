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

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.model.HoodieLSMTimelineManifest;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineLoaderV2;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestArchivedCleanRangePushdown {
  @TempDir
  java.nio.file.Path tempDir;

  @ParameterizedTest
  @ValueSource(ints = {1, 32})
  void testWindowsOnlyVisitInRangeRecords(int batchSize) throws IOException {
    StoragePath archivePath = new StoragePath(tempDir.toUri());
    String fileName = "001_320_0.parquet";
    Schema schema = HoodieLSMTimelineInstant.getClassSchema();
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
        new Path(new StoragePath(archivePath, fileName).toUri())).withSchema(schema).withConf(new Configuration()).build()) {
      for (int i = 1; i <= 320; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("instantTime", time(i));
        record.put("completionTime", time(i));
        record.put("action", HoodieTimeline.CLEAN_ACTION);
        record.put("version", 1);
        record.put("metadata", ByteBuffer.wrap(new byte[] {(byte) i}));
        record.put("plan", ByteBuffer.wrap(new byte[] {(byte) i}));
        writer.write(record);
      }
    }
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getArchivePath()).thenReturn(archivePath);
    when(metaClient.getStorage()).thenReturn(HoodieTestUtils.getStorage(archivePath));
    HoodieLSMTimelineManifest manifest = new HoodieLSMTimelineManifest();
    manifest.addFile(fileName, 0);
    AtomicInteger visited = new AtomicInteger();
    ArchivedTimelineLoaderV2 loader = new ArchivedTimelineLoaderV2() {
      @Override
      protected ClosableIterator<IndexedRecord> getArchivedInstantIterator(HoodieTableMetaClient client, String file,
                                                                          Schema readSchema, HoodieArchivedTimeline.TimeRangeFilter filter) throws IOException {
        ClosableIterator<IndexedRecord> delegate = super.getArchivedInstantIterator(client, file, readSchema, filter);
        return new ClosableIterator<IndexedRecord>() {
          @Override
          public boolean hasNext() {
            return delegate.hasNext();
          }

          @Override
          public IndexedRecord next() {
            visited.incrementAndGet();
            return delegate.next();
          }

          @Override
          public void close() {
            delegate.close();
          }
        };
      }
    };
    try (MockedStatic<LSMTimeline> lsm = mockStatic(LSMTimeline.class, CALLS_REAL_METHODS)) {
      lsm.when(() -> LSMTimeline.latestSnapshotManifest(metaClient, archivePath)).thenReturn(manifest);
      for (HoodieArchivedTimeline.LoadMode mode : new HoodieArchivedTimeline.LoadMode[] {
          HoodieArchivedTimeline.LoadMode.PLAN, HoodieArchivedTimeline.LoadMode.METADATA}) {
        visited.set(0);
        for (int high = 320; high > 0; high -= batchSize) {
          int low = Math.max(1, high - batchSize + 1);
          List<String> accepted = new ArrayList<>();
          loader.loadInstants(metaClient, new HoodieArchivedTimeline.ClosedClosedTimeRangeFilter(time(low), time(high)),
              mode, record -> true, (instant, record) -> accepted.add(instant));
          assertEquals(high - low + 1, accepted.size());
          assertEquals(time(low), accepted.get(0));
          assertEquals(time(high), accepted.get(accepted.size() - 1));
        }
        // Count actual iterator visits, not just payloads accepted by the consumer. Without
        // reader pushdown this is 3200 for 32-instant windows and 102400 for singletons.
        assertEquals(320, visited.get());
      }
    }
  }

  private static String time(int i) {
    return String.format("%03d", i);
  }
}
