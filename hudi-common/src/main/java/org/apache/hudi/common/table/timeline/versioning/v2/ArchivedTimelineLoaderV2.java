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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

public class ArchivedTimelineLoaderV2 implements ArchivedTimelineLoader {

  private static final String INSTANT_TIME_ARCHIVED_META_FIELD = "instantTime";

  @Override
  public void loadInstants(HoodieTableMetaClient metaClient,
                           @Nullable HoodieArchivedTimeline.TimeRangeFilter filter,
                           HoodieArchivedTimeline.LoadMode loadMode,
                           Function<GenericRecord, Boolean> commitsFilter,
                           BiConsumer<String, GenericRecord> recordConsumer) {
    try {
      // List all files
      List<String> fileNames = LSMTimeline.latestSnapshotManifest(metaClient, metaClient.getArchivePath()).getFileNames();

      Schema readSchema = LSMTimeline.getReadSchema(loadMode);
      fileNames.stream()
          .filter(fileName -> filter == null || LSMTimeline.isFileInRange(filter, fileName))
          .parallel().forEach(fileName -> {
            // Read the archived file
            try (HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(metaClient.getStorage())
                .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
                .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, new StoragePath(metaClient.getArchivePath(), fileName))) {
              try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieLSMTimelineInstant.getClassSchema(), readSchema)) {
                while (iterator.hasNext()) {
                  GenericRecord record = (GenericRecord) iterator.next();
                  String instantTime = record.get(INSTANT_TIME_ARCHIVED_META_FIELD).toString();
                  if ((filter == null || filter.isInRange(instantTime))
                      && commitsFilter.apply(record)) {
                    recordConsumer.accept(instantTime, record);
                  }
                }
              }
            } catch (IOException ioException) {
              throw new HoodieIOException("Error open file reader for path: "
                  + new StoragePath(metaClient.getArchivePath(), fileName));
            }
          });
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
  }
}
