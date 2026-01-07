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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

public class ArchivedTimelineLoaderV2 implements ArchivedTimelineLoader {

  private static final String INSTANT_TIME_ARCHIVED_META_FIELD = "instantTime";

  @Override
  public Option<String> loadInstants(HoodieTableMetaClient metaClient,
                                     @Nullable HoodieArchivedTimeline.TimeRangeFilter fileTimeRangeFilter,
                                     HoodieArchivedTimeline.LoadMode loadMode,
                                     Function<GenericRecord, Boolean> commitsFilter,
                                     BiConsumer<String, GenericRecord> recordConsumer) {
    return loadInstants(metaClient, fileTimeRangeFilter, loadMode, commitsFilter, recordConsumer, Option.empty());
  }

  @Override
  public Option<String> loadInstants(HoodieTableMetaClient metaClient,
                                     @Nullable HoodieArchivedTimeline.TimeRangeFilter fileTimeRangeFilter,
                                     HoodieArchivedTimeline.LoadMode loadMode,
                                     Function<GenericRecord, Boolean> commitsFilter,
                                     BiConsumer<String, GenericRecord> recordConsumer,
                                     Option<Integer> limit) {
    try {
      // List all files
      List<String> fileNames = LSMTimeline.latestSnapshotManifest(metaClient, metaClient.getArchivePath()).getFileNames();

      boolean hasLimit = limit.isPresent() && limit.get() > 0;
      AtomicInteger loadedCount = new AtomicInteger(0);
      
      List<String> filteredFiles = new ArrayList<>();
      for (String fileName : fileNames) {
        if (fileTimeRangeFilter == null || LSMTimeline.isFileInRange(fileTimeRangeFilter, fileName)) {
          filteredFiles.add(fileName);
        }
      }

      // Sort files in reverse chronological order if limit is specified (newest first for limit queries)
      if (hasLimit) {
        filteredFiles.sort(Comparator.comparing(LSMTimeline::getMaxInstantTime).reversed());
      }

      Schema readSchema = LSMTimeline.getReadSchema(loadMode);
      // Use serial stream when limit is involved to guarantee order
      Stream<String> fileStream = hasLimit
          ? filteredFiles.stream()
          : filteredFiles.parallelStream();
      return Option.fromJavaOptional(fileStream.map(fileName -> {
        if (hasLimit && loadedCount.get() >= limit.get()) {
          return null;
        }
        // Read the archived file
        try (HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
            .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, new StoragePath(metaClient.getArchivePath(), fileName))) {
          //TODO boundary to revisit in later pr to use HoodieSchema directly
          try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema()),
                  HoodieSchema.fromAvroSchema(readSchema))) {
            int instantTimeFieldPosition = readSchema.getField(INSTANT_TIME_ARCHIVED_META_FIELD).pos();
            while (iterator.hasNext() && (!hasLimit || loadedCount.get() < limit.get())) {
              GenericRecord record = (GenericRecord) iterator.next();
              String instantTime = record.get(instantTimeFieldPosition).toString();
              if (commitsFilter.apply(record)) {
                recordConsumer.accept(instantTime, record);
                if (hasLimit) {
                  loadedCount.incrementAndGet();
                }
              }
            }
          }
          return LSMTimeline.getMinInstantTime(fileName);
        } catch (IOException ioException) {
          throw new HoodieIOException("Error open file reader for path: "
              + new StoragePath(metaClient.getArchivePath(), fileName));
        }
      }).filter(Objects::nonNull).reduce(InstantComparison::minTimestamp));
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
  }
}
