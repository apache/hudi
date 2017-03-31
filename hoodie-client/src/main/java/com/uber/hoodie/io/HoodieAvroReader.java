/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.AvroUtils;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

/**
 * This reads a bunch of HoodieRecords from avro log files and deduplicates and mantains the merged
 * state in memory. This is useful for compaction and record reader
 */
public class HoodieAvroReader implements Iterable<HoodieRecord<HoodieAvroPayload>> {

  private final Collection<HoodieRecord<HoodieAvroPayload>> records;
  private AtomicLong totalLogFiles = new AtomicLong(0);
  private AtomicLong totalLogRecords = new AtomicLong(0);
  private long totalRecordsToUpdate;


  public HoodieAvroReader(FileSystem fs, List<String> logFilePaths, Schema readerSchema) {
    Map<String, HoodieRecord<HoodieAvroPayload>> records = Maps.newHashMap();
    for (String path : logFilePaths) {
      totalLogFiles.incrementAndGet();
      List<HoodieRecord<HoodieAvroPayload>> recordsFromFile = AvroUtils
          .loadFromFile(fs, path, readerSchema);
      totalLogRecords.addAndGet(recordsFromFile.size());
      for (HoodieRecord<HoodieAvroPayload> recordFromFile : recordsFromFile) {
        String key = recordFromFile.getRecordKey();
        if (records.containsKey(key)) {
          // Merge and store the merged record
          HoodieAvroPayload combinedValue = records.get(key).getData()
              .preCombine(recordFromFile.getData());
          records.put(key, new HoodieRecord<>(new HoodieKey(key, recordFromFile.getPartitionPath()),
              combinedValue));
        } else {
          // Put the record as is
          records.put(key, recordFromFile);
        }
      }
    }
    this.records = records.values();
    this.totalRecordsToUpdate = records.size();
  }

  @Override
  public Iterator<HoodieRecord<HoodieAvroPayload>> iterator() {
    return records.iterator();
  }

  @Override
  public void forEach(Consumer<? super HoodieRecord<HoodieAvroPayload>> consumer) {
    records.forEach(consumer);
  }

  @Override
  public Spliterator<HoodieRecord<HoodieAvroPayload>> spliterator() {
    return records.spliterator();
  }

  public long getTotalLogFiles() {
    return totalLogFiles.get();
  }

  public long getTotalLogRecords() {
    return totalLogRecords.get();
  }

  public long getTotalRecordsToUpdate() {
    return totalRecordsToUpdate;
  }
}
