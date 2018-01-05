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

package com.uber.hoodie.common.table.log;

import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieLogIndex implements Serializable {
  private BloomFilter bloomFilter;
  private Set<String> keys;

  public HoodieLogIndex(List<String> logFilePaths, Schema schema, int numEntries, double errorRate) {
    keys = new HashSet<>();
    init(logFilePaths, schema, numEntries, errorRate);
  }

  public HoodieLogIndex(Stream<HoodieLogFile> logFilePaths, Schema schema, int numEntries, double errorRate) {
    this(logFilePaths.map(logFilePath -> logFilePath.getPath().toString()).collect(Collectors.toList()),
            schema, numEntries, errorRate);
  }

  private void init(List<String> logFilePaths, Schema schema, int numEntries, double errorRate) {
    // sort log files in descending order of version
    logFilePaths.stream().map(path -> new HoodieLogFile(new Path(path)))
        .sorted((HoodieLogFile left, HoodieLogFile right) -> left.getLogVersion() < right.getLogVersion() ? 1 : -1)
        .forEach(logFile -> {
      try {
        HoodieLogFormatReader reader = new HoodieLogFormatReader(FSUtils.getFs(),
                logFile, schema, true);
        HoodieLogBlock.HoodieLogBlockType blockType;
        while(reader.hasPrev()) {
          HoodieLogBlock block = reader.prev();
          blockType = block.getBlockType();
          if(blockType == HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK && this.bloomFilter == null) {
            String bloomFilterString = block.getLogMetadata().get(HoodieLogBlock.LogMetadataType.LOG_INDEX);
            this.bloomFilter = new BloomFilter(bloomFilterString);
          }
          addKeys(block);
        }
      } catch(IOException io) {
        throw new UncheckedIOException(io);
      }
    });

    if(this.bloomFilter == null) {
      this.bloomFilter = new BloomFilter(numEntries, errorRate);
    }
  }

  private void addKeys(HoodieLogBlock block) {
    keys.addAll(((HoodieAvroDataBlock) block).getRecords().stream()
            .map(record -> ((GenericRecord)record).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet()));
  }

  public Set<String> getKeys() {
    return keys;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }
}
