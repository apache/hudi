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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.MapperUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

/**
 * Reads records from base file and merges any updates from log files and provides iterable over all records in the file slice.
 */
public class HoodieFileSliceReader<T> implements Iterator<HoodieRecord<T>> {

  private final Iterator<HoodieRecord<T>> recordsIterator;

  public static HoodieFileSliceReader getFileSliceReader(
      Option<HoodieFileReader> baseFileReader, HoodieMergedLogRecordScanner scanner, Schema schema, String payloadClass,
      String preCombineField, Option<Pair<String, String>> simpleKeyGenFieldsOpt) throws IOException {
    Map<String, Object> map = MapperUtils.buildMapperConfig(payloadClass, preCombineField, simpleKeyGenFieldsOpt, scanner.isWithOperationField(), scanner.getPartitionName());
    if (baseFileReader.isPresent()) {
      Iterator<HoodieRecord> baseIterator = baseFileReader.get().getRecordIterator(schema);
      while (baseIterator.hasNext()) {
        scanner.processNextRecord(baseIterator.next().expansion(schema, new Properties(), map));
      }
      return new HoodieFileSliceReader(scanner.iterator());
    } else {
      Iterable<HoodieRecord> iterable = () -> scanner.iterator();
      HoodiePayloadConfig payloadConfig = HoodiePayloadConfig.newBuilder().withPayloadOrderingField(preCombineField).build();
      return new HoodieFileSliceReader(StreamSupport.stream(iterable.spliterator(), false)
          .map(e -> {
            try {
              return e.expansion(schema, payloadConfig.getProps(), map);
            } catch (IOException io) {
              throw new HoodieIOException("Error while creating reader for file slice with no base file.", io);
            }
          }).iterator());
    }
  }

  private HoodieFileSliceReader(Iterator<HoodieRecord<T>> recordsItr) {
    this.recordsIterator = recordsItr;
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public HoodieRecord<T> next() {
    return recordsIterator.next();
  }
}
