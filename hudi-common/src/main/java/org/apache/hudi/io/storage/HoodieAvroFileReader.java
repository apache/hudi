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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface HoodieAvroFileReader extends HoodieFileReader, AutoCloseable {

  default Map<String, IndexedRecord> getRecordsByKeys(List<String> rowKeys) throws IOException {
    throw new UnsupportedOperationException();
  }

  Iterator<IndexedRecord> getRecordIterator(Schema readerSchema) throws IOException;

  default Option<IndexedRecord> getRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  default Map<String, HoodieRecord> getRecordsByKeys(List<String> rowKeys, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordsByKeys(rowKeys).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> mapper.apply(e.getValue())));
  }

  @Override
  default Iterator<HoodieRecord> getRecordIterator(Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    return new MappingIterator<>(getRecordIterator(schema), mapper::apply);
  }

  @Override
  default Option<HoodieRecord> getRecordByKey(String key, Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordByKey(key, readerSchema).map(mapper::apply);
  }

}
