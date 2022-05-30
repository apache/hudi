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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.Mapper;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public interface HoodieAvroFileReader extends HoodieFileReader, AutoCloseable {

  ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema) throws IOException;

  default Option<IndexedRecord> getRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeysIterator(List<String> keys) throws IOException {
    return getRecordsByKeysIterator(keys, getSchema());
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes) throws IOException {
    return getRecordsByKeyPrefixIterator(keyPrefixes, getSchema());
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeysIterator(List<String> keys, Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getRecordsByKeysIterator(keys, schema);
    return new HoodieRecordTransformIterator(iterator, mapper);
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getRecordsByKeyPrefixIterator(keyPrefixes, schema);
    return new HoodieRecordTransformIterator(iterator, mapper);
  }

  @Override
  default ClosableIterator<HoodieRecord> getRecordIterator(Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    return new MappingIterator<>(getIndexedRecordIterator(schema), mapper::apply);
  }

  @Override
  default ClosableIterator<HoodieRecord> getRecordIterator(Schema schema) throws IOException {
    HoodieRecord.Mapper<IndexedRecord> mapper = HoodieAvroIndexedRecord::new;
    return new MappingIterator<>(getIndexedRecordIterator(schema), mapper::apply);
  }

  @Override
  default Option<HoodieRecord> getRecordByKey(String key, Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordByKey(key, readerSchema).map(mapper::apply);
  }

  class HoodieRecordTransformIterator implements ClosableIterator<HoodieRecord> {
    private final ClosableIterator<IndexedRecord> dataIterator;
    private final HoodieRecord.Mapper mapper;

    public HoodieRecordTransformIterator(ClosableIterator<IndexedRecord> dataIterator, Mapper mapper) {
      this.dataIterator = dataIterator;
      this.mapper = mapper;
    }

    @Override
    public boolean hasNext() {
      return dataIterator.hasNext();
    }

    @Override
    public HoodieRecord next() {
      return mapper.apply(dataIterator.next());
    }

    @Override
    public void close() {
      dataIterator.close();
    }
  }
}
