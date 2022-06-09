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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.apache.hudi.TypeUtils.unsafeCast;

public interface HoodieAvroFileReader extends HoodieFileReader<IndexedRecord>, AutoCloseable {

  ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema) throws IOException;

  default Option<IndexedRecord> getIndexedRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> keys) throws IOException {
    return getIndexedRecordsByKeysIterator(keys, getSchema());
  }

  default ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(List<String> keyPrefixes) throws IOException {
    return getIndexedRecordsByKeyPrefixIterator(keyPrefixes, getSchema());
  }

  default ClosableIterator<HoodieRecord<IndexedRecord>> getRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordsByKeysIterator(keys, schema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  default ClosableIterator<HoodieRecord<IndexedRecord>> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordsByKeyPrefixIterator(keyPrefixes, schema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  default ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(Schema schema) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(schema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  default Option<HoodieRecord<IndexedRecord>> getRecordByKey(String key, Schema readerSchema) throws IOException {
    return getIndexedRecordByKey(key, readerSchema)
        .map(data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }
}
