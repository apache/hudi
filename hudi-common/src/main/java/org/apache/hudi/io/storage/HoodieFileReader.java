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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

public interface HoodieFileReader extends AutoCloseable {

  String[] readMinMaxRecordKeys();

  BloomFilter readBloomFilter();

  Set<String> filterRowKeys(Set<String> candidateRowKeys);

  ClosableIterator<HoodieRecord> getRecordIterator(Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException;

  default ClosableIterator<HoodieRecord> getRecordIterator(HoodieRecord.Mapper mapper) throws IOException {
    return getRecordIterator(getSchema(), mapper);
  }

  default Option<HoodieRecord> getRecordByKey(String key, Schema readerSchema, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Option<HoodieRecord> getRecordByKey(String key, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordByKey(key, getSchema(), mapper);
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeysIterator(List<String> keys, Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeysIterator(List<String> keys, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordsByKeysIterator(keys, getSchema(), mapper);
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema, HoodieRecord.Mapper mapper) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default ClosableIterator<HoodieRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, HoodieRecord.Mapper mapper) throws IOException {
    return getRecordsByKeyPrefixIterator(keyPrefixes, getSchema(), mapper);
  }

  Schema getSchema();

  void close();

  long getTotalRecords();
}
