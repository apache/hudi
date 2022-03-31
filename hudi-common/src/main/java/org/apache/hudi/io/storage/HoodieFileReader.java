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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.Option;

public interface HoodieFileReader<R extends IndexedRecord> {

  public String[] readMinMaxRecordKeys();

  public BloomFilter readBloomFilter();

  public Set<String> filterRowKeys(Set<String> candidateRowKeys);

  public Iterator<R> getRecordIterator(Schema readerSchema) throws IOException;

  public Iterator<R> getRecordKeyIterator() throws IOException;

  default Iterator<R> getRecordIterator() throws IOException {
    return getRecordIterator(getSchema());
  }

  default Option<R> getRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default Option<R> getRecordByKey(String key) throws IOException {
    return getRecordByKey(key, getSchema());
  }

  Schema getSchema();

  void close();

  long getTotalRecords();
}
