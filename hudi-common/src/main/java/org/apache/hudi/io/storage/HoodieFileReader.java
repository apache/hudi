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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.Set;

/**
 * Hudi's File Reader interface providing common set of APIs to fetch
 *
 * <ul>
 *   <li>{@link HoodieRecord}s</li>
 *   <li>Metadata (statistics, bloom-filters, etc)</li>
 * </ul>
 *
 * from a file persisted in storage.
 *
 * @param <T> target engine-specific representation of the raw data ({@code IndexedRecord} for Avro,
 *           {@code InternalRow} for Spark, etc)
 */
public interface HoodieFileReader<T> extends AutoCloseable {

  String[] readMinMaxRecordKeys();

  BloomFilter readBloomFilter();

  Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys);

  ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException;

  default ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieSchema readerSchema) throws IOException {
    return getRecordIterator(readerSchema, readerSchema);
  }

  default ClosableIterator<HoodieRecord<T>> getRecordIterator() throws IOException {
    return getRecordIterator(getSchema());
  }

  ClosableIterator<String> getRecordKeyIterator() throws IOException;

  HoodieSchema getSchema();

  void close();

  long getTotalRecords();
}
