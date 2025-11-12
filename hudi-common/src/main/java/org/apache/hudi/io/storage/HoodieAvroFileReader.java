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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.expression.Predicate;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * Base class for every Avro file reader
 */
public abstract class HoodieAvroFileReader implements HoodieFileReader<IndexedRecord> {

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(readerSchema, requestedSchema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema) throws IOException {
    return getIndexedRecordIterator(readerSchema, readerSchema);
  }

  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    return getIndexedRecordIterator(readerSchema, requestedSchema, Collections.emptyMap());
  }

  public abstract ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema, Map<String, String> renamedColumns) throws IOException;

  public abstract ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> keys,
                                                                                  Schema readerSchema)
      throws IOException;

  public abstract ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(
      List<String> sortedKeyPrefixes, Schema readerSchema) throws IOException;

  // No key predicate support by default.
  public boolean supportKeyPredicate() {
    return false;
  }

  // No key prefix predicate support by default.
  public boolean supportKeyPrefixPredicate() {
    return false;
  }

  public List<String> extractKeys(Option<Predicate> keyPredicateOpt) {
    throw new UnsupportedOperationException("Option extractKeys is not supported");
  }

  public List<String> extractKeyPrefixes(Option<Predicate> keyPrefixPredicateOpt) {
    throw new UnsupportedOperationException("Option extractKeyPrefixes is not supported");
  }
}
