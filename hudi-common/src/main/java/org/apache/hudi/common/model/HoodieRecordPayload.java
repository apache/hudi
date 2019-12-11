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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Every Hoodie dataset has an implementation of the <code>HoodieRecordPayload</code> This abstracts out callbacks which
 * depend on record specific logic.
 */
public interface HoodieRecordPayload<T extends HoodieRecordPayload> extends Serializable {

  /**
   * When more than one HoodieRecord have the same HoodieKey, this function combines them before attempting to
   * insert/upsert (if combining turned on in HoodieClientConfig).
   */
  T preCombine(T another);

  /**
   * This methods lets you write custom merging/combining logic to produce new values as a function of current value on
   * storage and whats contained in this object.
   * <p>
   * eg: 1) You are updating counters, you may want to add counts to currentValue and write back updated counts 2) You
   * may be reading DB redo logs, and merge them with current image for a database row on storage
   *
   * @param currentValue Current value in storage, to merge/combine this payload with
   * @param schema Schema used for record
   * @return new combined/merged value to be written back to storage. EMPTY to skip writing this record.
   */
  Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException;

  /**
   * Generates an avro record out of the given HoodieRecordPayload, to be written out to storage. Called when writing a
   * new value for the given HoodieKey, wherein there is no existing record in storage to be combined against. (i.e
   * insert) Return EMPTY to skip writing this record.
   */
  Option<IndexedRecord> getInsertValue(Schema schema) throws IOException;

  /**
   * This method can be used to extract some metadata from HoodieRecordPayload. The metadata is passed to
   * {@code WriteStatus.markSuccess()} and {@code WriteStatus.markFailure()} in order to compute some aggregate metrics
   * using the metadata in the context of a write success or failure.
   */
  default Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }
}
