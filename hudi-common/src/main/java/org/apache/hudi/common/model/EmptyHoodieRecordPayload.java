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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

/**
 * Empty payload used for deletions.
 */
public class EmptyHoodieRecordPayload implements HoodieRecordPayload<EmptyHoodieRecordPayload> {

  public EmptyHoodieRecordPayload() {
  }

  public EmptyHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
  }

  @Override
  public EmptyHoodieRecordPayload preCombine(EmptyHoodieRecordPayload oldValue) {
    return oldValue;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
    return Option.empty();
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) {
    return Option.empty();
  }
}
