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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class DeleteRecord implements IndexedRecord {
  private final String key;
  private final IndexedRecord record;

  public DeleteRecord(String key, IndexedRecord record) {
    this.key = key;
    this.record = record;
  }

  public String getRecordKey() {
    return key;
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException("DeleteRecord does not support put operation");
  }

  @Override
  public Object get(int i) {
    return record == null ? null : record.get(i);
  }

  @Override
  public Schema getSchema() {
    return record == null ? Schema.create(Schema.Type.NULL) : record.getSchema();
  }
}
