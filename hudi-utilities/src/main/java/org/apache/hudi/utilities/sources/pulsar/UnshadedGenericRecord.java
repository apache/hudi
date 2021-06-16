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

package org.apache.hudi.utilities.sources.pulsar;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class UnshadedGenericRecord implements GenericRecord {

  private org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord shadedRecord;

  public UnshadedGenericRecord(org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord shadedGenericRecord) {
    this.shadedRecord = shadedGenericRecord;
  }

  @Override
  public void put(String key, Object v) {
    shadedRecord.put(key, v);
  }

  @Override
  public Object get(String key) {
    return shadedRecord.get(key);
  }

  @Override
  public void put(int i, Object v) {
    shadedRecord.put(i, v);
  }

  @Override
  public Object get(int i) {
    return shadedRecord.get(i);
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
