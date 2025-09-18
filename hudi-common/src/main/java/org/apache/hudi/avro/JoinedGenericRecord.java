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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Impl of {@link GenericRecord} to abstract meta fields and an actual data records of type GenericRecord.
 */
public class JoinedGenericRecord implements GenericRecord {
  private final GenericRecord dataRecord;
  private final Map<String, Object> metaFields;
  private final List<String> metaFieldNames;
  private final Schema schema;

  public JoinedGenericRecord(GenericRecord dataRecord, List<String> metaFieldNames, Schema schema) {
    this.dataRecord = dataRecord;
    this.metaFields = new HashMap<>(metaFieldNames.size());
    this.metaFieldNames = metaFieldNames;
    this.schema = schema;
  }

  @Override
  public void put(String key, Object v) {
    if (metaFieldNames.contains(key)) {
      metaFields.put(key, v);
    } else {
      dataRecord.put(key, v);
    }
  }

  @Override
  public Object get(String key) {
    if (metaFieldNames.contains(key)) {
      return metaFields.get(key);
    } else {
      return dataRecord.get(key);
    }
  }

  @Override
  public void put(int i, Object v) {
    if (i < metaFieldNames.size()) {
      metaFields.put(metaFieldNames.get(i), v);
    } else {
      dataRecord.put(i - metaFieldNames.size(), v);
    }
  }

  @Override
  public Object get(int i) {
    if (i < metaFieldNames.size()) {
      return metaFields.get(metaFieldNames.get(i));
    } else {
      return dataRecord.get(i - metaFieldNames.size());
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
