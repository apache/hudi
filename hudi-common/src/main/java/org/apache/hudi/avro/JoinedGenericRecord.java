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

import org.apache.hudi.common.model.HoodieRecord;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Impl of {@link GenericRecord} to abstract meta fields and an actual data records of type GenericRecord.
 */
public class JoinedGenericRecord implements GenericRecord {
  private final GenericRecord dataRecord;
  private final Object[] metaFields;
  @Getter
  private final Schema schema;

  public JoinedGenericRecord(GenericRecord dataRecord, int metaFieldsSize, Schema schema) {
    this.dataRecord = dataRecord;
    this.metaFields = new Object[metaFieldsSize];
    this.schema = schema;
  }

  @Override
  public void put(String key, Object v) {
    Integer metaFieldPos = getMetaFieldPos(key);
    if (metaFieldPos != null) {
      metaFields[metaFieldPos] = v;
    } else {
      dataRecord.put(key, v);
    }
  }

  @Override
  public Object get(String key) {
    Integer metaFieldPos = getMetaFieldPos(key);
    if (metaFieldPos != null) {
      return metaFields[metaFieldPos];
    } else {
      return dataRecord.get(key);
    }
  }

  @Override
  public void put(int i, Object v) {
    if (i < metaFields.length) {
      metaFields[i] = v;
    } else {
      dataRecord.put(i - metaFields.length, v);
    }
  }

  @Override
  public Object get(int i) {
    if (i < metaFields.length) {
      return metaFields[i];
    } else {
      return dataRecord.get(i - metaFields.length);
    }
  }

  private Integer getMetaFieldPos(String fieldName) {
    Integer pos = HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(fieldName);
    if (pos == null && fieldName.equals(HoodieRecord.OPERATION_METADATA_FIELD)) {
      return HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.size();
    }
    return pos;
  }
}
