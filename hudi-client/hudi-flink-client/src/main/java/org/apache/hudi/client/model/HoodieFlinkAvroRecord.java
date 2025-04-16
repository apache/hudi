/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.Map;
import java.util.Properties;

/**
 * Flink implementation of `HoodieRecord`, which is expected to hold Avro {@code IndexedRecord} as payload.
 * It's only used by writer when the log block format type is AVRO.
 */
public class HoodieFlinkAvroRecord extends HoodieRecord<IndexedRecord> {
  private Comparable<?> orderingValue = 0;

  public HoodieFlinkAvroRecord(HoodieKey key, HoodieOperation op, Comparable<?> orderingValue, IndexedRecord record) {
    super(key, record, op, Option.empty());
    this.orderingValue = orderingValue;
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance() {
    return new HoodieFlinkAvroRecord(key, operation, orderingValue, data);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieFlinkAvroRecord(key, op, orderingValue, data);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    return this.orderingValue;
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.FLINK;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return getRecordKey();
  }

  @Override
  protected void writeRecordPayload(IndexedRecord payload, Kryo kryo, Output output) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  protected IndexedRecord readRecordPayload(Kryo kryo, Input input) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    GenericRecord recordWithMetaFields = new GenericData.Record(targetSchema);
    // update meta fields
    if (!metadataValues.isEmpty()) {
      String[] values = metadataValues.getValues();
      for (int i = 0; i < values.length; i++) {
        if (values[i] != null) {
          recordWithMetaFields.put(i, values[i]);
        }
      }
    }
    // update data fields
    int metaFieldsSize = targetSchema.getFields().size() - recordSchema.getFields().size();
    for (int i = 0; i < recordSchema.getFields().size(); i++) {
      recordWithMetaFields.put(metaFieldsSize + i, data.get(i));
    }
    return new HoodieFlinkAvroRecord(key, operation, orderingValue, recordWithMetaFields);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) {
    if (data == null) {
      return true;
    }

    if (HoodieOperation.isDelete(getOperation())) {
      return true;
    }

    // Use data field to decide.
    Schema.Field deleteField = recordSchema.getField(HOODIE_IS_DELETED_FIELD);
    if (deleteField == null) {
      return false;
    }
    Object deleteMarker = data.get(deleteField.pos());
    return deleteMarker instanceof Boolean && (Boolean) deleteMarker;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) {
    return false;
  }

  @Override
  public HoodieRecord<IndexedRecord> copy() {
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<Schema> schemaWithoutMetaFields) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) {
    return Option.of(new HoodieAvroIndexedRecord(getKey(), getData(), getOperation(), getMetadata()));
  }
}
