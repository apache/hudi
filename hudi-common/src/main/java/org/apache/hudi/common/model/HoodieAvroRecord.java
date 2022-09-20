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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HoodieAvroRecord<T extends HoodieRecordPayload> extends HoodieRecord<T> {

  public HoodieAvroRecord(HoodieKey key, T data) {
    super(key, data);
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieAvroRecord(HoodieRecord<T> record) {
    super(record);
  }

  public HoodieAvroRecord() {
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return new HoodieAvroRecord<>(this);
  }

  @Override
  public HoodieRecord<T> newInstance(T data) {
    return new HoodieAvroRecord<>(key, data, operation);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroRecord<>(key, data, op);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key) {
    return new HoodieAvroRecord<>(key, data);
  }

  @Override
  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  public Comparable<?> getOrderingValue(Properties props) {
    return this.getData().getOrderingValue();
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return getRecordKey();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public Object getRecordColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return HoodieAvroUtils.getRecordColumnValues(this, columns, recordSchema, consistentLogicalTimestampEnabled);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    Option<IndexedRecord> avroRecordPayloadOpt = getData().getInsertValue(recordSchema, props);
    GenericRecord avroPayloadInNewSchema =
        HoodieAvroUtils.rewriteRecord((GenericRecord) avroRecordPayloadOpt.get(), targetSchema);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroPayloadInNewSchema), getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    GenericRecord oldRecord = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(oldRecord, newSchema, renameCols);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(rewriteRecord), getOperation());
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    GenericRecord avroRecordPayload = (GenericRecord) getData().getInsertValue(recordSchema, props).get();

    metadataValues.getKv().forEach((key, value) -> {
      if (value != null) {
        avroRecordPayload.put(key, value);
      }
    });

    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroRecordPayload), getOperation());
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props,
      String keyName,
      String keyValue) throws IOException {
    GenericRecord avroRecordPayload = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    avroRecordPayload.put(keyName, keyValue);
    return new HoodieAvroRecord<>(getKey(), new RewriteAvroPayload(avroRecordPayload), getOperation());
  }

  @Override
  public boolean isDelete(Schema schema, Properties props) throws IOException {
    return !getData().getInsertValue(schema, props).isPresent();
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties props) throws IOException {
    Option<IndexedRecord> insertRecord = getData().getInsertValue(schema, props);
    // just skip the ignored record
    if (insertRecord.isPresent() && insertRecord.get().equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      Schema schema, Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields) throws IOException {
    IndexedRecord indexedRecord = (IndexedRecord) data.getInsertValue(schema, props).get();
    String payloadClass = ConfigUtils.getPayloadClass(props);
    String preCombineField = ConfigUtils.getOrderingField(props);
    return HoodieAvroUtils.createHoodieRecordFromAvro(indexedRecord, payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation, partitionNameOp, populateMetaFields);
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException();
  }

  public Option<Map<String, String>> getMetadata() {
    return getData().getMetadata();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema schema, Properties props) throws IOException {
    Option<IndexedRecord> avroData = getData().getInsertValue(schema, props);
    if (avroData.isPresent()) {
      return Option.of(new HoodieAvroIndexedRecord(avroData.get()));
    } else {
      return Option.empty();
    }
  }
}
