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

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieAvroIndexedRecord.updateMetadataValuesInternal;

/**
 * Implementation of {@link HoodieRecord} using Avro payload.
 *
 * @param <T> payload implementation.
 */
public class HoodieAvroRecord<T extends HoodieRecordPayload> extends HoodieRecord<T> {

  public HoodieAvroRecord(HoodieKey key, T data) {
    super(key, data);
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation hoodieOperation, Comparable orderingValue, Boolean isDelete) {
    super(key, data, hoodieOperation, isDelete, Option.empty());
    this.orderingValue = orderingValue;
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation operation) {
    super(key, data, operation, Option.empty());
  }

  public HoodieAvroRecord(HoodieRecord<T> record) {
    super(record);
  }

  public HoodieAvroRecord(
      HoodieKey key,
      T data,
      HoodieOperation operation,
      HoodieRecordLocation currentLocation,
      HoodieRecordLocation newLocation) {
    super(key, data, operation, currentLocation, newLocation);
  }

  public HoodieAvroRecord() {
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return new HoodieAvroRecord<>(this);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroRecord<>(key, data, op, orderingValue, isDelete);
  }

  @Override
  public HoodieRecord<T> newInstance(HoodieKey key) {
    return new HoodieAvroRecord<>(key, data, operation, orderingValue, isDelete);
  }

  @Override
  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  public Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props, String[] orderingFields) {
    return this.getData().getOrderingValue();
  }

  @Override
  public String getRecordKey(Schema recordSchema,
      Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return getRecordKey();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return HoodieAvroUtils.getRecordColumnValues(this, columns, recordSchema, consistentLogicalTimestampEnabled);
  }

  @Override
  public Comparable<?> getOrderingValueAsJava(Schema recordSchema, Properties props, String[] orderingFields) {
    return getOrderingValue(recordSchema, props, orderingFields);
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    try {
      Option<IndexedRecord> indexedRecordOpt = getData().getInsertValue(recordSchema, props);
      if (indexedRecordOpt.isPresent()) {
        return AvroRecordContext.getFieldValueFromIndexedRecord(indexedRecordOpt.get(), column);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not fetch value for column: " + column);
    }
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    try {
      Option<IndexedRecord> avroRecordOpt = getData().getInsertValue(recordSchema, props);
      GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecordOpt.get(), targetSchema);
      updateMetadataValuesInternal(newAvroRecord, metadataValues);
      return new HoodieAvroIndexedRecord(getKey(), newAvroRecord, getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public HoodieRecord updateMetaField(Schema recordSchema, int ordinal, String value) {
    try {
      Option<IndexedRecord> avroRecordOpt = getData().getInsertValue(recordSchema);
      // value should always be present if the meta fields are being updated since the record is being written
      IndexedRecord avroRecord = avroRecordOpt.orElseThrow(() -> new HoodieIOException("Failed to get insert value for record schema: " + recordSchema));
      avroRecord.put(ordinal, value);
      return new HoodieAvroIndexedRecord(getKey(), avroRecord, getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    try {
      GenericRecord oldRecord = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
      GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(oldRecord, newSchema, renameCols);
      return new HoodieAvroIndexedRecord(getKey(), rewriteRecord, getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    GenericRecord avroRecordPayload = (GenericRecord) getData().getInsertValue(recordSchema, props).get();
    avroRecordPayload.put(keyFieldName, StringUtils.EMPTY_STRING);
    return new HoodieAvroIndexedRecord(getKey(), avroRecordPayload, getOperation(), this.currentLocation, this.newLocation);
  }

  @Override
  protected boolean checkIsDelete(DeleteContext deleteContext, Properties props) throws IOException {
    if (HoodieOperation.isDelete(getOperation())) {
      return true;
    }
    if (this.data instanceof BaseAvroPayload) {
      return ((BaseAvroPayload) this.data).isDeleted(deleteContext.getReaderSchema(), props);
    } else if (this.data instanceof HoodieMetadataPayload) {
      return ((HoodieMetadataPayload) this.data).isDeleted();
    } else {
      return !this.data.getInsertValue(deleteContext.getReaderSchema(), props).isPresent();
    }
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    HoodieRecordPayload<?> recordPayload = getData();
    // NOTE: Currently only records borne by [[ExpressionPayload]] can currently be ignored,
    //       as such, we limit exposure of this method only to such payloads
    if (recordPayload instanceof BaseAvroPayload && ((BaseAvroPayload) recordPayload).canProduceSentinel()) {
      Option<IndexedRecord> insertRecord = recordPayload.getInsertValue(recordSchema, props);
      return insertRecord.isPresent() && insertRecord.get().equals(SENTINEL);
    }

    return false;
  }

  @Override
  public HoodieRecord<T> copy() {
    return this;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      Schema recordSchema, Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields,
      Option<Schema> schemaWithoutMetaFields) throws IOException {
    IndexedRecord indexedRecord = (IndexedRecord) data.getInsertValue(recordSchema, props).get();
    String payloadClass = ConfigUtils.getPayloadClass(props);
    String[] orderingFields = ConfigUtils.getOrderingFields(props);
    return HoodieAvroUtils.createHoodieRecordFromAvro(indexedRecord, payloadClass, orderingFields, simpleKeyGenFieldsOpt,
        withOperation, partitionNameOp, populateMetaFields, schemaWithoutMetaFields);
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
      Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException();
  }

  public Option<Map<String, String>> getMetadata() {
    return getData().getMetadata();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    Option<IndexedRecord> avroData = getData().getIndexedRecord(recordSchema, props);
    if (avroData.isPresent()) {
      HoodieAvroIndexedRecord record =
          new HoodieAvroIndexedRecord(key, avroData.get(), operation, getData().getMetadata(), getData().getOrderingValue());
      return Option.of(record);
    } else {
      return Option.empty();
    }
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) throws IOException {
    if (data instanceof BaseAvroPayload) {
      byte[] data = ((BaseAvroPayload) getData()).getRecordBytes();
      ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
      baos.write(data);
      return baos;
    } else {
      Option<IndexedRecord> avroData = getData().getInsertValue(recordSchema, props);
      return avroData.map(HoodieAvroUtils::avroToBytesStream).orElse(new ByteArrayOutputStream(0));
    }
  }

  @Override
  protected final void writeRecordPayload(T payload, Kryo kryo, Output output) {
    // NOTE: Since [[orderingVal]] is polymorphic we have to write out its class
    //       to be able to properly deserialize it
    kryo.writeClassAndObject(output, payload);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected final T readRecordPayload(Kryo kryo, Input input) {
    return (T) kryo.readClassAndObject(input);
  }

  @Override
  public Object convertColumnValueForLogicalType(Schema fieldSchema,
                                                 Object fieldValue,
                                                 boolean keepConsistentLogicalTimestamp) {
    return HoodieAvroUtils.convertValueForAvroLogicalTypes(
        fieldSchema, fieldValue, keepConsistentLogicalTimestamp);
  }
}
