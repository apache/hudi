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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
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

public class HoodieAvroBinaryRecord extends HoodieRecord<byte[]> {

  public HoodieAvroBinaryRecord(HoodieKey key, byte[] data) {
    super(key, data);
  }

  public HoodieAvroBinaryRecord(HoodieKey key, byte[] data, Comparable orderingValue) {
    super(key, data);
    this.orderingValue = orderingValue;
  }

  public HoodieAvroBinaryRecord(HoodieKey key, byte[] data, HoodieOperation operation) {
    super(key, data, operation, Option.empty());
  }

  public HoodieAvroBinaryRecord(HoodieRecord<byte[]> record) {
    super(record);
  }

  public HoodieAvroBinaryRecord(
      HoodieKey key,
      byte[] data,
      HoodieOperation operation,
      HoodieRecordLocation currentLocation,
      HoodieRecordLocation newLocation) {
    super(key, data, operation, currentLocation, newLocation);
  }

  public HoodieAvroBinaryRecord(
      HoodieKey key,
      byte[] data,
      HoodieOperation operation,
      Option<Map<String, String>> metaData) {
    super(key, data, operation, metaData);
  }

  public HoodieAvroBinaryRecord() {

  }

  @Override
  public HoodieRecord<byte[]> newInstance() {
    return new HoodieAvroBinaryRecord(this);
  }

  @Override
  public HoodieRecord<byte[]> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroBinaryRecord(key, data, op);
  }

  @Override
  public HoodieRecord<byte[]> newInstance(HoodieKey key) {
    return new HoodieAvroBinaryRecord(key, data);
  }

  @Override
  public byte[] getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  @Override
  protected Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props) {
    if (orderingValue != null) {
      return orderingValue;
    }
    if (data == null) {
      orderingValue = OrderingValues.getDefault();
      return orderingValue;
    }
    String[] orderingFields = ConfigUtils.getOrderingFields(props);
    if (orderingFields != null) {
      boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(props.getProperty(
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
      orderingValue = OrderingValues.create(orderingFields, field -> {
        GenericRecord avroRecord = null;
        try {
          avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
        }
        return (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(avroRecord,
            field, true, consistentLogicalTimestampEnabled);
      });
      return orderingValue;
    }
    orderingValue = OrderingValues.getDefault();
    return orderingValue;
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO_BINARY;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null && key.getRecordKey() != null) {
      return key.getRecordKey();
    }
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return keyGeneratorOpt.isPresent()
          ? keyGeneratorOpt.get().getRecordKey(avroRecord)
          : avroRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    } catch (IOException e) {
      throw new HoodieIOException("Fail to deserialize bytes to Avro data", e);
    }
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return getRecordKey();
  }

  @Override
  protected void writeRecordPayload(byte[] payload, Kryo kryo, Output output) {
    Serializer<byte[]> avroSerializer = kryo.getSerializer(byte[].class);
    kryo.writeObjectOrNull(output, payload, avroSerializer);
  }

  @Override
  protected byte[] readRecordPayload(Kryo kryo, Input input) {
    return kryo.readObjectOrNull(input, byte[].class);
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return HoodieAvroUtils.getRecordColumnValues(this, columns, recordSchema, consistentLogicalTimestampEnabled);
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    try {
      IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return HoodieAvroReaderContext.getFieldValueFromIndexedRecord(indexedRecord, column);
    } catch (IOException e) {
      throw new HoodieIOException("Could not fetch value for column: " + column);
    }
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    try {
      GenericRecord genericRecord = HoodieAvroUtils.bytesToAvro(data, targetSchema);
      GenericRecord record = HoodieAvroUtils.stitchRecords(genericRecord, (GenericRecord) HoodieAvroUtils.bytesToAvro((byte[]) other.data, targetSchema), targetSchema);
      return new HoodieAvroBinaryRecord(key, HoodieAvroUtils.avroToBytes(record), operation, metaData);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser record ", e);
    }
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    try {
      IndexedRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, targetSchema);
      updateMetadataValuesInternal(newAvroRecord, metadataValues);
      return new HoodieAvroBinaryRecord(getKey(), HoodieAvroUtils.avroToBytes(newAvroRecord), getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public HoodieRecord updateMetaField(Schema recordSchema, int ordinal, String value) {
    try {
      IndexedRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      avroRecord.put(ordinal, value);
      return new HoodieAvroBinaryRecord(getKey(), HoodieAvroUtils.avroToBytes(avroRecord), getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    try {
      IndexedRecord oldRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(oldRecord, newSchema, renameCols);
      return new HoodieAvroBinaryRecord(getKey(), HoodieAvroUtils.avroToBytes(rewriteRecord), getOperation(), this.currentLocation, this.newLocation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize record!", e);
    }
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    if (HoodieOperation.isDelete(getOperation())) {
      return true;
    }
    if (data == null) {
      return true;
    }

    // Use data field to decide.
    if (recordSchema.getField(HOODIE_IS_DELETED_FIELD) == null) {
      return false;
    }

    Object deleteMarker = HoodieAvroUtils.bytesToAvro(data, recordSchema).get(HOODIE_IS_DELETED_FIELD);
    return deleteMarker instanceof Boolean && (boolean) deleteMarker;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    if (data == null) {
      return true;
    }
    IndexedRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    return avroRecord.equals(SENTINEL);
  }

  @Override
  public HoodieRecord<byte[]> copy() {
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return metaData != null ? metaData : Option.empty();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<Schema> schemaWithoutMetaFields) throws IOException {
    if (populateMetaFieldsOp) {
      return convertToHoodieAvroBinaryRecord(recordSchema, this, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return convertToHoodieAvroBinaryRecord(recordSchema, this, simpleKeyGenFieldsOpt.get(), withOperation, Option.empty(), schemaWithoutMetaFields);
    } else {
      return convertToHoodieAvroBinaryRecord(recordSchema, this, withOperation, partitionNameOp, schemaWithoutMetaFields);
    }
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    return null;
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    avroRecord.put(keyFieldName, StringUtils.EMPTY_STRING);
    return new HoodieAvroBinaryRecord(getKey(), HoodieAvroUtils.avroToBytes(avroRecord), getOperation(), this.currentLocation, this.newLocation);
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    if (data == null) {
      return Option.empty();
    }
    GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    return Option.of(new HoodieAvroIndexedRecord(key, avroRecord, operation, getMetadata()));
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) throws IOException {
    return HoodieAvroUtils.avroToBytesStream(HoodieAvroUtils.bytesToAvro(data, recordSchema));
  }

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<byte[]> convertToHoodieAvroBinaryRecord(Schema schema, HoodieAvroBinaryRecord record, boolean withOperationField) {
    return convertToHoodieAvroBinaryRecord(schema, record,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty(), Option.empty());
  }

  private static HoodieRecord<byte[]> convertToHoodieAvroBinaryRecord(Schema schema, HoodieAvroBinaryRecord record, boolean withOperationField,
                                                                      Option<String> partitionName, Option<Schema> schemaWithoutMetaFields) {
    return convertToHoodieAvroBinaryRecord(schema, record,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName, schemaWithoutMetaFields);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<byte[]> convertToHoodieAvroBinaryRecord(Schema schema, HoodieAvroBinaryRecord record, Pair<String, String> recordKeyPartitionPathFieldPair,
                                                                      boolean withOperationField, Option<String> partitionName, Option<Schema> schemaWithoutMetaFields) {

    try {
      HoodieKey hoodieKey = record.getKey();
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(record.data, schema);

      HoodieOperation operation = withOperationField
          ? HoodieOperation.fromName(avroRecord.get(HoodieRecord.OPERATION_METADATA_FIELD).toString())
          : null;

      if (schemaWithoutMetaFields.isPresent()) {
        Schema schemaNoMetaFields = schemaWithoutMetaFields.get();
        record.rewriteRecordWithNewSchema(schema, new Properties(), schemaNoMetaFields);
        return new HoodieAvroBinaryRecord(hoodieKey, record.data, operation);
      }

      return new HoodieAvroBinaryRecord(hoodieKey, record.data, operation);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser avro record ", e);
    }
  }
}
