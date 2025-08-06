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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
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

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;

/**
 * This only use by reader returning.
 */
public class HoodieAvroIndexedRecord extends HoodieRecord<IndexedRecord> {
  private static final long serialVersionUID = 1L;
  private static final AvroRecordContext AVRO_RECORD_CONTEXT = new AvroRecordContext();
  private SerializableIndexedRecord optimizedRecord;

  public HoodieAvroIndexedRecord(IndexedRecord data) {
    this(null, data, null, null, null);
  }

  public HoodieAvroIndexedRecord(IndexedRecord data, Comparable orderingValue) {
    this(null, data, null, null, null);
    this.orderingValue = orderingValue;
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data) {
    this(key, data, null, null, null);
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, HoodieOperation hoodieOperation) {
    this(key, data, hoodieOperation, Option.empty());
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, HoodieOperation hoodieOperation, HoodieRecordLocation currentLocation) {
    this(key, data, hoodieOperation, currentLocation, null);
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, Comparable<?> orderingValue) {
    this(key, data, null, null, null);
    this.orderingValue = orderingValue;
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, Comparable<?> orderingValue, HoodieOperation operation) {
    this(key, data, operation, null, null);
    this.orderingValue = orderingValue;
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, HoodieOperation operation, HoodieRecordLocation currentLocation, HoodieRecordLocation newLocation) {
    super(key, new SerializableIndexedRecord(data), operation, currentLocation, newLocation);
    this.optimizedRecord = (SerializableIndexedRecord) this.data;
  }

  public HoodieAvroIndexedRecord(IndexedRecord data, HoodieRecordLocation currentLocation) {
    this(null, data, null, currentLocation, null);
  }

  public HoodieAvroIndexedRecord(
      HoodieKey key,
      IndexedRecord data,
      HoodieOperation operation,
      Option<Map<String, String>> metaData) {
    super(key, new SerializableIndexedRecord(data), operation, metaData);
    this.optimizedRecord = (SerializableIndexedRecord) this.data;
  }

  HoodieAvroIndexedRecord(HoodieRecord<IndexedRecord> record) {
    super(record);
    this.optimizedRecord = (SerializableIndexedRecord) this.data;
  }

  public HoodieAvroIndexedRecord() {
    this.optimizedRecord = (SerializableIndexedRecord) this.data;
  }

  @Override
  public HoodieRecord newInstance() {
    return new HoodieAvroIndexedRecord(this);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroIndexedRecord(key, new SerializableIndexedRecord(data), op, metaData);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key) {
    return new HoodieAvroIndexedRecord(key, new SerializableIndexedRecord(data), operation, metaData);
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return key.getRecordKey();
    }
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey((GenericRecord) data) : ((GenericRecord) data).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    setSchema(recordSchema);
    if (key != null) {
      return key.getRecordKey();
    }
    return Option.ofNullable(data.getSchema().getField(keyFieldName))
        .map(keyField -> data.get(keyField.pos()))
        .map(Object::toString).orElse(null);
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    setSchema(recordSchema);
    return AvroRecordContext.getFieldValueFromIndexedRecord(data, column);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    setSchema(targetSchema);
    GenericRecord record = HoodieAvroUtils.stitchRecords((GenericRecord) data, (GenericRecord) other.getData(), targetSchema);
    return new HoodieAvroIndexedRecord(key, record, operation, metaData);
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    setSchema(recordSchema);
    GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(data, targetSchema);
    updateMetadataValuesInternal(newAvroRecord, metadataValues);
    return new HoodieAvroIndexedRecord(key, newAvroRecord, operation, metaData);
  }

  @Override
  public HoodieRecord updateMetaField(Schema recordSchema, int ordinal, String value) {
    setSchema(recordSchema);
    data.put(ordinal, value);
    return new HoodieAvroIndexedRecord(key, data, operation, metaData);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    setSchema(recordSchema);
    GenericRecord record = HoodieAvroUtils.rewriteRecordWithNewSchema(data, newSchema, renameCols);
    return new HoodieAvroIndexedRecord(key, record, operation, metaData);
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    setSchema(recordSchema);
    ((GenericRecord) data).put(keyFieldName, StringUtils.EMPTY_STRING);
    return this;
  }

  @Override
  protected boolean checkIsDelete(Schema recordSchema, Properties props) {
    DeleteContext deleteContext = new DeleteContext(props, recordSchema);
    setSchema(recordSchema);
    return AVRO_RECORD_CONTEXT.isDeleteRecord(data, deleteContext);
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return getData().equals(SENTINEL);
  }

  @Override
  public HoodieRecord<IndexedRecord> copy() {
    return this;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      Schema recordSchema,
      Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields,
      Option<Schema> schemaWithoutMetaFields) {
    String payloadClass = ConfigUtils.getPayloadClass(props);
    String[] orderingFields = ConfigUtils.getOrderingFields(props);
    setSchema(recordSchema);
    return HoodieAvroUtils.createHoodieRecordFromAvro(data, payloadClass, orderingFields, simpleKeyGenFieldsOpt, withOperation, partitionNameOp, populateMetaFields, schemaWithoutMetaFields);
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
                                                            Properties props, Option<BaseKeyGenerator> keyGen) {
    setSchema(recordSchema);
    GenericRecord record = (GenericRecord) data;
    String key;
    String partition;
    if (keyGen.isPresent() && !Boolean.parseBoolean(props.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      BaseKeyGenerator keyGeneratorOpt = keyGen.get();
      key = keyGeneratorOpt.getRecordKey(record);
      partition = keyGeneratorOpt.getPartitionPath(record);
    } else {
      key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    return new HoodieAvroIndexedRecord(hoodieKey, record);
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    if (metaData == null) {
      return Option.empty();
    }
    return metaData;
  }

  @Override
  public Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props, String[] orderingFields) {
    if (orderingFields == null || orderingFields.length == 0) {
      return OrderingValues.getDefault();
    }
    setSchema(recordSchema);
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(props.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    return OrderingValues.create(
        orderingFields,
        field -> (Comparable<?>) HoodieAvroUtils.getNestedFieldVal((GenericRecord) data, field, true, consistentLogicalTimestampEnabled));
  }

  private void setSchema(Schema recordSchema) {
    optimizedRecord.setSchema(recordSchema);
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) {
    setSchema(recordSchema);
    return Option.of(this);
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) {
    return HoodieAvroUtils.avroToBytesStream(data);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @SuppressWarnings("unchecked")
  @Override
  protected final void writeRecordPayload(IndexedRecord payload, Kryo kryo, Output output) {
    // NOTE: We're leveraging Spark's default [[GenericAvroSerializer]] to serialize Avro
    Serializer<SerializableIndexedRecord> avroSerializer = kryo.getSerializer(SerializableIndexedRecord.class);

    kryo.writeObjectOrNull(output, payload, avroSerializer);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @SuppressWarnings("unchecked")
  @Override
  protected final IndexedRecord readRecordPayload(Kryo kryo, Input input) {
    SerializableIndexedRecord data = kryo.readObjectOrNull(input, SerializableIndexedRecord.class);
    this.optimizedRecord = data;
    return data;
  }

  @Override
  public Object convertColumnValueForLogicalType(Schema fieldSchema,
                                                 Object fieldValue,
                                                 boolean keepConsistentLogicalTimestamp) {
    return HoodieAvroUtils.convertValueForAvroLogicalTypes(
        fieldSchema, fieldValue, keepConsistentLogicalTimestamp);
  }

  static void updateMetadataValuesInternal(GenericRecord avroRecord, MetadataValues metadataValues) {
    if (metadataValues.isEmpty()) {
      return; // no-op
    }

    String[] values = metadataValues.getValues();
    for (int pos = 0; pos < values.length; ++pos) {
      String value = values[pos];
      if (value != null) {
        avroRecord.put(HoodieMetadataField.values()[pos].getFieldName(), value);
      }
    }
  }
}
