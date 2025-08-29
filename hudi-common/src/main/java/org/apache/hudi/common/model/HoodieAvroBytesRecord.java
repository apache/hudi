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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieAvroIndexedRecord.updateMetadataValuesInternal;
import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

public class HoodieAvroBytesRecord extends HoodieRecord<byte[]> {
  public HoodieAvroBytesRecord(HoodieAvroBytesRecord other) {
    super(other);
  }

  public HoodieAvroBytesRecord(HoodieKey key,
                               byte[] data,
                               HoodieOperation op,
                               Option<Map<String, String>> metaData) {
    this.key = key;
    this.data = data;
    this.operation = op;
    this.metaData = metaData;
  }

  @Override
  public HoodieRecord<byte[]> newInstance() {
    return new HoodieAvroBytesRecord(this);
  }

  @Override
  public HoodieRecord<byte[]> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroBytesRecord(key, data, op, metaData);
  }

  @Override
  public HoodieRecord<byte[]> newInstance(HoodieKey key) {
    return new HoodieAvroBytesRecord(key, data, operation, metaData);
  }

  @Override
  protected Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props) {
    String orderingField = ConfigUtils.getOrderingField(props);
    if (isNullOrEmpty(orderingField)) {
      return DEFAULT_ORDERING_VALUE;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(props.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(avroRecord,
          orderingField, true, consistentLogicalTimestampEnabled);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO_BYTES;
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
    if (key != null && key.getRecordKey() != null) {
      return key.getRecordKey();
    }
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return avroRecord.get(keyFieldName).toString();
    } catch (IOException e) {
      throw new HoodieIOException("Fail to deserialize bytes to Avro data", e);
    }
  }

  @Override
  protected final void writeRecordPayload(byte[] payload, Kryo kryo, Output output) {
    // NOTE: We're leveraging Spark's default [[GenericAvroSerializer]] to serialize Avro
    Serializer<byte[]> avroSerializer = kryo.getSerializer(byte[].class);
    kryo.writeObjectOrNull(output, payload, avroSerializer);
  }

  @Override
  protected byte[] readRecordPayload(Kryo kryo, Input input) {
    // NOTE: We're leveraging Spark's default [[GenericAvroSerializer]] to serialize Avro
    Serializer<byte[]> avroSerializer = kryo.getSerializer(byte[].class);
    return kryo.readObjectOrNull(input, byte[].class, avroSerializer);
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema,
                                        Schema targetSchema,
                                        MetadataValues metadataValues,
                                        Properties props) {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, targetSchema);
      updateMetadataValuesInternal(newAvroRecord, metadataValues);
      return new HoodieAvroBytesRecord(key, HoodieAvroUtils.avroToBytes(newAvroRecord), operation, metaData);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema,
                                                 Properties props,
                                                 Schema newSchema,
                                                 Map<String, String> renameCols) {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      GenericRecord newRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newSchema, renameCols);
      return new HoodieAvroBytesRecord(key, HoodieAvroUtils.avroToBytes(newRecord), operation, metaData);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    return data == null || data.length == 0;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return avroRecord.equals(SENTINEL);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public HoodieRecord<byte[]> copy() {
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return metaData;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema,
                                                            Properties props,
                                                            Option<Pair<String, String>> simpleKeyGenFieldsOpt,
                                                            Boolean withOperation,
                                                            Option<String> partitionNameOp,
                                                            Boolean populateMetaFieldsOp,
                                                            Option<Schema> schemaWithoutMetaFields) throws IOException {
    String payloadClass = ConfigUtils.getPayloadClass(props);
    String preCombineField = ConfigUtils.getOrderingField(props);
    GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    return HoodieAvroUtils.createHoodieRecordFromAvro(
        avroRecord, payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation,
        partitionNameOp, populateMetaFieldsOp, schemaWithoutMetaFields);
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
                                                            Properties props,
                                                            Option<BaseKeyGenerator> keyGen) {
    GenericRecord avroRecord;
    try {
      avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
    String key;
    String partition;
    if (keyGen.isPresent() && !Boolean.parseBoolean(
        props.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      BaseKeyGenerator keyGeneratorOpt = keyGen.get();
      key = keyGeneratorOpt.getRecordKey(avroRecord);
      partition = keyGeneratorOpt.getPartitionPath(avroRecord);
    } else {
      key = avroRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      partition = avroRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);
    return new HoodieAvroIndexedRecord(hoodieKey, avroRecord);
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      avroRecord.put(keyFieldName, StringUtils.EMPTY_STRING);
      data = HoodieAvroUtils.avroToBytes(avroRecord);
      return this;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    try {
      GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
      return Option.of(new HoodieAvroIndexedRecord(key, avroRecord, operation, metaData));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize bytes to Avro record", e);
    }
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) throws IOException {
    GenericRecord avroRecord = HoodieAvroUtils.bytesToAvro(data, recordSchema);
    return HoodieAvroUtils.avroToBytesStream(avroRecord);
  }
}
