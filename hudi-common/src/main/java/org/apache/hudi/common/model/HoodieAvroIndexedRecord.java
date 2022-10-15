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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;

/**
 * This only use by reader returning.
 */
public class HoodieAvroIndexedRecord extends HoodieRecord<IndexedRecord> {

  public HoodieAvroIndexedRecord(IndexedRecord data) {
    super(null, data);
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data) {
    super(key, data);
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieAvroIndexedRecord(HoodieRecord<IndexedRecord> record) {
    super(record);
  }

  public HoodieAvroIndexedRecord() {
  }

  @Override
  public HoodieRecord newInstance() {
    return new HoodieAvroIndexedRecord(this);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroIndexedRecord(key, data, op);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key) {
    return new HoodieAvroIndexedRecord(key, data);
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey((GenericRecord) data) : ((GenericRecord) data).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return Option.ofNullable(data.getSchema().getField(keyFieldName))
        .map(keyField -> data.get(keyField.pos()))
        .map(Object::toString).orElse(null);
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other,
      Schema targetSchema) throws IOException {
    GenericRecord record = HoodieAvroUtils.stitchRecords((GenericRecord) data, (GenericRecord) other.getData(), targetSchema);
    return new HoodieAvroIndexedRecord(record);
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    GenericRecord genericRecord = HoodieAvroUtils.rewriteRecord((GenericRecord) data, targetSchema);
    return new HoodieAvroIndexedRecord(genericRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    GenericRecord genericRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(data, newSchema, renameCols);
    return new HoodieAvroIndexedRecord(genericRecord);
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    metadataValues.getKv().forEach((key, value) -> {
      if (value != null) {
        ((GenericRecord) data).put(key, value);
      }
    });

    return new HoodieAvroIndexedRecord(data);
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    ((GenericRecord) data).put(keyFieldName, StringUtils.EMPTY_STRING);
    return this;
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) {
    return false;
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
      Boolean populateMetaFields) {
    String payloadClass = ConfigUtils.getPayloadClass(props);
    String preCombineField = ConfigUtils.getOrderingField(props);
    return HoodieAvroUtils.createHoodieRecordFromAvro(data, payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation, partitionNameOp, populateMetaFields);
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
      Properties props, Option<BaseKeyGenerator> keyGen) {
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

    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(props.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    return (Comparable<?>) HoodieAvroUtils.getNestedFieldVal((GenericRecord) data,
        ConfigUtils.getOrderingField(props),
        true, consistentLogicalTimestampEnabled);
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) {
    return Option.of(this);
  }
}
