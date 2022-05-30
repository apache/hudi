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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    super(key, data, operation, null);
  }

  public HoodieAvroIndexedRecord(HoodieRecord<IndexedRecord> record) {
    super(record);
  }

  public HoodieAvroIndexedRecord() {
  }

  @Override
  public Option<IndexedRecord> toIndexedRecord(Schema schema, Properties prop) {
    return Option.of(data);
  }

  public Option<IndexedRecord> toIndexedRecord() {
    return Option.of(data);
  }

  @Override
  public HoodieRecord newInstance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key, HoodieOperation op) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey((GenericRecord) data) : ((GenericRecord) data).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return Option.ofNullable(data.getSchema().getField(keyFieldName))
        .map(keyField -> data.get(keyField.pos()))
        .map(Object::toString).orElse(null);
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema readerSchema, Schema writerSchema) throws IOException {
    ValidationUtils.checkState(other instanceof HoodieAvroIndexedRecord);
    GenericRecord record = HoodieAvroUtils.stitchRecords((GenericRecord) data, (GenericRecord) other.getData(), writerSchema);
    return new HoodieAvroIndexedRecord(record);
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    GenericRecord avroPayloadInNewSchema =
        HoodieAvroUtils.rewriteRecord((GenericRecord) data, targetSchema);
    return new HoodieAvroIndexedRecord(avroPayloadInNewSchema);
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    GenericRecord rewriteRecord = schemaOnReadEnabled ? HoodieAvroUtils.rewriteRecordWithNewSchema(data, writeSchemaWithMetaFields, new HashMap<>())
        : HoodieAvroUtils.rewriteRecord((GenericRecord) data, writeSchemaWithMetaFields);
    return new HoodieAvroIndexedRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    GenericRecord rewriteRecord = schemaOnReadEnabled ? HoodieAvroUtils.rewriteEvolutionRecordWithMetadata((GenericRecord) data, writeSchemaWithMetaFields, fileName)
        : HoodieAvroUtils.rewriteRecordWithMetadata((GenericRecord) data, writeSchemaWithMetaFields, fileName);
    return new HoodieAvroIndexedRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols) throws IOException {
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(data, newSchema, renameCols);
    return new HoodieAvroIndexedRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols, Mapper mapper) throws IOException {
    GenericRecord oldRecord = (GenericRecord) getData();
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(oldRecord, newSchema, renameCols);
    return mapper.apply(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema) throws IOException {
    GenericRecord oldRecord = (GenericRecord) data;
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecord(oldRecord, newSchema);
    return new HoodieAvroIndexedRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord addMetadataValues(Schema recordSchema, Properties prop, Map<HoodieMetadataField, String> metadataValues) throws IOException {
    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        ((GenericRecord) data).put(metadataField.getFieldName(), value);
      }
    });

    return new HoodieAvroIndexedRecord(data);
  }

  @Override
  public HoodieRecord overrideMetadataFieldValue(Schema recordSchema, Properties prop, int pos, String newValue) throws IOException {
    data.put(pos, newValue);
    return this;
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties prop) throws IOException {
    return getData().equals(SENTINEL);
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public boolean isPresent(Schema schema, Properties prop) {
    return true;
  }
}
