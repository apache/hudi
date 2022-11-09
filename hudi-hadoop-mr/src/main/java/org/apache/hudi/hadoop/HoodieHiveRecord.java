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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.hadoop.io.ArrayWritable;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Hive specific implementation of {@link HoodieRecord}
 */
public class HoodieHiveRecord extends HoodieRecord<ArrayWritable> {

  private transient Schema schema;

  public HoodieHiveRecord(HoodieRecord<ArrayWritable> record) {
    super(record);
  }

  public HoodieHiveRecord(ArrayWritable arrayWritable) {
    this(arrayWritable, null);
  }

  public HoodieHiveRecord(ArrayWritable arrayWritable, Schema schema) {
    super(null, arrayWritable);
    this.schema = schema;
  }

  public HoodieHiveRecord(HoodieKey key, ArrayWritable arrayWritable, Schema schema) {
    super(key, arrayWritable);
    this.schema = schema;
  }

  public HoodieHiveRecord(HoodieKey key, ArrayWritable arrayWritable, HoodieOperation operation, Schema schema) {
    super(key, arrayWritable, operation);
    this.schema = schema;
  }

  public HoodieHiveRecord() {
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance() {
    return new HoodieHiveRecord(this);
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieHiveRecord(key, this.data, op, this.schema);
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance(HoodieKey key) {
    return new HoodieHiveRecord(key, this.data, this.schema);
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    String orderingField = ConfigUtils.getOrderingField(props);
    if (recordSchema.getField(orderingField) == null) {
      return 0;
    }
    // TODO: handle nested fields
    int index = recordSchema.getField(orderingField).pos();
    return String.valueOf(data.get()[index]);
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.HIVE;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return getRecordKey();
    }
    return String.valueOf(data.get()[HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal()]);
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    if (key != null) {
      return getRecordKey();
    }
    if (recordSchema.getField(keyFieldName) == null) {
      throw new HoodieException(String.format("Record key field: %s not present in schema: %s", keyFieldName, recordSchema));
    }
    // TODO: handle nested fields
    int index = recordSchema.getField(keyFieldName).pos();
    return String.valueOf(data.get()[index]);
  }

  @Override
  protected void writeRecordPayload(ArrayWritable payload, Kryo kryo, Output output) {

  }

  @Override
  protected ArrayWritable readRecordPayload(Kryo kryo, Input input) {
    return kryo.readObjectOrNull(input, ArrayWritable.class);
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return new Object[0];
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    return null;
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    return null;
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord<ArrayWritable> copy() {
    return null;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return null;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    return null;
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    return null;
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    return null;
  }
}
