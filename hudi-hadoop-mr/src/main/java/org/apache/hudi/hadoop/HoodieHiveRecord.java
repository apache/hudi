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
import org.apache.hudi.hadoop.utils.HoodieArrayWritableAvroUtils;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.keygen.BaseKeyGenerator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * {@link HoodieRecord} implementation for Hive records of {@link ArrayWritable}.
 */
public class HoodieHiveRecord extends HoodieRecord<ArrayWritable> {

  private boolean copy;
  private final boolean isDeleted;

  public boolean isDeleted() {
    return isDeleted;
  }

  private final ArrayWritableObjectInspector objectInspector;

  private final ObjectInspectorCache objectInspectorCache;

  protected Schema schema;

  public HoodieHiveRecord(HoodieKey key, ArrayWritable data, Schema schema, ObjectInspectorCache objectInspectorCache) {
    super(key, data);
    this.objectInspector = objectInspectorCache.getObjectInspector(schema);
    this.objectInspectorCache = objectInspectorCache;
    this.schema = schema;
    this.copy = false;
    isDeleted = data == null;
  }

  private HoodieHiveRecord(HoodieKey key, ArrayWritable data, Schema schema, HoodieOperation operation, boolean isCopy,
                           ArrayWritableObjectInspector objectInspector, ObjectInspectorCache objectInspectorCache) {
    super(key, data, operation, Option.empty());
    this.schema = schema;
    this.copy = isCopy;
    isDeleted = data == null;
    this.objectInspector = objectInspector;
    this.objectInspectorCache = objectInspectorCache;
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance() {
    return new HoodieHiveRecord(this.key, this.data, this.schema, this.operation, this.copy, this.objectInspector, this.objectInspectorCache);
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance(HoodieKey key, HoodieOperation op) {
    throw new UnsupportedOperationException("ObjectInspector is needed for HoodieHiveRecord");
  }

  @Override
  public HoodieRecord<ArrayWritable> newInstance(HoodieKey key) {
    throw new UnsupportedOperationException("ObjectInspector is needed for HoodieHiveRecord");
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    String orderingField = ConfigUtils.getOrderingField(props);
    if (orderingField == null) {
      return 0;
      //throw new IllegalArgumentException("Ordering Field is not set. Precombine must be set. (If you are using a custom record merger it might be something else)");
    }
    return (Comparable<?>) getValue(ConfigUtils.getOrderingField(props));
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.HIVE;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  protected void writeRecordPayload(ArrayWritable payload, Kryo kryo, Output output) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  protected ArrayWritable readRecordPayload(Kryo kryo, Input input) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    Object[] objects = new Object[columns.length];
    for (int i = 0; i < objects.length; i++) {
      objects[i] = getValue(columns[i]);
    }
    return objects;
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    if (null == data) {
      return true;
    }
    if (recordSchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) == null) {
      return false;
    }
    Object deleteMarker = getValue(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    return deleteMarker instanceof BooleanWritable && ((BooleanWritable) deleteMarker).get();
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieRecord<ArrayWritable> copy() {
    if (!copy) {
      this.data = new ArrayWritable(Writable.class, Arrays.copyOf(this.data.get(), this.data.get().length));
      this.copy = true;
    }
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    // TODO HUDI-5282 support metaData
    return Option.empty();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<Schema> schemaWithoutMetaFields) throws IOException {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    data.get()[recordSchema.getIndexNamed(keyFieldName)] = new Text();
    return this;
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    throw new UnsupportedOperationException("Not supported for HoodieHiveRecord");
  }

  private Object getValue(String name) {
    return HoodieArrayWritableAvroUtils.getWritableValue(data, objectInspector, name);
  }

  protected Schema getSchema() {
    return schema;
  }
}
