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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RowDataAvroQueryContexts.RowDataQueryContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.JoinedRowData;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * Flink Engine-specific Implementations of `HoodieRecord`, which is expected to hold {@code RowData} as payload.
 */
public class HoodieFlinkRecord extends HoodieRecord<RowData> {
  private Comparable<?> orderingValue;

  public HoodieFlinkRecord(RowData rowData) {
    super(null, rowData);
  }

  public HoodieFlinkRecord(HoodieKey key, HoodieOperation op, RowData rowData) {
    super(key, rowData, op, Option.empty());
  }

  public HoodieFlinkRecord(HoodieKey key, HoodieOperation op, Comparable<?> orderingValue, RowData rowData) {
    super(key, rowData, op, Option.empty());
    this.orderingValue = orderingValue;
  }

  public HoodieFlinkRecord(HoodieKey key, RowData rowData) {
    super(key, rowData);
  }

  @Override
  public HoodieRecord<RowData> newInstance() {
    return new HoodieFlinkRecord(key, operation, orderingValue, data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieFlinkRecord(key, op, orderingValue, this.data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieOperation getOperation() {
    if (this.operation == null) {
      this.operation = HoodieOperation.fromValue(data.getRowKind().toByteValue());
    }
    return this.operation;
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    if (this.orderingValue == null) {
      String orderingField = ConfigUtils.getOrderingField(props);
      if (isNullOrEmpty(orderingField)) {
        this.orderingValue = DEFAULT_ORDERING_VALUE;
      } else {
        this.orderingValue = (Comparable<?>) getColumnValueAsJava(recordSchema, orderingField, props, false);
      }
    }
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
  protected void writeRecordPayload(RowData payload, Kryo kryo, Output output) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  protected RowData readRecordPayload(Kryo kryo, Input input) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object getColumnValueAsJava(Schema recordSchema, String column, Properties props) {
    return getColumnValueAsJava(recordSchema, column, props, true);
  }

  private Object getColumnValueAsJava(Schema recordSchema, String column, Properties props, boolean allowsNull) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone);
    return rowDataQueryContext.getFieldQueryContext(column).getValAsJava(data, allowsNull);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    int metaFieldSize = targetSchema.getFields().size() - recordSchema.getFields().size();
    GenericRowData metaRow = new GenericRowData(metaFieldSize);
    String[] metaVals = metadataValues.getValues();
    for (int i = 0; i < metaVals.length; i++) {
      if (metaVals[i] != null) {
        metaRow.setField(i, StringData.fromString(metaVals[i]));
      }
    }
    return new HoodieFlinkRecord(key, operation, orderingValue, new JoinedRowData(data.getRowKind(), metaRow, data));
  }

  @Override
  public HoodieRecord updateMetaField(Schema recordSchema, int ordinal, String value) {
    ValidationUtils.checkArgument(recordSchema.getField(RECORD_KEY_METADATA_FIELD) != null,
        "The record is expected to contain metadata fields.");
    GenericRowData rowData = (GenericRowData) getData();
    rowData.setField(ordinal, StringData.fromString(value));
    return this;
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
    return deleteField != null && data.getBoolean(deleteField.pos());
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) {
    return false;
  }

  @Override
  public HoodieRecord<RowData> copy() {
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
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone);
    IndexedRecord indexedRecord = (IndexedRecord) rowDataQueryContext.getRowDataToAvroConverter().convert(recordSchema, getData());
    return Option.of(new HoodieAvroIndexedRecord(getKey(), indexedRecord, getOperation(), getMetadata()));
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(Schema recordSchema, Properties props) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone);
    IndexedRecord indexedRecord = (IndexedRecord) rowDataQueryContext.getRowDataToAvroConverter().convert(recordSchema, getData());
    return HoodieAvroUtils.avroToBytesStream(indexedRecord);
  }
}
