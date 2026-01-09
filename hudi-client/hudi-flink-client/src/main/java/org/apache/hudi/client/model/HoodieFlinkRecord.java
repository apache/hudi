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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.format.FlinkRecordContext;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RowDataAvroQueryContexts.RowDataQueryContext;
import org.apache.hudi.util.RowProjection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Flink Engine-specific Implementations of `HoodieRecord`, which is expected to hold {@code RowData} as payload.
 */
public class HoodieFlinkRecord extends HoodieRecord<RowData> {

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

  public HoodieFlinkRecord(HoodieKey key, HoodieOperation op, Comparable<?> orderingValue, RowData rowData, boolean isDelete) {
    super(key, rowData, op, isDelete, Option.empty());
    this.orderingValue = orderingValue;
  }

  @Override
  public HoodieRecord<RowData> newInstance() {
    return new HoodieFlinkRecord(key, operation, orderingValue, data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieFlinkRecord(key, op, orderingValue, data);
  }

  @Override
  public HoodieRecord<RowData> newInstance(HoodieKey key) {
    return new HoodieFlinkRecord(key, operation, orderingValue, data);
  }

  @Override
  protected Comparable<?> doGetOrderingValue(HoodieSchema recordSchema, Properties props, String[] orderingFields) {
    if (orderingFields == null) {
      return OrderingValues.getDefault();
    } else {
      return OrderingValues.create(orderingFields, field -> {
        if (recordSchema.getField(field).isEmpty()) {
          return OrderingValues.getDefault();
        }
        return (Comparable<?>) getColumnValue(recordSchema.toAvroSchema(), field, props);
      });
    }
  }

  @Override
  public Comparable<?> getOrderingValueAsJava(HoodieSchema recordSchema, Properties props, String[] orderingFields) {
    if (orderingFields == null) {
      return OrderingValues.getDefault();
    } else {
      return OrderingValues.create(orderingFields, field -> {
        if (recordSchema.getField(field).isEmpty()) {
          return OrderingValues.getDefault();
        }
        return (Comparable<?>) getColumnValueAsJava(recordSchema, field, props);
      });
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.FLINK;
  }

  @Override
  public String getRecordKey(HoodieSchema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key == null) {
      ValidationUtils.checkArgument(recordSchema.getField(RECORD_KEY_METADATA_FIELD).isPresent(),
          "There should be `_hoodie_record_key` in record schema.");
      String recordKey = Objects.toString(data.getString(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal()));
      key = new HoodieKey(recordKey, null);
    }
    return getRecordKey();
  }

  @Override
  public String getRecordKey(HoodieSchema recordSchema, String keyFieldName) {
    if (key == null) {
      String recordKey = Objects.toString(RowDataAvroQueryContexts.fromAvroSchema(recordSchema.toAvroSchema()).getFieldQueryContext(keyFieldName).getFieldGetter().getFieldOrNull(data));
      key = new HoodieKey(recordKey, null);
    }
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
  public Object convertColumnValueForLogicalType(HoodieSchema fieldSchema,
                                                 Object fieldValue,
                                                 boolean keepConsistentLogicalTimestamp) {
    if (fieldValue == null) {
      return null;
    }
    
    HoodieSchemaType schemaType = fieldSchema.getType();
    
    if (schemaType == HoodieSchemaType.DATE) {
      return LocalDate.ofEpochDay(((Integer) fieldValue).longValue());
    } else if (schemaType == HoodieSchemaType.TIMESTAMP && keepConsistentLogicalTimestamp) {
      HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) fieldSchema;
      TimestampData ts = (TimestampData) fieldValue;
      if (timestampSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS) {
        return ts.getMillisecond();
      } else if (timestampSchema.getPrecision() == HoodieSchema.TimePrecision.MICROS) {
        return ts.getMillisecond() / 1000;
      }
    } else if (schemaType == HoodieSchemaType.DECIMAL) {
      return ((DecimalData) fieldValue).toBigDecimal();
    }
    return fieldValue;
  }

  @Override
  public Object[] getColumnValues(HoodieSchema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object getColumnValueAsJava(HoodieSchema recordSchema, String column, Properties props) {
    return getColumnValueAsJava(recordSchema.toAvroSchema(), column, props, true);
  }

  private Object getColumnValueAsJava(Schema recordSchema, String column, Properties props, boolean allowsNull) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone);
    return rowDataQueryContext.getFieldQueryContext(column).getValAsJava(data, allowsNull);
  }

  private Object getColumnValue(Schema recordSchema, String column, Properties props) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone);
    return rowDataQueryContext.getFieldQueryContext(column).getFieldGetter().getFieldOrNull(data);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, HoodieSchema targetSchema) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord prependMetaFields(HoodieSchema recordSchema, HoodieSchema targetSchema, MetadataValues metadataValues, Properties props) {
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
  public HoodieRecord updateMetaField(HoodieSchema recordSchema, int ordinal, String value) {
    String[] metaVals = new String[HoodieRecord.HOODIE_META_COLUMNS.size()];
    metaVals[ordinal] = value;
    boolean withOperation = recordSchema.getField(OPERATION_METADATA_FIELD).isPresent();
    RowData rowData = new HoodieRowDataWithUpdatedMetaField(metaVals, ordinal, getData(), withOperation);
    return new HoodieFlinkRecord(getKey(), getOperation(), orderingValue, rowData);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(HoodieSchema recordSchema, Properties props, HoodieSchema newSchema, Map<String, String> renameCols) {
    RowProjection rowProjection = RowDataAvroQueryContexts.getRowProjection(recordSchema.toAvroSchema(), newSchema.toAvroSchema(), renameCols);
    RowData newRow = rowProjection.project(getData());
    return new HoodieFlinkRecord(getKey(), getOperation(), newRow);
  }

  @Override
  protected boolean checkIsDelete(DeleteContext deleteContext, Properties props) {
    if (data == null || HoodieOperation.isDelete(getOperation())) {
      return true;
    }

    return FlinkRecordContext.getDeleteCheckingInstance().isDeleteRecord(data, deleteContext);
  }

  @Override
  public boolean shouldIgnore(HoodieSchema recordSchema, Properties props) {
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
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(HoodieSchema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp, Option<HoodieSchema> schemaWithoutMetaFields) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(HoodieSchema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord truncateRecordKey(HoodieSchema recordSchema, Properties props, String keyFieldName) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(HoodieSchema recordSchema, Properties props) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema.toAvroSchema(), utcTimezone);
    IndexedRecord indexedRecord = (IndexedRecord) rowDataQueryContext.getRowDataToAvroConverter().convert(recordSchema.toAvroSchema(), getData());
    return Option.of(new HoodieAvroIndexedRecord(getKey(), indexedRecord, getOperation(), getMetadata(), orderingValue, isDelete));
  }

  @Override
  public ByteArrayOutputStream getAvroBytes(HoodieSchema recordSchema, Properties props) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema.toAvroSchema(), utcTimezone);
    IndexedRecord indexedRecord = (IndexedRecord) rowDataQueryContext.getRowDataToAvroConverter().convert(recordSchema.toAvroSchema(), getData());
    return HoodieAvroUtils.avroToBytesStream(indexedRecord);
  }
}
