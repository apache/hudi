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

package org.apache.hudi.table.format;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.DefaultJavaTypeConverter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.OrderingValueEngineTypeConverter;
import org.apache.hudi.util.RecordKeyToRowDataConverter;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RowDataUtils;
import org.apache.hudi.util.RowProjection;
import org.apache.hudi.util.SchemaEvolvingRowDataProjection;

import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class FlinkRecordContext extends RecordContext<RowData> {
  private static final FlinkRecordContext DELETE_CHECKING_INSTANCE = new FlinkRecordContext(true);

  private final boolean utcTimezone;
  // the converter is used to create a RowData contains primary key fields only
  // for DELETE cases, it'll not be initialized if primary key semantics is lost.
  // For e.g, if the pk fields are [a, b] but user only select a, then the pk
  // semantics is lost.
  @Setter
  private RecordKeyToRowDataConverter recordKeyRowConverter;
  private OrderingValueEngineTypeConverter orderingValueConverter;

  public FlinkRecordContext(HoodieTableConfig tableConfig, StorageConfiguration<?> storageConf) {
    super(tableConfig, new DefaultJavaTypeConverter());
    this.utcTimezone = storageConf.getBoolean("read.utc-timezone",true);
  }

  private FlinkRecordContext(boolean utcTimezone) {
    super(new DefaultJavaTypeConverter());
    this.utcTimezone = utcTimezone;
  }

  public static FlinkRecordContext getDeleteCheckingInstance() {
    return DELETE_CHECKING_INSTANCE;
  }

  @Override
  public Object getValue(RowData record, HoodieSchema schema, String fieldName) {
    RowDataAvroQueryContexts.FieldQueryContext fieldQueryContext =
        RowDataAvroQueryContexts.fromAvroSchema(schema.toAvroSchema(), utcTimezone).getFieldQueryContext(fieldName);
    if (fieldQueryContext == null) {
      return null;
    } else {
      return fieldQueryContext.getFieldGetter().getFieldOrNull(record);
    }
  }

  @Override
  public String getMetaFieldValue(RowData record, int pos) {
    return record.getString(pos).toString();
  }

  @Override
  public Comparable convertValueToEngineType(Comparable value) {
    return (Comparable) RowDataUtils.convertValueToFlinkType(value);
  }

  @Override
  public Comparable convertOrderingValueToEngineType(Comparable value) {
    return orderingValueConverter.convert(value);
  }

  @Override
  public GenericRecord convertToAvroRecord(RowData record, HoodieSchema schema) {
    return (GenericRecord) RowDataAvroQueryContexts.fromAvroSchema(schema.toAvroSchema()).getRowDataToAvroConverter().convert(schema.toAvroSchema(), record);
  }

  @Override
  public RowData getDeleteRow(String recordKey) {
    // don't need to emit record key row if primary key semantic is lost
    if (recordKeyRowConverter == null) {
      return null;
    }
    RowData recordKeyRow = recordKeyRowConverter.convert(recordKey);
    recordKeyRow.setRowKind(RowKind.DELETE);
    return recordKeyRow;
  }

  @Override
  public RowData convertAvroRecord(IndexedRecord avroRecord) {
    Schema recordSchema = avroRecord.getSchema();
    AvroToRowDataConverters.AvroToRowDataConverter converter = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone).getAvroToRowDataConverter();
    RowData rowData = (RowData) converter.convert(avroRecord);
    Schema.Field operationField = recordSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD);
    if (operationField != null) {
      HoodieOperation operation = HoodieOperation.fromName(rowData.getString(operationField.pos()).toString());
      rowData.setRowKind(RowKind.fromByteValue(operation.getValue()));
    }
    return rowData;
  }

  @Override
  public HoodieRecord<RowData> constructHoodieRecord(BufferedRecord<RowData> bufferedRecord, String partitionPath) {
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    // delete record
    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(hoodieKey, bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), HoodieRecord.HoodieRecordType.FLINK);
    }
    RowData rowData = bufferedRecord.getRecord();
    return new HoodieFlinkRecord(hoodieKey, bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), rowData, bufferedRecord.isDelete());
  }

  @Override
  public RowData constructEngineRecord(HoodieSchema recordSchema, Object[] fieldValues) {
    return GenericRowData.of(fieldValues);
  }

  @Override
  public RowData mergeWithEngineRecord(HoodieSchema schema,
                                       Map<Integer, Object> updateValues,
                                       BufferedRecord<RowData> baseRecord) {
    GenericRowData genericRowData = new GenericRowData(schema.getFields().size());
    for (HoodieSchemaField field : schema.getFields()) {
      int pos = field.pos();
      if (updateValues.containsKey(pos)) {
        genericRowData.setField(pos, updateValues.get(pos));
      } else {
        genericRowData.setField(pos, getValue(baseRecord.getRecord(), schema, field.name()));
      }
    }
    return genericRowData;
  }

  @Override
  public RowData seal(RowData rowData) {
    if (rowData instanceof BinaryRowData) {
      return ((BinaryRowData) rowData).copy();
    }
    return rowData;
  }

  @Override
  public RowData toBinaryRow(HoodieSchema schema, RowData record) {
    if (record instanceof BinaryRowData) {
      return record;
    }
    RowDataSerializer rowDataSerializer = RowDataAvroQueryContexts.getRowDataSerializer(schema.toAvroSchema());
    return rowDataSerializer.toBinaryRow(record);
  }

  /**
   * Creates a function that will reorder records of schema "from" to schema of "to".
   * It's possible there exist fields in `to` schema, but not in `from` schema because of schema
   * evolution.
   *
   * @param from           the schema of records to be passed into UnaryOperator
   * @param to             the schema of records produced by UnaryOperator
   * @param renamedColumns map of renamed columns where the key is the new name from the query and
   *                       the value is the old name that exists in the file
   * @return a function that takes in a record and returns the record with reordered columns
   */
  @Override
  public UnaryOperator<RowData> projectRecord(HoodieSchema from, HoodieSchema to, Map<String, String> renamedColumns) {
    RowType fromType = (RowType) RowDataAvroQueryContexts.fromAvroSchema(from.toAvroSchema()).getRowType().getLogicalType();
    RowType toType =  (RowType) RowDataAvroQueryContexts.fromAvroSchema(to.toAvroSchema()).getRowType().getLogicalType();
    RowProjection rowProjection = SchemaEvolvingRowDataProjection.instance(fromType, toType, renamedColumns);
    return rowProjection::project;
  }

  public void initOrderingValueConverter(HoodieSchema dataSchema, List<String> orderingFieldNames) {
    this.orderingValueConverter = OrderingValueEngineTypeConverter.create(dataSchema, orderingFieldNames, utcTimezone);
  }
}
