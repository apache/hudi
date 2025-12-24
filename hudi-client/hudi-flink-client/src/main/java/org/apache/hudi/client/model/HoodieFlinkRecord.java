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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.util.RowDataAvroQueryContexts;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * Flink Engine-specific Implementations of `HoodieRecord`, which is expected to hold {@code RowData} as payload.
 */
public class HoodieFlinkRecord extends HoodieRecord<RowData> {
  private RowDataSerializer rowDataSerializer;
  protected transient Comparable<?> orderingValue;
  private boolean copy = false;
  public static final int DEFAULT_ORDERING_VALUE = 0;
  private StringData recordKeyStringData;

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

  public HoodieFlinkRecord(HoodieKey key, RowData rowData, RowDataSerializer rowDataSerializer) {
    super(key, rowData);
    this.rowDataSerializer = rowDataSerializer;
  }

  public HoodieFlinkRecord(StringData recordKey, RowData record) {
    super(null, record);
    this.recordKeyStringData = recordKey;
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
  public Comparable<?> doGetOrderingValue(Schema recordSchema, Properties props) {
    String orderingField = ConfigUtils.getOrderingField(props);
    if (isNullOrEmpty(orderingField)) {
      return DEFAULT_ORDERING_VALUE;
    } else {
      return (Comparable<?>) getColumnValueAsJava(recordSchema, orderingField, props, false);
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.FLINK;
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  public StringData getRecordKeyStringData() {
    return this.recordKeyStringData;
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return getRecordKey();
  }

  @Override
  protected void writeRecordPayload(RowData payload, Kryo kryo, Output output) {

  }

  @Override
  protected RowData readRecordPayload(Kryo kryo, Input input) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    Object[] fieldValues = new Object[columns.length];
    for (int i = 0; i < columns.length; i++) {
      fieldValues[i] = getColumnValueAsJava(recordSchema, columns[i], new Properties(), true);
    }
    return fieldValues;
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
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
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
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return false;
  }

  @Override
  public HoodieFlinkRecord copy() {
    if (!copy) {
      this.data = rowDataSerializer.copy(data);
      this.copy = true;
    }
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(Schema recordSchema, Properties props, Option<Pair<String, String>> simpleKeyGenFieldsOpt, Boolean withOperation,
                                                            Option<String> partitionNameOp, Boolean populateMetaFieldsOp) throws IOException {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) throws IOException {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) throws IOException {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  private Object getColumnValueAsJava(Schema recordSchema, String column, Properties props, boolean allowsNull) {
    RowDataAvroQueryContexts.RowDataQueryContext rowDataQueryContext = RowDataAvroQueryContexts.fromAvroSchema(recordSchema);
    return rowDataQueryContext.getFieldQueryContext(column).getValAsJava(data, allowsNull);
  }
}
