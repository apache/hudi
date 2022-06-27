/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.commmon.model;

import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.util.HoodieSparkRecordUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieDefaultCatalystExpressionUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils.NestedFieldPath;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Spark Engine-specific Implementations of `HoodieRecord`.
 */
public class HoodieSparkRecord extends HoodieRecord<InternalRow> {

  private StructType structType;

  public HoodieSparkRecord(InternalRow data, StructType schema) {
    super(null, data);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema) {
    super(key, data);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema, HoodieOperation operation) {
    super(key, data, operation);
    this.structType = schema;
  }

  public HoodieSparkRecord(HoodieSparkRecord record) {
    super(record);
    this.structType = record.structType;
  }

  @Override
  public HoodieRecord<InternalRow> newInstance() {
    return new HoodieSparkRecord(this);
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieSparkRecord(key, data, structType, op);
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key) {
    return new HoodieSparkRecord(key, data, structType);
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return getRecordKey();
    }
    return keyGeneratorOpt.isPresent() ? ((SparkKeyGeneratorInterface) keyGeneratorOpt.get())
        .getRecordKey(data, structType).toString() : data.getString(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal());
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    if (key != null) {
      return getRecordKey();
    }
    DataType dataType = structType.apply(keyFieldName).dataType();
    int pos = structType.fieldIndex(keyFieldName);
    return data.get(pos, dataType).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  @Override
  public Object getRecordColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return HoodieSparkRecordUtils.getRecordColumnValues(data, columns, structType, consistentLogicalTimestampEnabled);
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema targetSchema) throws IOException {
    StructType otherStructType = ((HoodieSparkRecord) other).getStructType();
    StructType writerStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    InternalRow mergeRow = HoodieInternalRowUtils.stitchRecords(data, structType, (InternalRow) other.getData(), otherStructType, writerStructType);
    return new HoodieSparkRecord(getKey(), mergeRow, writerStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    StructType targetStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    UTF8String[] metaFields = extractMetaField(targetStructType);
    if (metaFields.length == 0) {
      throw new UnsupportedOperationException();
    }

    InternalRow resultRow;
    if (extractMetaField(structType).length == 0) {
      resultRow = new HoodieInternalRow(metaFields, data, false);
    } else {
      resultRow = new HoodieInternalRow(metaFields, data, true);
    }

    return new HoodieSparkRecord(getKey(), resultRow, targetStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, structType, newStructType, renameCols);
    UnsafeProjection unsafeConvert = HoodieInternalRowUtils.getCachedUnsafeConvert(newStructType);
    InternalRow resultRow = unsafeConvert.apply(rewriteRow);
    UTF8String[] metaFields = extractMetaField(newStructType);
    if (metaFields.length > 0) {
      resultRow = new HoodieInternalRow(metaFields, data, true);
    }

    return new HoodieSparkRecord(getKey(), resultRow, newStructType, getOperation());
  }

  @Override
  public HoodieRecord updateValues(Schema recordSchema, Properties props, Map<String, String> metadataValues) throws IOException {
    metadataValues.forEach((key, value) -> {
      int pos = structType.fieldIndex(key);
      if (value != null) {
        data.update(pos, CatalystTypeConverters.convertToCatalyst(value));
      }
    });

    return new HoodieSparkRecord(getKey(), data, structType, getOperation());
  }

  @Override
  public boolean isDelete(Schema schema, Properties props) throws IOException {
    if (null == data) {
      return true;
    }
    if (schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) == null) {
      return false;
    }
    Object deleteMarker = data.get(schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos(), BooleanType);
    return deleteMarker instanceof Boolean && (boolean) deleteMarker;
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties props) throws IOException {
    if (data != null && data.equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public HoodieRecord getKeyWithParams(
      Schema schema, Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields) {
    if (populateMetaFields) {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, simpleKeyGenFieldsOpt.get(), withOperation, Option.empty());
    } else {
      return HoodieSparkRecordUtils.convertToHoodieSparkRecord(structType, data, withOperation, partitionNameOp);
    }
  }

  @Override
  public HoodieRecord getKeyWithKeyGen(Properties props, Option<BaseKeyGenerator> keyGen) {
    String key;
    String partition;
    if (keyGen.isPresent() && !Boolean.parseBoolean(props.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      SparkKeyGeneratorInterface keyGenerator = (SparkKeyGeneratorInterface) keyGen.get();
      key = keyGenerator.getRecordKey(data, structType).toString();
      partition = keyGenerator.getPartitionPath(data, structType).toString();
    } else {
      key = data.get(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal(), StringType).toString();
      partition = data.get(HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.ordinal(), StringType).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);
    return new HoodieSparkRecord(hoodieKey, data, structType, getOperation());
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema schema, Properties prop) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparable<?> getOrderingValue(Properties props) {
    String orderingField = ConfigUtils.getOrderingField(props);
    if (!HoodieDefaultCatalystExpressionUtils.existField(structType, orderingField)) {
      return 0;
    } else {
      NestedFieldPath nestedFieldPath = HoodieInternalRowUtils.getCachedPosList(structType,
          orderingField);
      Comparable<?> value = (Comparable<?>) HoodieUnsafeRowUtils.getNestedInternalRowValue(
          data, nestedFieldPath);
      return value;
    }
  }

  public StructType getStructType() {
    return structType;
  }

  private UTF8String[] extractMetaField(StructType structType) {
    return HOODIE_META_COLUMNS_WITH_OPERATION.stream()
        .filter(f -> HoodieDefaultCatalystExpressionUtils.existField(structType, f))
        .map(UTF8String::fromString)
        .toArray(UTF8String[]::new);
  }
}
