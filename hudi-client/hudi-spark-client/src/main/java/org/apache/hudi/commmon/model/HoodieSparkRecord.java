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

import com.esotericsoftware.kryo.DefaultSerializer;
import java.util.List;
import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.util.HoodieSparkRecordUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieCatalystExpressionUtils$;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils.NestedFieldPath;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.hudi.HoodieSparkRecordSerializer;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.util.HoodieSparkRecordUtils.getNullableValAsString;
import static org.apache.hudi.util.HoodieSparkRecordUtils.getValue;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Spark Engine-specific Implementations of `HoodieRecord`.
 */
@DefaultSerializer(HoodieSparkRecordSerializer.class)
public class HoodieSparkRecord extends HoodieRecord<InternalRow> {

  private StructType structType = null;

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
  public HoodieSparkRecord newInstance() {
    return new HoodieSparkRecord(this);
  }

  @Override
  public HoodieSparkRecord newInstance(InternalRow data) {
    return new HoodieSparkRecord(key, data, getStructType(), operation);
  }

  @Override
  public HoodieSparkRecord newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieSparkRecord(key, data, getStructType(), op);
  }

  @Override
  public HoodieSparkRecord newInstance(HoodieKey key) {
    return new HoodieSparkRecord(key, data, getStructType());
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return getRecordKey();
    }
    return keyGeneratorOpt.isPresent() ? ((SparkKeyGeneratorInterface) keyGeneratorOpt.get())
        .getRecordKey(data, getStructType()).toString() : data.getString(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal());
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    if (key != null) {
      return getRecordKey();
    }
    DataType dataType = getStructType().apply(keyFieldName).dataType();
    int pos = getStructType().fieldIndex(keyFieldName);
    return data.get(pos, dataType).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  @Override
  public List<Object> getRecordColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    return HoodieSparkRecordUtils.getRecordColumnValues(data, columns, getStructType(), consistentLogicalTimestampEnabled);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) throws IOException {
    StructType otherStructType = ((HoodieSparkRecord) other).getStructType();
    StructType writerStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    InternalRow mergeRow = HoodieInternalRowUtils.stitchRecords(data, getStructType(), (InternalRow) other.getData(), otherStructType, writerStructType);
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
    if (extractMetaField(getStructType()).length == 0) {
      resultRow = new HoodieInternalRow(metaFields, data, false);
    } else {
      resultRow = new HoodieInternalRow(metaFields, data, true);
    }

    return new HoodieSparkRecord(getKey(), resultRow, targetStructType, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, getStructType(), newStructType, renameCols);
    UnsafeProjection unsafeConvert = HoodieInternalRowUtils.getCachedUnsafeConvert(newStructType);
    InternalRow resultRow = unsafeConvert.apply(rewriteRow);
    UTF8String[] metaFields = extractMetaField(newStructType);
    if (metaFields.length > 0) {
      resultRow = new HoodieInternalRow(metaFields, data, true);
    }

    return new HoodieSparkRecord(getKey(), resultRow, newStructType, getOperation());
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    metadataValues.getKv().forEach((key, value) -> {
      int pos = getStructType().fieldIndex(key);
      if (value != null) {
        data.update(pos, CatalystTypeConverters.convertToCatalyst(value));
      }
    });

    return new HoodieSparkRecord(getKey(), data, getStructType(), getOperation());
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props,
      String keyName,
      String keyValue) {
    int pos = getStructType().fieldIndex(keyName);
    data.update(pos, CatalystTypeConverters.convertToCatalyst(keyValue));
    return this;
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
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      Schema schema, Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields) {
    if (populateMetaFields) {
      return convertToHoodieSparkRecord(getStructType(), data, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return convertToHoodieSparkRecord(getStructType(), data, simpleKeyGenFieldsOpt.get(), withOperation, Option.empty());
    } else {
      return convertToHoodieSparkRecord(getStructType(), data, withOperation, partitionNameOp);
    }
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Properties props, Option<BaseKeyGenerator> keyGen) {
    String key;
    String partition;
    if (keyGen.isPresent() && !Boolean.parseBoolean(props.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      SparkKeyGeneratorInterface keyGenerator = (SparkKeyGeneratorInterface) keyGen.get();
      key = keyGenerator.getRecordKey(data, getStructType()).toString();
      partition = keyGenerator.getPartitionPath(data, getStructType()).toString();
    } else {
      key = data.get(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal(), StringType).toString();
      partition = data.get(HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.ordinal(), StringType).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);
    return new HoodieSparkRecord(hoodieKey, data, getStructType(), getOperation());
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
    if (!HoodieCatalystExpressionUtils$.MODULE$.existField(getStructType(), orderingField)) {
      return 0;
    } else {
      NestedFieldPath nestedFieldPath = HoodieInternalRowUtils.getCachedPosList(getStructType(),
          orderingField);
      Comparable<?> value = (Comparable<?>) HoodieUnsafeRowUtils.getNestedInternalRowValue(
          data, nestedFieldPath);
      return value;
    }
  }

  public StructType getStructType() {
    return structType;
  }

  public void setStructType(StructType structType) {
    this.structType = structType;
  }

  private UTF8String[] extractMetaField(StructType structType) {
    return HOODIE_META_COLUMNS_WITH_OPERATION.stream()
        .filter(f -> HoodieCatalystExpressionUtils$.MODULE$.existField(structType, f))
        .map(UTF8String::fromString)
        .toArray(UTF8String[]::new);
  }

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, boolean withOperationField) {
    return convertToHoodieSparkRecord(structType, data,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, boolean withOperationField,
      Option<String> partitionName) {
    return convertToHoodieSparkRecord(structType, data,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, Pair<String, String> recordKeyPartitionPathFieldPair,
      boolean withOperationField, Option<String> partitionName) {
    final String recKey = getValue(structType, recordKeyPartitionPathFieldPair.getKey(), data).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        getValue(structType, recordKeyPartitionPathFieldPair.getRight(), data).toString());

    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(structType, data, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    return new HoodieSparkRecord(new HoodieKey(recKey, partitionPath), data, structType, operation);
  }
}
