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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.util.HoodieSparkRecordUtils;
import org.apache.spark.sql.HoodieCatalystExpressionUtils$;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils.NestedFieldPath;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
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
 * Spark Engine-specific Implementations of `HoodieRecord`
 *
 * NOTE: [[HoodieSparkRecord]] is expected to hold either [[UnsafeRow]] or [[HoodieInternalRow]]:
 *
 * <ul>
 *    <li>[[UnsafeRow]] is held to make sure a) we don't deserialize raw bytes payload
 *       into JVM types unnecessarily, b) we don't incur penalty of ser/de during shuffling,
 *       c) we don't add strain on GC</li>
 *    <li>[[HoodieInternalRow]] is held in cases when underlying [[UnsafeRow]]'s metadata fields
 *       need to be updated (ie serving as an overlay layer on top of [[UnsafeRow]])</li>
 * </ul>
 *
 */
public class HoodieSparkRecord extends HoodieRecord<InternalRow> implements KryoSerializable {

  /**
   * Record copy operation to avoid double copying. InternalRow do not need to copy twice.
   */
  private boolean copy;

  /**
   * NOTE: {@code HoodieSparkRecord} is holding the schema only in cases when it would have
   *       to execute {@link UnsafeProjection} so that the {@link InternalRow} it's holding to
   *       could be projected into {@link UnsafeRow} and be efficiently serialized subsequently
   *       (by Kryo)
   */
  private final transient StructType schema;

  public HoodieSparkRecord(UnsafeRow data) {
    this(data, null);
  }

  public HoodieSparkRecord(InternalRow data, StructType schema) {
    super(null, data);

    validateRow(data, schema);
    this.copy = false;
    this.schema = schema;
  }

  public HoodieSparkRecord(HoodieKey key, UnsafeRow data, boolean copy) {
    this(key, data, null, copy);
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema, boolean copy) {
    super(key, data);

    validateRow(data, schema);
    this.copy = copy;
    this.schema = schema;
  }

  private HoodieSparkRecord(HoodieKey key, InternalRow data, StructType schema, HoodieOperation operation, boolean copy) {
    super(key, data, operation);

    validateRow(data, schema);
    this.copy = copy;
    this.schema = schema;
  }

  @Override
  public HoodieSparkRecord newInstance() {
    return new HoodieSparkRecord(this.key, this.data, this.schema, this.operation, this.copy);
  }

  @Override
  public HoodieSparkRecord newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieSparkRecord(key, this.data, this.schema, op, this.copy);
  }

  @Override
  public HoodieSparkRecord newInstance(HoodieKey key) {
    return new HoodieSparkRecord(key, this.data, this.schema, this.operation, this.copy);
  }

  @Override
  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (key != null) {
      return getRecordKey();
    }
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    return keyGeneratorOpt.isPresent() ? ((SparkKeyGeneratorInterface) keyGeneratorOpt.get())
        .getRecordKey(data, structType).toString() : data.getString(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal());
  }

  @Override
  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    if (key != null) {
      return getRecordKey();
    }
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    DataType dataType = structType.apply(keyFieldName).dataType();
    int pos = structType.fieldIndex(keyFieldName);
    return data.get(pos, dataType).toString();
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  @Override
  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    return HoodieSparkRecordUtils.getRecordColumnValues(data, columns, structType, consistentLogicalTimestampEnabled);
  }

  @Override
  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    StructType targetStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    InternalRow mergeRow = new JoinedRow(data, (InternalRow) other.getData());
    UnsafeProjection projection =
        HoodieInternalRowUtils.getCachedUnsafeProjection(targetStructType, targetStructType);
    return new HoodieSparkRecord(getKey(), projection.apply(mergeRow), targetStructType, getOperation(), copy);
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema) throws IOException {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType targetStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);

    boolean containMetaFields = hasMetaFields(structType);
    UTF8String[] metaFields = tryExtractMetaFields(data, structType);

    // TODO add actual rewriting
    InternalRow finalRow = new HoodieInternalRow(metaFields, data, containMetaFields);

    return new HoodieSparkRecord(getKey(), finalRow, targetStructType, getOperation(), copy);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) throws IOException {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);

    boolean containMetaFields = hasMetaFields(structType);
    UTF8String[] metaFields = tryExtractMetaFields(data, structType);

    InternalRow rewrittenRow =
        HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, structType, newStructType, renameCols);
    HoodieInternalRow finalRow = new HoodieInternalRow(metaFields, rewrittenRow, containMetaFields);

    return new HoodieSparkRecord(getKey(), finalRow, newStructType, getOperation(), copy);
  }

  @Override
  public HoodieRecord updateMetadataValues(Schema recordSchema, Properties props, MetadataValues metadataValues) throws IOException {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    HoodieInternalRow updatableRow = wrapIntoUpdatableOverlay(data, structType);

    metadataValues.getKv().forEach((key, value) -> {
      int pos = structType.fieldIndex(key);
      if (value != null) {
        updatableRow.update(pos, CatalystTypeConverters.convertToCatalyst(value));
      }
    });

    return new HoodieSparkRecord(getKey(), updatableRow, structType, getOperation(), copy);
  }

  @Override
  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    int pos = structType.fieldIndex(keyFieldName);
    data.update(pos, CatalystTypeConverters.convertToCatalyst(StringUtils.EMPTY_STRING));
    return this;
  }

  @Override
  public boolean isDelete(Schema recordSchema, Properties props) throws IOException {
    if (null == data) {
      return true;
    }
    if (recordSchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) == null) {
      return false;
    }
    Object deleteMarker = data.get(recordSchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos(), BooleanType);
    return deleteMarker instanceof Boolean && (boolean) deleteMarker;
  }

  @Override
  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    if (data != null && data.equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      Schema recordSchema, Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    if (populateMetaFields) {
      return convertToHoodieSparkRecord(structType, this, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return convertToHoodieSparkRecord(structType, this, simpleKeyGenFieldsOpt.get(), withOperation, Option.empty());
    } else {
      return convertToHoodieSparkRecord(structType, this, withOperation, partitionNameOp);
    }
  }

  @Override
  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema, Properties props, Option<BaseKeyGenerator> keyGen) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    String key;
    String partition;
    boolean populateMetaFields = Boolean.parseBoolean(props.getOrDefault(POPULATE_META_FIELDS.key(),
        POPULATE_META_FIELDS.defaultValue().toString()).toString());
    if (!populateMetaFields && keyGen.isPresent()) {
      SparkKeyGeneratorInterface keyGenerator = (SparkKeyGeneratorInterface) keyGen.get();
      key = keyGenerator.getRecordKey(data, structType).toString();
      partition = keyGenerator.getPartitionPath(data, structType).toString();
    } else {
      key = data.get(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.ordinal(), StringType).toString();
      partition = data.get(HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.ordinal(), StringType).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);
    return new HoodieSparkRecord(hoodieKey, data, structType, getOperation(), copy);
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties prop) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodieSparkRecord copy() {
    if (!copy) {
      this.data = this.data.copy();
      this.copy = true;
    }
    return this;
  }

  @Override
  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    String orderingField = ConfigUtils.getOrderingField(props);
    if (!HoodieCatalystExpressionUtils$.MODULE$.existField(structType, orderingField)) {
      return 0;
    } else {
      NestedFieldPath nestedFieldPath = HoodieInternalRowUtils.getCachedPosList(structType, orderingField);
      Comparable<?> value = (Comparable<?>) HoodieUnsafeRowUtils.getNestedInternalRowValue(data, nestedFieldPath);
      return value;
    }
  }

  @Override
  public void write(Kryo kryo, Output output) {
    // NOTE: We only serialize data held by the [[HoodieRecord]] base class
    super.write(kryo, output);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    super.read(kryo, input);
    // NOTE: After deserialization every object is allocated on the heap, therefore
    //       we annotate this object as being copied
    this.copy = true;
  }

  private static HoodieInternalRow wrapIntoUpdatableOverlay(InternalRow data, StructType structType) {
    if (data instanceof HoodieInternalRow) {
      return (HoodieInternalRow) data;
    }

    boolean containsMetaFields = hasMetaFields(structType);
    UTF8String[] metaFields = tryExtractMetaFields(data, structType);
    return new HoodieInternalRow(metaFields, data, containsMetaFields);
  }

  private static UTF8String[] tryExtractMetaFields(InternalRow row, StructType structType) {
    boolean containsMetaFields = hasMetaFields(structType);
    if (containsMetaFields) {
      return HoodieRecord.HOODIE_META_COLUMNS.stream()
          .map(col -> row.getUTF8String(HOODIE_META_COLUMNS_NAME_TO_POS.get(col)))
          .toArray(UTF8String[]::new);
    } else {
      return new UTF8String[HoodieRecord.HOODIE_META_COLUMNS.size()];
    }
  }

  private static boolean hasMetaFields(StructType structType) {
    return structType.getFieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD).isDefined();
  }

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, HoodieSparkRecord record, boolean withOperationField) {
    return convertToHoodieSparkRecord(structType, record,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, HoodieSparkRecord record, boolean withOperationField,
      Option<String> partitionName) {
    return convertToHoodieSparkRecord(structType, record,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  private static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, HoodieSparkRecord record, Pair<String, String> recordKeyPartitionPathFieldPair,
      boolean withOperationField, Option<String> partitionName) {
    final String recKey = getValue(structType, recordKeyPartitionPathFieldPair.getKey(), record.data).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        getValue(structType, recordKeyPartitionPathFieldPair.getRight(), record.data).toString());

    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(structType, record.data, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    return new HoodieSparkRecord(new HoodieKey(recKey, partitionPath), record.data, structType, operation, record.copy);
  }

  private static void validateRow(InternalRow data, StructType schema) {
    // NOTE: [[HoodieSparkRecord]] is expected to hold either
    //          - Instance of [[UnsafeRow]] or
    //          - Instance of [[HoodieInternalRow]] or
    //          - Instance of [[ColumnarBatchRow]]
    //
    //       In case provided row is anything but [[UnsafeRow]], it's expected that the
    //       corresponding schema has to be provided as well so that it could be properly
    //       serialized (in case it would need to be)
    boolean isValid = data instanceof UnsafeRow ||
        schema != null && (data instanceof HoodieInternalRow || SparkAdapterSupport$.MODULE$.sparkAdapter().isColumnarBatchRow(data));

    ValidationUtils.checkState(isValid);
  }
}
