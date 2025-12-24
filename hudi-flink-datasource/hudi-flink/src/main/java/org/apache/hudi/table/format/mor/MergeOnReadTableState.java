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

package org.apache.hudi.table.format.mor;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MultiplePartialUpdateUnit;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodiePayloadConfig;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Statistics for merge on read table source.
 */
public class MergeOnReadTableState implements Serializable {

  private static final long serialVersionUID = 1L;

  private final RowType rowType;
  private final RowType requiredRowType;
  private final String avroSchema;
  private final String requiredAvroSchema;
  private final List<MergeOnReadInputSplit> inputSplits;
  private final String[] pkFields;
  private final int operationPos;

  public MergeOnReadTableState(
      RowType rowType,
      RowType requiredRowType,
      String avroSchema,
      String requiredAvroSchema,
      List<MergeOnReadInputSplit> inputSplits,
      String[] pkFields) {
    this.rowType = rowType;
    this.requiredRowType = requiredRowType;
    this.avroSchema = avroSchema;
    this.requiredAvroSchema = requiredAvroSchema;
    this.inputSplits = inputSplits;
    this.pkFields = pkFields;
    this.operationPos = rowType.getFieldIndex(HoodieRecord.OPERATION_METADATA_FIELD);
  }

  public RowType getRowType() {
    return rowType;
  }

  public RowType getRequiredRowType() {
    return requiredRowType;
  }

  public String getAvroSchema() {
    return avroSchema;
  }

  public String getRequiredAvroSchema() {
    return requiredAvroSchema;
  }

  public List<MergeOnReadInputSplit> getInputSplits() {
    return inputSplits;
  }

  public int getOperationPos() {
    return operationPos;
  }

  public int[] getRequiredPositions() {
    final List<String> fieldNames = rowType.getFieldNames();
    return requiredRowType.getFieldNames().stream()
        .map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
  }

  // 这里可以乱序
  public HashSet<String> getLsmRequiredColumns(TypedProperties payloadProps, Schema tableSchema, Configuration conf) {
    HashSet<String> requiredWithMeta = new HashSet<>(requiredRowType.getFieldNames());

    requiredWithMeta.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    requiredWithMeta.add(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    requiredWithMeta.add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    // Avoid finding deleted data during query
    if (tableSchema.getFields().stream().anyMatch(field -> HoodieRecord.HOODIE_IS_DELETED_FIELD.equals(field.name()))) {
      requiredWithMeta.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }

    String orderingField = payloadProps.getString(HoodiePayloadConfig.ORDERING_FIELD.key(),
        HoodiePayloadConfig.ORDERING_FIELD.defaultValue());

    if (!StringUtils.isNullOrEmpty(orderingField) && orderingField.split(":").length > 1) {
      requiredWithMeta.addAll(new MultiplePartialUpdateUnit(orderingField).getAllOrderingFields());
    } else {
      if (tableSchema.getField(orderingField) != null) {
        requiredWithMeta.add(orderingField);
      }
    }

    return requiredWithMeta;
  }

  // copy from HoodieBaseRelation#projectSchema
  public Schema getLsmReadSchema(TypedProperties payloadProps, Schema tableSchema, Configuration conf) {
    HashSet<String> requiredColumns = getLsmRequiredColumns(payloadProps, tableSchema, conf);

    List<Schema.Field> requiredFields = tableSchema.getFields().stream().filter(f -> {
      return requiredColumns.contains(f.name());
    }).map(f -> {
      // We have to create a new [[Schema.Field]] since Avro schemas can't share field
      // instances (and will throw "org.apache.avro.AvroRuntimeException: Field already used")
      return new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
    }).collect(Collectors.toList());

    ValidationUtils.checkArgument(requiredColumns.size() == requiredFields.size(),
        "Miss match between required Col and table Col " + requiredFields + " VS " + requiredColumns);

    return Schema.createRecord(tableSchema.getName(), tableSchema.getDoc(), tableSchema.getNamespace(),
        tableSchema.isError(), requiredFields);
  }

  /**
   * Get the primary key positions in required row type.
   */
  public int[] getPkOffsetsInRequired() {
    final List<String> fieldNames = requiredRowType.getFieldNames();
    return Arrays.stream(pkFields)
        .map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
  }

  /**
   * Returns the primary key fields logical type with given offsets.
   *
   * @param pkOffsets the pk offsets in required row type
   * @return pk field logical types
   * @see #getPkOffsetsInRequired()
   */
  public LogicalType[] getPkTypes(int[] pkOffsets) {
    final LogicalType[] requiredTypes = requiredRowType.getFields().stream()
        .map(RowType.RowField::getType).toArray(LogicalType[]::new);
    return Arrays.stream(pkOffsets).mapToObj(offset -> requiredTypes[offset])
        .toArray(LogicalType[]::new);
  }
}
