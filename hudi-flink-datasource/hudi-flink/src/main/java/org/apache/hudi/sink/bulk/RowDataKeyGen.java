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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.escapePathName;

/**
 * Key generator for {@link RowData}.
 */
public class RowDataKeyGen implements Serializable {
  private static final long serialVersionUID = 1L;

  // reference: NonpartitionedAvroKeyGenerator
  private static final String EMPTY_PARTITION = "";

  // reference: org.apache.hudi.keygen.KeyGenUtils
  private static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  private static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  private final String[] recordKeyFields;
  private final String[] partitionPathFields;

  private final RowDataProjection recordKeyProjection;
  private final RowDataProjection partitionPathProjection;

  private final boolean hiveStylePartitioning;
  private final boolean encodePartitionPath;

  private final Option<TimestampBasedAvroKeyGenerator> keyGenOpt;

  // efficient code path
  private boolean simpleRecordKey = false;
  private RowData.FieldGetter recordKeyFieldGetter;

  private boolean simplePartitionPath = false;
  private RowData.FieldGetter partitionPathFieldGetter;

  private boolean nonPartitioned;

  private RowDataKeyGen(
      String recordKeys,
      String partitionFields,
      RowType rowType,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath,
      Option<TimestampBasedAvroKeyGenerator> keyGenOpt) {
    this.recordKeyFields = recordKeys.split(",");
    this.partitionPathFields = partitionFields.split(",");
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();

    this.hiveStylePartitioning = hiveStylePartitioning;
    this.encodePartitionPath = encodePartitionPath;
    if (this.recordKeyFields.length == 1) {
      // efficient code path
      this.simpleRecordKey = true;
      int recordKeyIdx = fieldNames.indexOf(this.recordKeyFields[0]);
      this.recordKeyFieldGetter = RowData.createFieldGetter(fieldTypes.get(recordKeyIdx), recordKeyIdx);
      this.recordKeyProjection = null;
    } else {
      this.recordKeyProjection = getProjection(this.recordKeyFields, fieldNames, fieldTypes);
    }
    if (this.partitionPathFields.length == 1) {
      // efficient code path
      if (this.partitionPathFields[0].equals("")) {
        this.nonPartitioned = true;
      } else {
        this.simplePartitionPath = true;
        int partitionPathIdx = fieldNames.indexOf(this.partitionPathFields[0]);
        this.partitionPathFieldGetter = RowData.createFieldGetter(fieldTypes.get(partitionPathIdx), partitionPathIdx);
      }
      this.partitionPathProjection = null;
    } else {
      this.partitionPathProjection = getProjection(this.partitionPathFields, fieldNames, fieldTypes);
    }
    this.keyGenOpt = keyGenOpt;
  }

  public static RowDataKeyGen instance(Configuration conf, RowType rowType) {
    Option<TimestampBasedAvroKeyGenerator> keyGeneratorOpt = Option.empty();
    if (TimestampBasedAvroKeyGenerator.class.getName().equals(conf.getString(FlinkOptions.KEYGEN_CLASS_NAME))) {
      try {
        keyGeneratorOpt = Option.of(new TimestampBasedAvroKeyGenerator(StreamerUtil.flinkConf2TypedProperties(conf)));
      } catch (IOException e) {
        throw new HoodieKeyException("Initialize TimestampBasedAvroKeyGenerator error", e);
      }
    }
    return new RowDataKeyGen(conf.getString(FlinkOptions.RECORD_KEY_FIELD), conf.getString(FlinkOptions.PARTITION_PATH_FIELD),
        rowType, conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING), conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING),
        keyGeneratorOpt);
  }

  public HoodieKey getHoodieKey(RowData rowData) {
    return new HoodieKey(getRecordKey(rowData), getPartitionPath(rowData));
  }

  public String getRecordKey(RowData rowData) {
    if (this.simpleRecordKey) {
      return getRecordKey(recordKeyFieldGetter.getFieldOrNull(rowData), this.recordKeyFields[0]);
    } else {
      Object[] keyValues = this.recordKeyProjection.projectAsValues(rowData);
      return getRecordKey(keyValues, this.recordKeyFields);
    }
  }

  public String getPartitionPath(RowData rowData) {
    if (this.simplePartitionPath) {
      return getPartitionPath(partitionPathFieldGetter.getFieldOrNull(rowData),
          this.partitionPathFields[0], this.hiveStylePartitioning, this.encodePartitionPath, this.keyGenOpt);
    } else if (this.nonPartitioned) {
      return EMPTY_PARTITION;
    } else {
      Object[] partValues = this.partitionPathProjection.projectAsValues(rowData);
      return getRecordPartitionPath(partValues, this.partitionPathFields, this.hiveStylePartitioning, this.encodePartitionPath);
    }
  }

  // reference: org.apache.hudi.keygen.KeyGenUtils.getRecordPartitionPath
  private static String getRecordKey(Object[] keyValues, String[] keyFields) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (int i = 0; i < keyValues.length; i++) {
      String recordKeyField = keyFields[i];
      String recordKeyValue = StringUtils.objToString(keyValues[i]);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField).append(":").append(NULL_RECORDKEY_PLACEHOLDER).append(",");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField).append(":").append(EMPTY_RECORDKEY_PLACEHOLDER).append(",");
      } else {
        recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + Arrays.toString(keyFields) + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }

  // reference: org.apache.hudi.keygen.KeyGenUtils.getRecordPartitionPath
  private static String getRecordPartitionPath(
      Object[] partValues,
      String[] partFields,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath) {
    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partFields.length; i++) {
      String partField = partFields[i];
      String partValue = StringUtils.objToString(partValues[i]);
      if (partValue == null || partValue.isEmpty()) {
        partitionPath.append(hiveStylePartitioning ? partField + "=" + DEFAULT_PARTITION_PATH
            : DEFAULT_PARTITION_PATH);
      } else {
        if (encodePartitionPath) {
          partValue = escapePathName(partValue);
        }
        partitionPath.append(hiveStylePartitioning ? partField + "=" + partValue : partValue);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  // reference: org.apache.hudi.keygen.KeyGenUtils.getRecordKey
  public static String getRecordKey(Object recordKeyValue, String recordKeyField) {
    String recordKey = StringUtils.objToString(recordKeyValue);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
    }
    return recordKey;
  }

  // reference: org.apache.hudi.keygen.KeyGenUtils.getPartitionPath
  public static String getPartitionPath(
      Object partValue,
      String partField,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath,
      Option<TimestampBasedAvroKeyGenerator> keyGenOpt) {
    if (keyGenOpt.isPresent()) {
      TimestampBasedAvroKeyGenerator keyGenerator = keyGenOpt.get();
      return keyGenerator.getPartitionPath(toEpochMilli(partValue, keyGenerator));
    }
    String partitionPath = StringUtils.objToString(partValue);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    if (encodePartitionPath) {
      partitionPath = escapePathName(partitionPath);
    }
    if (hiveStylePartitioning) {
      partitionPath = partField + "=" + partitionPath;
    }
    return partitionPath;
  }

  private static Object toEpochMilli(Object val, TimestampBasedAvroKeyGenerator keyGenerator) {
    if (val instanceof TimestampData) {
      return ((TimestampData) val).toInstant().toEpochMilli();
    }
    if (val == null) {
      // should match the default partition path when STRING partition path re-format is supported
      return keyGenerator.getDefaultPartitionVal();
    }
    return val;
  }

  /**
   * Returns the row data projection for the given field names and table schema.
   *
   * @param fields       The projected field names
   * @param schemaFields The table schema names
   * @param schemaTypes  The table schema types
   * @return the row data projection for the fields
   */
  private static RowDataProjection getProjection(String[] fields, List<String> schemaFields, List<LogicalType> schemaTypes) {
    int[] positions = getFieldPositions(fields, schemaFields);
    LogicalType[] types = Arrays.stream(positions).mapToObj(schemaTypes::get).toArray(LogicalType[]::new);
    return RowDataProjection.instance(types, positions);
  }

  /**
   * Returns the field positions of the given fields {@code fields} among all the fields {@code allFields}.
   */
  private static int[] getFieldPositions(String[] fields, List<String> allFields) {
    return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
  }
}
