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

import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.escapePathName;
import static org.apache.hudi.keygen.KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR;

/**
 * Key generator for {@link RowData}.
 */
public class RowDataKeyGen implements Serializable {
  private static final long serialVersionUID = 1L;

  // reference: NonpartitionedAvroKeyGenerator
  private static final String EMPTY_PARTITION = "";

  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  private static final String DEFAULT_FIELD_SEPARATOR = ",";

  private final String[] recordKeyFields;
  private final String[] partitionPathFields;

  private final RowDataProjection recordKeyProjection;
  private final RowDataProjection partitionPathProjection;

  private final boolean hiveStylePartitioning;
  private final boolean encodePartitionPath;
  private final boolean consistentLogicalTimestampEnabled;

  private final Option<TimestampBasedAvroKeyGenerator> keyGenOpt;

  // efficient code path
  private SerializableFunctionUnchecked<RowData, String> simpleRecordKeyFunc;
  private RowData.FieldGetter recordKeyFieldGetter;

  private boolean simplePartitionPath = false;
  private RowData.FieldGetter partitionPathFieldGetter;

  private boolean nonPartitioned;

  protected RowDataKeyGen(
      Option<String> recordKeys,
      String partitionFields,
      RowType rowType,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath,
      boolean consistentLogicalTimestampEnabled,
      Option<TimestampBasedAvroKeyGenerator> keyGenOpt,
      boolean useComplexKeygenNewEncoding) {
    this.partitionPathFields = partitionFields.split(DEFAULT_FIELD_SEPARATOR);
    this.hiveStylePartitioning = hiveStylePartitioning;
    this.encodePartitionPath = encodePartitionPath;
    this.consistentLogicalTimestampEnabled = consistentLogicalTimestampEnabled;

    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();

    boolean simpleRecordKey = false;
    boolean multiplePartitions = false;
    if (!recordKeys.isPresent()) {
      this.recordKeyFields = null;
      this.recordKeyProjection = null;
    } else {
      this.recordKeyFields = recordKeys.get().split(DEFAULT_FIELD_SEPARATOR);
      if (this.recordKeyFields.length == 1) {
        // efficient code path
        simpleRecordKey = true;
        int recordKeyIdx = fieldNames.indexOf(this.recordKeyFields[0]);
        this.recordKeyFieldGetter = RowData.createFieldGetter(fieldTypes.get(recordKeyIdx), recordKeyIdx);
        this.recordKeyProjection = null;
      } else {
        this.recordKeyProjection = getProjection(this.recordKeyFields, fieldNames, fieldTypes);
      }
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
      multiplePartitions = true;
    }
    if (simpleRecordKey) {
      if (multiplePartitions && !useComplexKeygenNewEncoding) {
        // single record key with multiple partition fields
        this.simpleRecordKeyFunc = rowData -> {
          String oriKey = getRecordKey(recordKeyFieldGetter.getFieldOrNull(rowData), this.recordKeyFields[0], consistentLogicalTimestampEnabled);
          return new StringBuilder(this.recordKeyFields[0]).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(oriKey).toString();
        };
      } else {
        this.simpleRecordKeyFunc = rowData -> getRecordKey(recordKeyFieldGetter.getFieldOrNull(rowData), this.recordKeyFields[0], consistentLogicalTimestampEnabled);
      }
    }
    this.keyGenOpt = keyGenOpt;
  }

  public static RowDataKeyGen instance(Configuration conf, RowType rowType) {
    Option<TimestampBasedAvroKeyGenerator> keyGeneratorOpt = Option.empty();
    if (TimestampBasedAvroKeyGenerator.class.getName().equals(conf.get(FlinkOptions.KEYGEN_CLASS_NAME))) {
      try {
        keyGeneratorOpt = Option.of(new TimestampBasedAvroKeyGenerator(StreamerUtil.flinkConf2TypedProperties(conf)));
      } catch (IOException e) {
        throw new HoodieKeyException("Initialize TimestampBasedAvroKeyGenerator error", e);
      }
    }
    boolean consistentLogicalTimestampEnabled = OptionsResolver.isConsistentLogicalTimestampEnabled(conf);
    return new RowDataKeyGen(Option.of(conf.get(FlinkOptions.RECORD_KEY_FIELD)), conf.get(FlinkOptions.PARTITION_PATH_FIELD),
        rowType, conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING), conf.get(FlinkOptions.URL_ENCODE_PARTITIONING),
        consistentLogicalTimestampEnabled, keyGeneratorOpt, OptionsResolver.useComplexKeygenNewEncoding(conf));
  }

  public HoodieKey getHoodieKey(RowData rowData) {
    return new HoodieKey(getRecordKey(rowData), getPartitionPath(rowData));
  }

  public String getRecordKey(RowData rowData) {
    if (this.simpleRecordKeyFunc != null) {
      return this.simpleRecordKeyFunc.apply(rowData);
    } else {
      Object[] keyValues = this.recordKeyProjection.projectAsValues(rowData);
      return getRecordKey(keyValues, this.recordKeyFields, consistentLogicalTimestampEnabled);
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

  private static String getRecordKey(Object[] keyValues, String[] keyFields, boolean consistentLogicalTimestampEnabled) {
    return KeyGenerator.constructRecordKey(keyFields,
        (key, index) -> StringUtils.objToString(getTimestampValue(consistentLogicalTimestampEnabled, keyValues[index])));
  }

  private static Object getTimestampValue(boolean consistentLogicalTimestampEnabled, Object value) {
    if (!consistentLogicalTimestampEnabled && (value instanceof TimestampData)) {
      TimestampData timestampData = (TimestampData) value;
      value = timestampData.toTimestamp().toInstant().toEpochMilli();
    }
    return value;
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
        partitionPath.append(hiveStylePartitioning ? partField + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH);
      } else {
        if (encodePartitionPath) {
          partValue = escapePathName(partValue);
        }
        partitionPath.append(hiveStylePartitioning ? partField + "=" + partValue : partValue);
      }
      if (i != partFields.length - 1) {
        partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }
    return partitionPath.toString();
  }

  // reference: org.apache.hudi.keygen.KeyGenUtils.getRecordKey
  public static String getRecordKey(Object recordKeyValue, String recordKeyField,boolean consistentLogicalTimestampEnabled) {
    recordKeyValue = getTimestampValue(consistentLogicalTimestampEnabled, recordKeyValue);
    String recordKey = StringUtils.objToString(recordKeyValue);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException(String.format("recordKey value: \"%s\" for field: \"%s\" cannot be null or empty.",
          recordKey, recordKeyField));
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
    if (val instanceof StringData) {
      // case of `TimestampType.DATE_STRING`, need to convert to string for consequent processing in `hudi-client-common` module
      return val.toString();
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
