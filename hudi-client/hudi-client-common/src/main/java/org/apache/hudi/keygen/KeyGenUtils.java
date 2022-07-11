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

package org.apache.hudi.keygen;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.parser.BaseHoodieDateTimeParser;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KeyGenUtils {

  protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  protected static final String HUDI_DEFAULT_PARTITION_PATH = PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  /**
   * Fetches record key from the GenericRecord.
   * @param genericRecord generic record of interest.
   * @param keyGeneratorOpt Optional BaseKeyGenerator. If not, meta field will be used.
   * @return the record key for the passed in generic record.
   */
  public static String getRecordKeyFromGenericRecord(GenericRecord genericRecord, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey(genericRecord) : genericRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  /**
   * Fetches partition path from the GenericRecord.
   * @param genericRecord generic record of interest.
   * @param keyGeneratorOpt Optional BaseKeyGenerator. If not, meta field will be used.
   * @return the partition path for the passed in generic record.
   */
  public static String getPartitionPathFromGenericRecord(GenericRecord genericRecord, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getPartitionPath(genericRecord) : genericRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
  }

  /**
   * Extracts the record key fields in strings out of the given record key,
   * this is the reverse operation of {@link #getRecordKey(GenericRecord, String, boolean)}.
   *
   * @see SimpleAvroKeyGenerator
   * @see org.apache.hudi.keygen.ComplexAvroKeyGenerator
   */
  public static String[] extractRecordKeys(String recordKey) {
    String[] fieldKV = recordKey.split(",");
    if (fieldKV.length == 1) {
      return fieldKV;
    } else {
      // a complex key
      return Arrays.stream(fieldKV).map(kv -> {
        final String[] kvArray = kv.split(":");
        if (kvArray[1].equals(NULL_RECORDKEY_PLACEHOLDER)) {
          return null;
        } else if (kvArray[1].equals(EMPTY_RECORDKEY_PLACEHOLDER)) {
          return "";
        } else {
          return kvArray[1];
        }
      }).toArray(String[]::new);
    }
  }

  public static String getRecordKey(GenericRecord record, List<String> recordKeyFields, boolean consistentLogicalTimestampEnabled) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      String recordKeyValue = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true, consistentLogicalTimestampEnabled);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
      } else {
        recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + recordKeyFields.toString() + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }

  public static String getRecordPartitionPath(GenericRecord record, List<String> partitionPathFields,
      boolean hiveStylePartitioning, boolean encodePartitionPath, boolean consistentLogicalTimestampEnabled) {
    if (partitionPathFields.isEmpty()) {
      return "";
    }

    StringBuilder partitionPath = new StringBuilder();
    for (String partitionPathField : partitionPathFields) {
      String fieldVal = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true, consistentLogicalTimestampEnabled);
      if (fieldVal == null || fieldVal.isEmpty()) {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + HUDI_DEFAULT_PARTITION_PATH
            : HUDI_DEFAULT_PARTITION_PATH);
      } else {
        if (encodePartitionPath) {
          fieldVal = PartitionPathEncodeUtils.escapePathName(fieldVal);
        }
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + fieldVal : fieldVal);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  public static String getRecordKey(GenericRecord record, String recordKeyField, boolean consistentLogicalTimestampEnabled) {
    String recordKey = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true, consistentLogicalTimestampEnabled);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
    }
    return recordKey;
  }

  public static String getPartitionPath(GenericRecord record, String partitionPathField,
      boolean hiveStylePartitioning, boolean encodePartitionPath, boolean consistentLogicalTimestampEnabled) {
    String partitionPath = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true, consistentLogicalTimestampEnabled);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = HUDI_DEFAULT_PARTITION_PATH;
    }
    if (encodePartitionPath) {
      partitionPath = PartitionPathEncodeUtils.escapePathName(partitionPath);
    }
    if (hiveStylePartitioning) {
      partitionPath = partitionPathField + "=" + partitionPath;
    }
    return partitionPath;
  }

  /**
   * Create a date time parser class for TimestampBasedKeyGenerator, passing in any configs needed.
   */
  public static BaseHoodieDateTimeParser createDateTimeParser(TypedProperties props, String parserClass) throws IOException  {
    try {
      return (BaseHoodieDateTimeParser) ReflectionUtils.loadClass(parserClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load date time parser class " + parserClass, e);
    }
  }

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   * <p>
   * This method is for user-defined classes. To create hudi's built-in key generators, please set proper
   * {@link org.apache.hudi.keygen.constant.KeyGeneratorType} conf, and use the relevant factory, see
   * {@link org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory}.
   */
  public static KeyGenerator createKeyGeneratorByClassName(TypedProperties props) throws IOException {
    KeyGenerator keyGenerator = null;
    String keyGeneratorClass = props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), null);
    if (!StringUtils.isNullOrEmpty(keyGeneratorClass)) {
      try {
        keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
      } catch (Throwable e) {
        throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
      }
    }
    return keyGenerator;
  }

  static List<String> getRecordKeyFields(TypedProperties props) {
    if (props.containsKey(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())) {
      return Collections.emptyList();
    }

    return Arrays.stream(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
