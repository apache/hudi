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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
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
  public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";
  public static final String DEFAULT_COMPOSITE_KEY_FILED_VALUE = ":";

  public static final String RECORD_KEY_GEN_PARTITION_ID_CONFIG = "_hoodie.record.key.gen.partition.id";
  public static final String RECORD_KEY_GEN_INSTANT_TIME_CONFIG = "_hoodie.record.key.gen.instant.time";

  /**
   * Infers the key generator type based on the record key and partition fields.
   * <p>
   * (1) partition field is empty: {@link KeyGeneratorType#NON_PARTITION};
   * (2) Only one partition field and one record key field: {@link KeyGeneratorType#SIMPLE};
   * (3) More than one partition and/or record key fields: {@link KeyGeneratorType#COMPLEX}.
   *
   * @param recordsKeyFields Record key field list.
   * @param partitionFields  Partition field list.
   * @return Inferred key generator type.
   */
  public static KeyGeneratorType inferKeyGeneratorType(
      Option<String> recordsKeyFields, String partitionFields) {
    boolean autoGenerateRecordKeys = !recordsKeyFields.isPresent();
    if (autoGenerateRecordKeys) {
      return inferKeyGeneratorTypeForAutoKeyGen(partitionFields);
    } else {
      if (!StringUtils.isNullOrEmpty(partitionFields)) {
        int numPartFields = partitionFields.split(",").length;
        int numRecordKeyFields = recordsKeyFields.get().split(",").length;
        if (numPartFields == 1 && numRecordKeyFields == 1) {
          return KeyGeneratorType.SIMPLE;
        }
        return KeyGeneratorType.COMPLEX;
      }
      return KeyGeneratorType.NON_PARTITION;
    }
  }

  // When auto record key gen is enabled, our inference will be based on partition path only.
  private static KeyGeneratorType inferKeyGeneratorTypeForAutoKeyGen(String partitionFields) {
    if (!StringUtils.isNullOrEmpty(partitionFields)) {
      int numPartFields = partitionFields.split(",").length;
      if (numPartFields == 1) {
        return KeyGeneratorType.SIMPLE;
      }
      return KeyGeneratorType.COMPLEX;
    }
    return KeyGeneratorType.NON_PARTITION;
  }

  /**
   * Fetches record key from the GenericRecord.
   *
   * @param genericRecord   generic record of interest.
   * @param keyGeneratorOpt Optional BaseKeyGenerator. If not, meta field will be used.
   * @return the record key for the passed in generic record.
   */
  public static String getRecordKeyFromGenericRecord(GenericRecord genericRecord, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey(genericRecord) : genericRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  /**
   * Fetches partition path from the GenericRecord.
   *
   * @param genericRecord   generic record of interest.
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
    return extractRecordKeysByFields(recordKey, Collections.emptyList());
  }

  public static String[] extractRecordKeysByFields(String recordKey, List<String> fields) {
    String[] fieldKV = recordKey.split(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
    return Arrays.stream(fieldKV).map(kv -> kv.split(DEFAULT_COMPOSITE_KEY_FILED_VALUE, 2))
        .filter(kvArray -> kvArray.length == 1 || fields.isEmpty() || (fields.contains(kvArray[0])))
        .map(kvArray -> {
          if (kvArray.length == 1) {
            return kvArray[0];
          } else if (kvArray[1].equals(NULL_RECORDKEY_PLACEHOLDER)) {
            return null;
          } else if (kvArray[1].equals(EMPTY_RECORDKEY_PLACEHOLDER)) {
            return "";
          } else {
            return kvArray[1];
          }
        }).toArray(String[]::new);
  }

  public static String getRecordKey(GenericRecord record, List<String> recordKeyFields, boolean consistentLogicalTimestampEnabled) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (int i = 0; i < recordKeyFields.size(); i++) {
      String recordKeyField = recordKeyFields.get(i);
      String recordKeyValue = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true, consistentLogicalTimestampEnabled);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField).append(DEFAULT_COMPOSITE_KEY_FILED_VALUE).append(NULL_RECORDKEY_PLACEHOLDER);
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField).append(DEFAULT_COMPOSITE_KEY_FILED_VALUE).append(EMPTY_RECORDKEY_PLACEHOLDER);
      } else {
        recordKey.append(recordKeyField).append(DEFAULT_COMPOSITE_KEY_FILED_VALUE).append(recordKeyValue);
        keyIsNullEmpty = false;
      }
      if (i != recordKeyFields.size() - 1) {
        recordKey.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + recordKeyFields + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }

  public static String getRecordPartitionPath(GenericRecord record, List<String> partitionPathFields,
                                              boolean hiveStylePartitioning, boolean encodePartitionPath, boolean consistentLogicalTimestampEnabled) {
    if (partitionPathFields.isEmpty()) {
      return "";
    }

    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partitionPathFields.size(); i++) {
      String partitionPathField = partitionPathFields.get(i);
      String fieldVal = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true, consistentLogicalTimestampEnabled);
      if (fieldVal == null || fieldVal.isEmpty()) {
        if (hiveStylePartitioning) {
          partitionPath.append(partitionPathField).append("=");
        }
        partitionPath.append(HUDI_DEFAULT_PARTITION_PATH);
      } else {
        if (encodePartitionPath) {
          fieldVal = PartitionPathEncodeUtils.escapePathName(fieldVal);
        }
        if (hiveStylePartitioning) {
          partitionPath.append(partitionPathField).append("=");
        }
        partitionPath.append(fieldVal);
      }
      if (i != partitionPathFields.size() - 1) {
        partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }
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
  public static BaseHoodieDateTimeParser createDateTimeParser(TypedProperties props, String parserClass) throws IOException {
    try {
      return (BaseHoodieDateTimeParser) ReflectionUtils.loadClass(parserClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load date time parser class " + parserClass, e);
    }
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

  public static List<String> getRecordKeyFields(TypedProperties props) {
    return Option.ofNullable(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null))
        .map(recordKeyConfigValue ->
            Arrays.stream(recordKeyConfigValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList())
        ).orElse(Collections.emptyList());
  }

  /**
   * @param props props of interest.
   * @return true if record keys need to be auto generated. false otherwise.
   */
  public static boolean isAutoGeneratedRecordKeysEnabled(TypedProperties props) {
    return !props.containsKey(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())
        || props.getProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).equals(StringUtils.EMPTY_STRING);
    // spark-sql sets record key config to empty string for update, and couple of other statements.
  }
}
