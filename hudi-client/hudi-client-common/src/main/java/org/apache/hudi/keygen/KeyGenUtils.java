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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.parser.BaseHoodieDateTimeParser;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.keygen.KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR;
import static org.apache.hudi.keygen.KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;
import static org.apache.hudi.keygen.KeyGenerator.EMPTY_RECORDKEY_PLACEHOLDER;
import static org.apache.hudi.keygen.KeyGenerator.NULL_RECORDKEY_PLACEHOLDER;
import static org.apache.hudi.keygen.KeyGenerator.constructRecordKey;

public class KeyGenUtils {
  protected static final String HUDI_DEFAULT_PARTITION_PATH = PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
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
    int numRecordKeyFields = recordsKeyFields.map(fields -> fields.split(",").length).orElse(0);
    KeyGeneratorType partitionKeyGeneratorType = inferKeyGeneratorTypeFromPartitionFields(partitionFields);
    if (numRecordKeyFields <= 1) {
      return partitionKeyGeneratorType;
    } else {
      // More than one record key fields are configured
      if (partitionKeyGeneratorType == KeyGeneratorType.SIMPLE) {
        // if there is a single partition field configured but multiple record key fields, key generator type
        // should be COMPLEX and not SIMPLE
        return KeyGeneratorType.COMPLEX;
      } else {
        // partition generator type is COMPLEX, CUSTOM or NON_PARTITION. In all these cases, partition
        // generator type determines the key generator type
        return partitionKeyGeneratorType;
      }
    }
  }

  // When auto record key gen is enabled, our inference will be based on partition path only.
  static KeyGeneratorType inferKeyGeneratorTypeFromPartitionFields(String partitionFields) {
    if (!StringUtils.isNullOrEmpty(partitionFields)) {
      String[] partitonFields = partitionFields.split(",");
      if (partitonFields[0].contains(BaseKeyGenerator.CUSTOM_KEY_GENERATOR_SPLIT_REGEX)) {
        return KeyGeneratorType.CUSTOM;
      } else if (partitonFields.length == 1) {
        return KeyGeneratorType.SIMPLE;
      } else {
        return KeyGeneratorType.COMPLEX;
      }
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
    // if there is no ',' and ':', then it's a key value
    if (!recordKey.contains(DEFAULT_RECORD_KEY_PARTS_SEPARATOR) || !recordKey.contains(DEFAULT_COLUMN_VALUE_SEPARATOR)) {
      return new String[] {recordKey};
    }
    // complex key case
    // Here we're reducing memory allocation for substrings and use index positions,
    // because for bucket index this will be called for each record, which leads to GC overhead
    int keyValueSep1;
    int keyValueSep2;
    int commaPosition;
    String currentField;
    String currentValue;
    List<String> values = new ArrayList<>();
    int processed = 0;
    while (processed < recordKey.length()) {
      // note that keyValueSeps and commaPosition are absolute
      keyValueSep1 = recordKey.indexOf(DEFAULT_COLUMN_VALUE_SEPARATOR, processed);
      currentField = recordKey.substring(processed, keyValueSep1);
      keyValueSep2 = recordKey.indexOf(DEFAULT_COLUMN_VALUE_SEPARATOR, keyValueSep1 + 1);
      if (fields.isEmpty() || (fields.size() == 1 && fields.get(0).isEmpty()) || fields.contains(currentField)) {
        if (keyValueSep2 < 0) {
          // there is no next key value pair
          currentValue = recordKey.substring(keyValueSep1 + 1);
          processed = recordKey.length();
        } else {
          // looking for ',' in reverse order to support multiple ',' in key values by looking for the latest ','
          commaPosition = recordKey.lastIndexOf(DEFAULT_RECORD_KEY_PARTS_SEPARATOR, keyValueSep2);
          // commaPosition could be -1 if didn't find ',', or we could find ',' from previous key-value pair ('col1:val1,...')
          // also we could have the last value with ':', so need to check if keyValueSep2 > 0
          while (commaPosition < keyValueSep1 && keyValueSep2 > 0) {
            // If we have key value as a timestamp with ':',
            // then we continue to skip ':' until before the next ':' there is a ',' character.
            // For instance, 'col1:val1,col2:2014-10-22 13:50:42,col3:val3'
            //                              ^             ^  ^       ^
            //   1)              keyValueSep1          skip  skip    keyValueSep2
            //                                                  ^
            //                                                  commaPosition
            //   2)                         |   currentValue    |
            //                                                  ^
            //   3)                                             processed
            keyValueSep2 = recordKey.indexOf(DEFAULT_COLUMN_VALUE_SEPARATOR, keyValueSep2 + 1);
            commaPosition = recordKey.lastIndexOf(DEFAULT_RECORD_KEY_PARTS_SEPARATOR, keyValueSep2);
          }
          if (commaPosition > 0) {
            currentValue = recordKey.substring(keyValueSep1 + 1, commaPosition);
            processed = commaPosition + 1;
          } else {
            // it could be the last value with many ':', in this case we wouldn't find any ',' before
            currentValue = recordKey.substring(keyValueSep1 + 1);
            processed = recordKey.length();
          }
        }
        // here could be any logic of conditional replacing of currentValue
        if (currentValue.equals(NULL_RECORDKEY_PLACEHOLDER)) {
          values.add(null);
        } else if (currentValue.equals(EMPTY_RECORDKEY_PLACEHOLDER)) {
          values.add("");
        } else {
          values.add(currentValue);
        }
      } else {
        if (keyValueSep2 < 0) {
          processed = recordKey.length();
        } else {
          commaPosition = recordKey.lastIndexOf(DEFAULT_RECORD_KEY_PARTS_SEPARATOR, keyValueSep2);
          while (commaPosition < keyValueSep1) {
            // described above
            keyValueSep2 = recordKey.indexOf(DEFAULT_COLUMN_VALUE_SEPARATOR, keyValueSep2 + 1);
            commaPosition = recordKey.lastIndexOf(DEFAULT_RECORD_KEY_PARTS_SEPARATOR, keyValueSep2);
          }
          if (commaPosition < 0) {
            // if something went wrong, and there is no ',', we should stop here, and pass the whole recordKey,
            // otherwise processed = commaPosition + 1 would lead to infinite loop
            processed = recordKey.length();
          } else {
            processed = commaPosition + 1;
          }
        }
      }
    }
    return values.isEmpty() ? new String[] {recordKey} : values.toArray(new String[0]);
  }

  public static String getRecordKey(GenericRecord record, List<String> recordKeyFields, boolean consistentLogicalTimestampEnabled) {
    BiFunction<String, Integer, String> valueFunction = (recordKeyField, index) -> {
      try {
        return HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, false, consistentLogicalTimestampEnabled);
      } catch (HoodieException e) {
        throw new HoodieKeyException("Record key field '" + recordKeyField + "' does not exist in the input record");
      }
    };
    return constructRecordKey(recordKeyFields.toArray(new String[]{}), valueFunction);
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

  public static boolean isComplexKeyGeneratorWithSingleRecordKeyField(HoodieTableConfig tableConfig) {
    Option<String[]> recordKeyFields = tableConfig.getRecordKeyFields();
    return KeyGeneratorType.isComplexKeyGenerator(tableConfig)
        && recordKeyFields.isPresent() && recordKeyFields.get().length == 1;
  }

  public static String getComplexKeygenErrorMessage(String operation) {
    return "This table uses the complex key generator with a single record "
        + "key field. If the table is written with Hudi 0.14.1, 0.15.0, 1.0.0, 1.0.1, or 1.0.2 "
        + "release before, the table may potentially contain duplicates due to a breaking "
        + "change in the key encoding in the _hoodie_record_key meta field (HUDI-7001) which "
        + "is crucial for upserts. Please take action based on the details on the deployment "
        + "guide (https://hudi.apache.org/docs/deployment#complex-key-generator) "
        + "before resuming the " + operation + " to the this table. If you're certain "
        + "that the table is not affected by the key encoding change, set "
        + "`hoodie.write.complex.keygen.validation.enable=false` to skip this validation.";
  }

  public static boolean encodeSingleKeyFieldNameForComplexKeyGen(TypedProperties props) {
    Integer tableVersionCode = ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION);
    HoodieTableVersion tableVersion = HoodieTableVersion.fromVersionCode(tableVersionCode);
    return tableVersion.greaterThanOrEquals(HoodieTableVersion.NINE)
        || !ConfigUtils.getBooleanWithAltKeys(props, COMPLEX_KEYGEN_NEW_ENCODING);
  }

  public static boolean mayUseNewEncodingForComplexKeyGen(HoodieTableConfig tableConfig) {
    return tableConfig.getTableVersion().lesserThan(HoodieTableVersion.NINE)
        && isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig);
  }
}
