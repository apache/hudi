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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.parser.BaseHoodieDateTimeParser;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING;

public class KeyGenUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KeyGenUtils.class);
  public static final String COMPLEX_KEY_ENCODING_FILE_NAME = "complex_key_encoding";

  protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  protected static final String HUDI_DEFAULT_PARTITION_PATH = PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";
  public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";

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
    return Arrays.stream(fieldKV).map(kv -> kv.split(DEFAULT_COLUMN_VALUE_SEPARATOR, 2))
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
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(NULL_RECORDKEY_PLACEHOLDER);
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(EMPTY_RECORDKEY_PLACEHOLDER);
      } else {
        recordKey.append(recordKeyField).append(DEFAULT_COLUMN_VALUE_SEPARATOR).append(recordKeyValue);
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
        + "before resuming the " + operation + " to the table. If you're certain "
        + "that the table is not affected by the key encoding change, set "
        + "`hoodie.write.complex.keygen.validation.enable=false` to skip this validation.";
  }

  public static boolean encodeSingleKeyFieldNameForComplexKeyGen(TypedProperties props) {
    return !ConfigUtils.getBooleanWithAltKeys(props, COMPLEX_KEYGEN_NEW_ENCODING);
  }

  public static boolean mayUseNewEncodingForComplexKeyGen(HoodieTableConfig tableConfig) {
    return isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig);
  }

  public static StoragePath getComplexKeyEncodingFilePath(StoragePath basePath) {
    return new StoragePath(basePath, AUXILIARYFOLDER_NAME + "/" + COMPLEX_KEY_ENCODING_FILE_NAME);
  }

  public static Option<Boolean> readComplexKeyEncodingFromAuxFile(HoodieStorage storage, StoragePath basePath) {
    StoragePath encodingFilePath = getComplexKeyEncodingFilePath(basePath);
    try {
      if (storage.exists(encodingFilePath)) {
        Properties props = new Properties();
        try (InputStream inputStream = storage.open(encodingFilePath)) {
          props.load(inputStream);
        }
        String value = props.getProperty(COMPLEX_KEYGEN_NEW_ENCODING.key());
        if (value != null) {
          return Option.of(Boolean.parseBoolean(value));
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to read complex key encoding from aux file: {}", encodingFilePath, e);
    }
    return Option.empty();
  }

  public static void writeComplexKeyEncodingToAuxFile(HoodieStorage storage, StoragePath basePath, boolean useNewEncoding) {
    StoragePath encodingFilePath = getComplexKeyEncodingFilePath(basePath);
    try {
      Properties props = new Properties();
      props.setProperty(COMPLEX_KEYGEN_NEW_ENCODING.key(), String.valueOf(useNewEncoding));
      try (OutputStream outputStream = storage.create(encodingFilePath, true)) {
        props.store(outputStream, "Complex key generator encoding format");
      }
      LOG.info("Wrote complex key encoding useNewEncoding={} to aux file {}", useNewEncoding, encodingFilePath);
    } catch (IOException e) {
      throw new HoodieKeyException("Failed to write complex key encoding file to " + encodingFilePath, e);
    }
  }

  /**
   * Deduces the complex-key encoding actually used by an existing table by inspecting a stored
   * {@code _hoodie_record_key} from one of its base files.
   *
   * @return the deduced encoding ({@code Option.of(true)} for the new bare-value encoding,
   *         {@code Option.of(false)} for the legacy {@code field:value} encoding), or
   *         {@code Option.empty()} when the encoding cannot be determined from data (a new/empty
   *         table, or a table with no readable base file yet, e.g. a MoR table with only log files).
   *         Callers must not cache an empty (undetermined) result, so that a later write can
   *         re-deduce once a readable base file exists rather than pinning a possibly-wrong guess.
   */
  public static Option<Boolean> deduceComplexKeyEncodingFromData(HoodieTableMetaClient metaClient, String recordKeyFieldName) {
    HoodieTimeline completedTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    if (completedTimeline.empty()) {
      LOG.info("No completed commits found in table {}; cannot deduce complex key encoding (new/empty table).",
          metaClient.getBasePath());
      return Option.empty();
    }

    try {
      HoodieStorage storage = metaClient.getStorage();
      // Use the table's actual base file format instead of assuming parquet, so ORC/HFILE tables
      // are inspected correctly rather than being skipped and mis-deduced.
      HoodieFileFormat baseFileFormat = metaClient.getTableConfig().getBaseFileFormat();
      FileFormatUtils fileFormatUtils = HoodieIOFactory.getIOFactory(storage)
          .getFileFormatUtils(baseFileFormat);
      String baseFileExtension = baseFileFormat.getFileExtension();

      List<HoodieInstant> instants = completedTimeline.getReverseOrderedInstants().collect(Collectors.toList());
      for (HoodieInstant instant : instants) {
        HoodieCommitMetadata commitMetadata = TimelineUtils.getCommitMetadata(instant, completedTimeline);
        for (HoodieWriteStat writeStat : commitMetadata.getWriteStats()) {
          String filePath = writeStat.getPath();
          if (filePath == null || filePath.isEmpty() || !filePath.endsWith(baseFileExtension)) {
            continue;
          }
          StoragePath baseFilePath = new StoragePath(metaClient.getBasePath(), filePath);
          if (!storage.exists(baseFilePath)) {
            continue;
          }
          try (ClosableIterator<HoodieKey> keyIterator = fileFormatUtils.getHoodieKeyIterator(storage, baseFilePath)) {
            if (keyIterator.hasNext()) {
              HoodieKey hoodieKey = keyIterator.next();
              String hoodieRecordKey = hoodieKey.getRecordKey();
              String expectedPrefix = recordKeyFieldName + DEFAULT_COLUMN_VALUE_SEPARATOR;
              boolean usesNewEncoding = !hoodieRecordKey.startsWith(expectedPrefix);
              LOG.info("Deduced complex key encoding from base file {} (commit {}): useNewEncoding={}",
                  baseFilePath, instant.getTimestamp(), usesNewEncoding);
              return Option.of(usesNewEncoding);
            }
          }
        }
      }

      // Non-empty timeline but no readable base file (e.g. a MoR table whose latest slices are
      // log-only). We cannot determine the encoding from data here; signal "undetermined" so the
      // caller applies the new-table default without caching it permanently.
      LOG.info("No readable {} base file with records found in table {}; cannot deduce complex key encoding.",
          baseFileExtension, metaClient.getBasePath());
      return Option.empty();
    } catch (IOException e) {
      throw new HoodieException("Failed to deduce complex key encoding from base files in table "
          + metaClient.getBasePath(), e);
    }
  }
}
