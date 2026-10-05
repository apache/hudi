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
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.parser.BaseHoodieDateTimeParser;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION;

public class KeyGenUtils {

  /**
   * How many of the most recent commits to inspect when deducing the record key encoding from data. Bounded so
   * that a table whose latest commits wrote no data files does not turn a write into a full timeline scan.
   */
  private static final int MAX_INSTANTS_SCANNED_FOR_ENCODING = 20;
  private static final Logger LOG = LoggerFactory.getLogger(KeyGenUtils.class);

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

  /**
   * Guidance for a write that keys records on a tracked table whose encoding has not been recorded yet.
   */
  public static String getComplexKeygenEncodingMissingMessage() {
    return "This table uses the complex key generator with a single record key field, but "
        + HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key() + " is not recorded in hoodie.properties, so the "
        + "writer cannot tell whether the stored _hoodie_record_key values carry the `<field>:` prefix (HUDI-7001). "
        + "The Spark datasource, Spark SQL, Hudi Streamer and the Java write client record it from the table's data "
        + "when ingestion is set up, and so does any table upgrade or downgrade: run one of those first, or set the "
        + "property to FIELD_PREFIXED or VALUE_ONLY to match the stored keys. See "
        + "https://hudi.apache.org/docs/deployment#complex-key-generator.";
  }

  /**
   * Guidance for a write whose config would key records differently from the encoding the table records.
   */
  public static String getComplexKeygenEncodingMismatchMessage(ComplexKeyGenEncoding recorded) {
    return "This table uses the complex key generator with a single record key field and records "
        + HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key() + "=" + recorded + ", but this writer's config would key "
        + "records " + (recorded.encodesFieldName() ? "without" : "with") + " the `<field>:` prefix, so its records "
        + "would not match the stored ones (HUDI-7001). Set " + HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key() + "="
        + recorded + " (or hoodie.write.complex.keygen.new.encoding=" + !recorded.encodesFieldName()
        + ") on the writer, or restart a job that planned its keys before the encoding was recorded.";
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

  /**
   * Whether a complex key generator with a single record key field prepends the field name to the key.
   *
   * <p>{@link HoodieTableConfig#COMPLEX_KEYGEN_ENCODING} wins whenever it is present in the properties, because
   * it describes what the table's data carries. Otherwise {@code hoodie.write.complex.keygen.new.encoding} decides.
   */
  public static boolean encodeSingleKeyFieldNameForComplexKeyGen(TypedProperties props) {
    String tableEncoding = ConfigUtils.getStringWithAltKeys(props, HoodieTableConfig.COMPLEX_KEYGEN_ENCODING, StringUtils.EMPTY_STRING);
    if (!StringUtils.isNullOrEmpty(tableEncoding)) {
      return ComplexKeyGenEncoding.fromString(tableEncoding).encodesFieldName();
    }
    return !ConfigUtils.getBooleanWithAltKeys(props, COMPLEX_KEYGEN_NEW_ENCODING);
  }

  /**
   * Whether the table's record key encoding is tracked by {@link HoodieTableConfig#COMPLEX_KEYGEN_ENCODING}:
   * a complex key generator with a single record key field and populated meta fields.
   * Without the meta fields there is no stored key whose encoding could diverge from the key generator's.
   */
  public static boolean requireComplexKeyGenEncodingTracked(HoodieTableConfig tableConfig) {
    return tableConfig.isComplexKeyGenWithSingleRecordKeyField() && tableConfig.populateMetaFields();
  }

  /**
   * Cheap pre-check on the write config alone, before any table config is loaded: whether the write may target a
   * table whose encoding is tracked and not yet recorded. A write config that names a built-in key generator other
   * than the complex one, keys on several fields or does not populate the meta fields cannot need the recording, so
   * callers skip loading the table config for it. A key generator that is not a built-in one (such as the
   * wrapper Spark SQL configures) is decided by the table's own key generator class when it is present, and
   * cannot be ruled out otherwise.
   */
  public static boolean mayNeedComplexKeyGenEncodingRecorded(HoodieConfig writeConfig) {
    // the recorded encoding is a table property: a write option of the same name does not say it is recorded
    if (!writeConfig.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)) {
      return false;
    }
    // the table's own key generator, merged into the write options for existing tables, is what keyed its data
    String keyGeneratorClass = writeConfig.getProps().getProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key());
    if (keyGeneratorClass == null && writeConfig.contains(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME)) {
      keyGeneratorClass = writeConfig.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME);
    }
    if (keyGeneratorClass == null || !keyGeneratorClass.trim().startsWith(BUILT_IN_KEY_GENERATOR_PACKAGE)) {
      // no key generator named, or a wrapper / custom class: the table config has to decide
      return true;
    }
    if (!isComplexKeyGeneratorClass(keyGeneratorClass.trim())) {
      return false;
    }
    String recordKeyFields = writeConfig.contains(KeyGeneratorOptions.RECORDKEY_FIELD_NAME)
        ? writeConfig.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME)
        : writeConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS);
    return recordKeyFields != null && Arrays.stream(recordKeyFields.split(","))
        .map(String::trim).filter(field -> !field.isEmpty()).count() == 1;
  }

  private static final String BUILT_IN_KEY_GENERATOR_PACKAGE = "org.apache.hudi.keygen.";

  private static boolean isComplexKeyGeneratorClass(String keyGeneratorClass) {
    return keyGeneratorClass.equals(ComplexAvroKeyGenerator.class.getName())
        || keyGeneratorClass.equals("org.apache.hudi.keygen.ComplexKeyGenerator");
  }

  /**
   * The record key encoding a new single-field complex key generator table is created with: the explicitly declared
   * {@link HoodieTableConfig#COMPLEX_KEYGEN_ENCODING}, otherwise the one {@code hoodie.write.complex.keygen.new.encoding}
   * makes the writer produce.
   */
  public static ComplexKeyGenEncoding getDeclaredComplexKeyGenEncoding(TypedProperties props) {
    String declared = ConfigUtils.getStringWithAltKeys(props, HoodieTableConfig.COMPLEX_KEYGEN_ENCODING, StringUtils.EMPTY_STRING);
    return !StringUtils.isNullOrEmpty(declared)
        ? ComplexKeyGenEncoding.fromString(declared)
        : ComplexKeyGenEncoding.fromUseNewEncoding(ConfigUtils.getBooleanWithAltKeys(props, COMPLEX_KEYGEN_NEW_ENCODING));
  }

  /**
   * Records a missing complex key generator encoding before ingestion starts. Engine entry points call this
   * once during setup, before creating record keys. Existing data determines the encoding under the table lock.
   */
  public static void recordComplexKeygenEncodingIfMissing(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    if (!requireComplexKeyGenEncodingTracked(metaClient.getTableConfig())
        || metaClient.getTableConfig().getComplexKeyGenEncoding().isPresent()) {
      return;
    }
    TransactionManager transactionManager = new TransactionManager(config, metaClient.getStorage());
    try {
      transactionManager.beginTransaction(Option.empty(), Option.empty());
      try {
        metaClient.reloadTableConfig();
        if (metaClient.getTableConfig().getComplexKeyGenEncoding().isPresent()) {
          return;
        }
        ComplexKeyGenEncoding encoding = resolveComplexKeyGenEncodingForWrite(metaClient, config)
            .orElseThrow(() -> new HoodieException(getComplexKeygenErrorMessage("ingestion")));
        Properties props = new Properties();
        props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), encoding.name());
        HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), props);
        metaClient.reloadTableConfig();
        LOG.info("Recorded complex keygen record key encoding {} on table {}", encoding, metaClient.getBasePath());
      } finally {
        transactionManager.endTransaction(Option.empty());
      }
    } finally {
      transactionManager.close();
    }
  }

  /**
   * Resolves the record key encoding a writer must use and persist on a table whose encoding is tracked
   * ({@link #requireComplexKeyGenEncodingTracked}) but not yet recorded: for a table that was never written to,
   * the encoding the writer's config keys with ({@link #getDeclaredComplexKeyGenEncoding}); otherwise the encoding
   * deduced from the data,
   * falling back to the configured one when {@code hoodie.write.complex.keygen.validation.enable} is false.
   *
   * @return empty when the encoding cannot be determined and the validation is enabled; the caller fails
   * the operation with {@link #getComplexKeygenErrorMessage}
   */
  public static Option<ComplexKeyGenEncoding> resolveComplexKeyGenEncodingForWrite(HoodieTableMetaClient metaClient,
                                                                                    HoodieWriteConfig config) {
    if (!hasCompletedCommits(metaClient)) {
      // Nothing was ever written, so the keys this writer produces are the ones the table will carry.
      return Option.of(getDeclaredComplexKeyGenEncoding(config.getProps()));
    }
    Option<ComplexKeyGenEncoding> deduced = deduceComplexKeyGenEncodingFromData(metaClient);
    if (deduced.isPresent() || config.enableComplexKeygenValidation()) {
      return deduced;
    }
    ComplexKeyGenEncoding configured =
        ComplexKeyGenEncoding.fromUseNewEncoding(config.getBooleanOrDefault(COMPLEX_KEYGEN_NEW_ENCODING));
    LOG.warn("Could not determine the record key encoding of table {} from its data; recording the configured {} "
            + "because {} is disabled. If the existing keys are not {}, fix {} and rerun, or the records written "
            + "from now on will not match the existing ones.",
        metaClient.getBasePath(), configured, ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), configured,
        COMPLEX_KEYGEN_NEW_ENCODING.key());
    return Option.of(configured);
  }

  /**
   * Deduces the record key encoding of a single-field complex key generator table from the
   * {@code _hoodie_record_key} of a record its most recent commit wrote.
   *
   * @return {@link ComplexKeyGenEncoding#FIELD_PREFIXED} for a table that was never written to, the encoding of a
   * record the most recent commit with a readable data file wrote otherwise, or empty when none
   * of the commits inspected yields a record key
   */
  public static Option<ComplexKeyGenEncoding> deduceComplexKeyGenEncodingFromData(HoodieTableMetaClient metaClient) {
    String expectedPrefix = metaClient.getTableConfig().getRecordKeyFields().get()[0] + DEFAULT_COLUMN_VALUE_SEPARATOR;
    HoodieTimeline completedTimeline = getCompletedCommitsTimeline(metaClient);
    if (completedTimeline.empty()) {
      // Nothing was ever written, so there is no stored key whose encoding could differ from the canonical one.
      return Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED);
    }
    List<HoodieInstant> instantsToScan = completedTimeline.getReverseOrderedInstants()
        .limit(MAX_INSTANTS_SCANNED_FOR_ENCODING).collect(Collectors.toList());
    for (HoodieInstant instant : instantsToScan) {
      for (HoodieWriteStat writeStat : getWriteStats(instant, completedTimeline)) {
        if (StringUtils.isNullOrEmpty(writeStat.getPath())) {
          continue;
        }
        StoragePath path = new StoragePath(metaClient.getBasePath(), writeStat.getPath());
        Option<String> recordKey = readLatestRecordKey(metaClient, path, instant.getTimestamp());
        if (recordKey.isPresent()) {
          ComplexKeyGenEncoding encoding =
              ComplexKeyGenEncoding.fromUseNewEncoding(!recordKey.get().startsWith(expectedPrefix));
          LOG.info("Deduced complex keygen record key encoding {} of table {} from {}",
              encoding, metaClient.getBasePath(), path);
          return Option.of(encoding);
        }
      }
    }
    LOG.warn("The most recent {} commit(s) of table {} yielded no data file with a readable record key, so the "
            + "complex keygen record key encoding cannot be deduced from the data.",
        instantsToScan.size(), metaClient.getBasePath());
    return Option.empty();
  }

  /**
   * The table's current completed commits, read afresh (the caller may hold the lock and need commits made since its
   * meta client loaded the timeline) without replacing the timeline the caller's meta client caches.
   */
  private static HoodieTimeline getCompletedCommitsTimeline(HoodieTableMetaClient metaClient) {
    return new HoodieActiveTimeline(metaClient).getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Whether the table has any completed commit, read afresh; a table without one holds no stored key yet.
   */
  public static boolean hasCompletedCommits(HoodieTableMetaClient metaClient) {
    return !getCompletedCommitsTimeline(metaClient).empty();
  }

  private static List<HoodieWriteStat> getWriteStats(HoodieInstant instant, HoodieTimeline timeline) {
    try {
      return TimelineUtils.getCommitMetadata(instant, timeline).getWriteStats();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read the commit metadata of " + instant, e);
    }
  }

  /**
   * Reads the {@code _hoodie_record_key} of the record the given commit wrote, which is keyed the way that commit's
   * writer keys records: in a base file, the record with the greatest {@code _hoodie_commit_time} (a commit that
   * rewrites a file, e.g. one packing inserts into a small file, carries the file's older records with the keys
   * their writers gave them); in a log file, the first record of the data block the commit appended, else of the
   * newest data block. Empty when the file is missing, holds no record, or cannot be read: a single unreadable file
   * must not fail the deduction, which reports an undetermined encoding instead.
   */
  private static Option<String> readLatestRecordKey(HoodieTableMetaClient metaClient, StoragePath path, String instantTime) {
    HoodieStorage storage = metaClient.getStorage();
    try {
      if (!storage.exists(path)) {
        return Option.empty();
      }
      if (FSUtils.isLogFile(path)) {
        return readRecordKeyFromLogFile(storage, path, instantTime);
      }
      try (HoodieFileReader<IndexedRecord> reader = HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecordType.AVRO).getFileReader(new HoodieConfig(), path)) {
        Schema fileSchema = reader.getSchema();
        Schema projection = HoodieAvroUtils.generateProjectionSchema(fileSchema,
            Arrays.asList(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD));
        String latestCommitTime = null;
        String latestRecordKey = null;
        try (ClosableIterator<HoodieRecord<IndexedRecord>> records = reader.getRecordIterator(fileSchema, projection)) {
          while (records.hasNext()) {
            GenericRecord record = (GenericRecord) records.next().getData();
            Object commitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
            Object recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
            if (commitTime == null || recordKey == null) {
              continue;
            }
            if (latestCommitTime == null
                || HoodieTimeline.compareTimestamps(commitTime.toString(), HoodieTimeline.GREATER_THAN, latestCommitTime)) {
              latestCommitTime = commitTime.toString();
              latestRecordKey = recordKey.toString();
            }
            if (latestCommitTime.equals(instantTime)) {
              // nothing in this commit's file is newer than a record the commit itself wrote
              break;
            }
          }
        }
        return Option.ofNullable(latestRecordKey);
      }
    } catch (Exception e) {
      LOG.warn("Could not read a record key from {} to deduce the complex keygen record key encoding", path, e);
      return Option.empty();
    }
  }

  private static Option<String> readRecordKeyFromLogFile(HoodieStorage storage, StoragePath path, String instantTime) throws IOException {
    HoodieDataBlock chosen = null;
    try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(path), null)) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (block instanceof HoodieDataBlock) {
          chosen = (HoodieDataBlock) block;
          if (instantTime.equals(block.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME))) {
            break;
          }
        }
      }
      if (chosen == null) {
        return Option.empty();
      }
      try (ClosableIterator<HoodieRecord<Object>> records = chosen.getRecordIterator(HoodieRecordType.AVRO)) {
        return records.hasNext()
            ? Option.ofNullable(records.next().getRecordKey(chosen.getSchema(), HoodieRecord.RECORD_KEY_METADATA_FIELD))
            : Option.empty();
      }
    }
  }

}
