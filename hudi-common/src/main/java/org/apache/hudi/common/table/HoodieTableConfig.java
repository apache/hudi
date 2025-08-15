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

package org.apache.hudi.common.table;

import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.NativeTableFormat;
import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.OrderedProperties;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.CUSTOM;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.DATE_TIME_PARSER;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.INPUT_TIME_UNIT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.apache.hudi.common.model.AWSDmsAvroPayload.DELETE_OPERATION_VALUE;
import static org.apache.hudi.common.model.AWSDmsAvroPayload.OP_FIELD;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.util.ConfigUtils.fetchConfigs;
import static org.apache.hudi.common.util.ConfigUtils.recoverIfNeeded;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

@Immutable
@ConfigClassProperty(name = "Hudi Table Basic Configs",
    groupName = ConfigGroups.Names.TABLE_CONFIG,
    description = "Configurations of the Hudi Table like type of ingestion, storage formats, hive table name etc."
        + " Configurations are loaded from hoodie.properties, these properties are usually set during"
        + " initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.")
public class HoodieTableConfig extends HoodieConfig {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_PROPERTIES_FILE_BACKUP = "hoodie.properties.backup";
  public static final String HOODIE_WRITE_TABLE_NAME_KEY = "hoodie.datasource.write.table.name";
  public static final String HOODIE_TABLE_NAME_KEY = "hoodie.table.name";
  public static final String PARTIAL_UPDATE_CUSTOM_MARKER = "hoodie.write.partial.update.custom.marker";
  public static final String DEBEZIUM_UNAVAILABLE_VALUE = "__debezium_unavailable_value";
  // This prefix is used to set merging related properties.
  // A reader might need to read some writer properties to function as expected,
  // and Hudi stores properties with this prefix so the reader parses these properties to fetch any custom property.
  public static final String RECORD_MERGE_PROPERTY_PREFIX = "hoodie.record.merge.property.";
  public static final Set<String> PAYLOADS_UNDER_DEPRECATION = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          AWSDmsAvroPayload.class.getName(),
          DefaultHoodieRecordPayload.class.getName(),
          EventTimeAvroPayload.class.getName(),
          MySqlDebeziumAvroPayload.class.getName(),
          OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
          OverwriteWithLatestAvroPayload.class.getName(),
          PartialUpdateAvroPayload.class.getName(),
          PostgresDebeziumAvroPayload.class.getName())));

  public static final Set<String> EVENT_TIME_ORDERING_PAYLOADS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          DefaultHoodieRecordPayload.class.getName(),
          EventTimeAvroPayload.class.getName(),
          MySqlDebeziumAvroPayload.class.getName(),
          PartialUpdateAvroPayload.class.getName(),
          PostgresDebeziumAvroPayload.class.getName())));

  public static final Set<String> BUILTIN_MERGE_STRATEGIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          CUSTOM_MERGE_STRATEGY_UUID,
          EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          PAYLOAD_BASED_MERGE_STRATEGY_UUID)));

  public static final ConfigProperty<String> DATABASE_NAME = ConfigProperty
      .key("hoodie.database.name")
      .noDefaultValue()
      .withDocumentation("Database name. If different databases have the same table name during incremental query, "
          + "we can set it to limit the table name under a specific database");

  public static final ConfigProperty<String> NAME = ConfigProperty
      .key(HOODIE_TABLE_NAME_KEY)
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with Hive. Needs to be same across runs.");

  public static final ConfigProperty<HoodieTableType> TYPE = ConfigProperty
      .key("hoodie.table.type")
      .defaultValue(HoodieTableType.COPY_ON_WRITE)
      .withDocumentation("The table type for the underlying data.");

  public static final ConfigProperty<HoodieTableVersion> VERSION = ConfigProperty
      .key("hoodie.table.version")
      .defaultValue(HoodieTableVersion.current())
      .withDocumentation("Version of table, used for running upgrade/downgrade steps between releases with potentially "
          + "breaking/backwards compatible changes.");

  public static final ConfigProperty<HoodieTableVersion> INITIAL_VERSION = ConfigProperty
      .key("hoodie.table.initial.version")
      .defaultValue(HoodieTableVersion.current())
      .sinceVersion("1.0.0")
      .withDocumentation("Initial Version of table when the table was created. Used for upgrade/downgrade"
          + " to identify what upgrade/downgrade paths happened on the table. This is only configured "
          + "when the table is initially setup.");

  // TODO: is this this called precombine in 1.0. ..
  public static final ConfigProperty<String> PRECOMBINE_FIELDS = ConfigProperty
      .key("hoodie.table.precombine.field")
      .noDefaultValue()
      .withDocumentation("Comma separated fields used in preCombining before actual write. By default, when two records have the same key value, "
          + "the largest value for the precombine field determined by Object.compareTo(..), is picked. If there are multiple fields configured, "
          + "comparison is made on the first field. If the first field values are same, comparison is made on the second field and so on.");

  public static final ConfigProperty<String> PARTITION_FIELDS = ConfigProperty
      .key("hoodie.table.partition.fields")
      .noDefaultValue()
      .withDocumentation("Comma separated field names used to partition the table. These field names also include "
          + "the partition type which is used by custom key generators");

  public static final ConfigProperty<String> RECORDKEY_FIELDS = ConfigProperty
      .key("hoodie.table.recordkey.fields")
      .noDefaultValue()
      .withDocumentation("Columns used to uniquely identify the table. Concatenated values of these fields are used as "
          + " the record key component of HoodieKey.");

  public static final ConfigProperty<Boolean> CDC_ENABLED = ConfigProperty
      .key("hoodie.table.cdc.enabled")
      .defaultValue(false)
      .sinceVersion("0.13.0")
      .withDocumentation("When enable, persist the change data if necessary, and can be queried as a CDC query mode.");

  public static final ConfigProperty<String> CDC_SUPPLEMENTAL_LOGGING_MODE = ConfigProperty
      .key("hoodie.table.cdc.supplemental.logging.mode")
      .defaultValue(HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER.name())
      .withDocumentation(HoodieCDCSupplementalLoggingMode.class)
      .sinceVersion("0.13.0");

  // TODO: is this necessary? won't we just use a table schema.
  public static final ConfigProperty<String> CREATE_SCHEMA = ConfigProperty
      .key("hoodie.table.create.schema")
      .noDefaultValue()
      .withDocumentation("Schema used when creating the table");

  public static final ConfigProperty<HoodieFileFormat> BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.table.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withAlternatives("hoodie.table.ro.file.format")
      .withDocumentation("Base file format to store all the base file data.");

  public static final ConfigProperty<HoodieFileFormat> LOG_FILE_FORMAT = ConfigProperty
      .key("hoodie.table.log.file.format")
      .defaultValue(HoodieFileFormat.HOODIE_LOG)
      .withAlternatives("hoodie.table.rt.file.format")
      .withDocumentation("Log format used for the delta logs.");

  public static final ConfigProperty<String> TIMELINE_LAYOUT_VERSION = ConfigProperty
      .key("hoodie.timeline.layout.version")
      .noDefaultValue()
      .withDocumentation("Version of timeline used, by the table.");

  public static final ConfigProperty<String> TABLE_FORMAT = ConfigProperty
      .key("hoodie.table.format")
      .defaultValue(NativeTableFormat.TABLE_FORMAT)
      .withDocumentation("Table format name used when writing to the table.");

  public static final ConfigProperty<RecordMergeMode> RECORD_MERGE_MODE = ConfigProperty
      .key("hoodie.record.merge.mode")
      .defaultValue((RecordMergeMode) null,
          "COMMIT_TIME_ORDERING if precombine is not set; EVENT_TIME_ORDERING if precombine is set")
      .sinceVersion("1.0.0")
      .withDocumentation(RecordMergeMode.class);

  public static final ConfigProperty<String> PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .noDefaultValue()
      .deprecatedAfter("1.0.0")
      .withDocumentation("Payload class to use for performing merges, compactions, i.e merge delta logs with current base file and then "
          + " produce a new base file.");

  public static final ConfigProperty<String> LEGACY_PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.table.legacy.payload.class")
      .noDefaultValue()
      .sinceVersion("1.1.0")
      .withDocumentation("Payload class to indicate the payload class that is used to create the table and is not used anymore.");

  // This is the default payload class used by Hudi 0.x releases (table version 6 and below)
  public static final String DEFAULT_PAYLOAD_CLASS_NAME = DefaultHoodieRecordPayload.class.getName();

  public static final ConfigProperty<String> RECORD_MERGE_STRATEGY_ID = ConfigProperty
      .key("hoodie.record.merge.strategy.id")
      .noDefaultValue()
      .withAlternatives("hoodie.compaction.record.merger.strategy")
      .sinceVersion("0.13.0")
      .withDocumentation("Id of merger strategy. Hudi will pick HoodieRecordMerger implementations in `"
          + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "` which has the same merger strategy id");

  public static final ConfigProperty<String> ARCHIVELOG_FOLDER = ConfigProperty
      .key("hoodie.archivelog.folder")
      .defaultValue("archived")
      .deprecatedAfter("1.0.0")
      .withDocumentation("path under the meta folder, to store archived timeline instants at.");

  public static final ConfigProperty<String> TIMELINE_HISTORY_PATH = ConfigProperty
      .key("hoodie.timeline.history.path")
      .defaultValue("history")
      .withDocumentation("path under the meta folder, to store timeline history at.");

  public static final ConfigProperty<String> TIMELINE_PATH = ConfigProperty
      .key("hoodie.timeline.path")
      .defaultValue("timeline")
      .withDocumentation("path under the meta folder, to store timeline instants at.");

  public static final ConfigProperty<Boolean> BOOTSTRAP_INDEX_ENABLE = ConfigProperty
      .key("hoodie.bootstrap.index.enable")
      .defaultValue(true)
      .withDocumentation("Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined, default true.");

  public static final ConfigProperty<String> BOOTSTRAP_INDEX_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .deprecatedAfter("1.0.0")
      .withDocumentation("Implementation to use, for mapping base files to bootstrap base file, that contain actual data.");

  public static final ConfigProperty<String> BOOTSTRAP_INDEX_TYPE = ConfigProperty
      .key("hoodie.bootstrap.index.type")
      .defaultValue(BootstrapIndexType.HFILE.name())
      .sinceVersion("1.0.0")
      .withDocumentation("Bootstrap index type determines which implementation to use, for mapping base files to bootstrap base file, that contain actual data.");

  public static final ConfigProperty<String> BOOTSTRAP_BASE_PATH = ConfigProperty
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .withDocumentation("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final ConfigProperty<Boolean> POPULATE_META_FIELDS = ConfigProperty
      .key("hoodie.populate.meta.fields")
      .defaultValue(true)
      .withDocumentation("When enabled, populates all meta fields. When disabled, no meta fields are populated "
          + "and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing");

  public static final ConfigProperty<String> KEY_GENERATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.table.keygenerator.class")
      .noDefaultValue()
      .deprecatedAfter("1.0.0")
      .withDocumentation("Key Generator class property for the hoodie table");

  public static final ConfigProperty<String> KEY_GENERATOR_TYPE = ConfigProperty
      .key("hoodie.table.keygenerator.type")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("Key Generator type to determine key generator class");

  // TODO: this has to be UTC. why is it not the default?
  public static final ConfigProperty<HoodieTimelineTimeZone> TIMELINE_TIMEZONE = ConfigProperty
      .key("hoodie.table.timeline.timezone")
      .defaultValue(HoodieTimelineTimeZone.LOCAL)
      .withDocumentation("User can set hoodie commit timeline timezone, such as utc, local and so on. local is default");

  public static final ConfigProperty<Boolean> PARTITION_METAFILE_USE_BASE_FORMAT = ConfigProperty
      .key("hoodie.partition.metafile.use.base.format")
      .defaultValue(false)
      .withDocumentation("If true, partition metafiles are saved in the same format as base-files for this dataset (e.g. Parquet / ORC). "
          + "If false (default) partition metafiles are saved as properties files.");

  public static final ConfigProperty<Boolean> DROP_PARTITION_COLUMNS = ConfigProperty
      .key("hoodie.datasource.write.drop.partition.columns")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("When set to true, will not write the partition columns into hudi. By default, false.");

  public static final ConfigProperty<Boolean> MULTIPLE_BASE_FILE_FORMATS_ENABLE = ConfigProperty
      .key("hoodie.table.multiple.base.file.formats.enable")
      .defaultValue(false)
      .sinceVersion("1.0.0")
      .withDocumentation("When set to true, the table can support reading and writing multiple base file formats.");

  public static final ConfigProperty<PartialUpdateMode> PARTIAL_UPDATE_MODE = ConfigProperty
      .key("hoodie.table.partial.update.mode")
      .defaultValue(PartialUpdateMode.NONE)
      .sinceVersion("1.1.0")
      .withDocumentation("This property when set, will define how two versions of the record will be "
          + "merged together where the later contains only partial set of values and not entire record.");

  public static final ConfigProperty<String> URL_ENCODE_PARTITIONING = KeyGeneratorOptions.URL_ENCODE_PARTITIONING;
  public static final ConfigProperty<String> HIVE_STYLE_PARTITIONING_ENABLE = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE;

  public static final List<ConfigProperty<String>> PERSISTED_CONFIG_LIST = Arrays.asList(
      TIMESTAMP_TYPE_FIELD,
      INPUT_TIME_UNIT,
      TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX,
      TIMESTAMP_INPUT_DATE_FORMAT,
      TIMESTAMP_INPUT_TIMEZONE_FORMAT,
      TIMESTAMP_OUTPUT_DATE_FORMAT,
      TIMESTAMP_OUTPUT_TIMEZONE_FORMAT,
      TIMESTAMP_TIMEZONE_FORMAT,
      DATE_TIME_PARSER
  );

  private static final Set<String> CONFIGS_REQUIRED_FOR_OLDER_VERSIONED_TABLES = new HashSet<>(Arrays.asList(
      KEY_GENERATOR_CLASS_NAME.key(),
      KEY_GENERATOR_TYPE.key()
  ));

  public static final ConfigProperty<String> TABLE_CHECKSUM = ConfigProperty
      .key("hoodie.table.checksum")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Table checksum is used to guard against partial writes in HDFS. It is added as the last entry in hoodie.properties and then used to validate while reading table config.");

  // TODO: understand why is writing/changing all these. this has to work on both HDFS and Cloud.
  public static final ConfigProperty<String> TABLE_METADATA_PARTITIONS_INFLIGHT = ConfigProperty
      .key("hoodie.table.metadata.partitions.inflight")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of metadata partitions whose building is in progress. "
          + "These partitions are not yet ready for use by the readers.");

  public static final ConfigProperty<String> TABLE_METADATA_PARTITIONS = ConfigProperty
      .key("hoodie.table.metadata.partitions")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Comma-separated list of metadata partitions that have been completely built and in-sync with data table. "
          + "These partitions are ready for use by the readers");

  public static final ConfigProperty<String> SECONDARY_INDEXES_METADATA = ConfigProperty
      .key("hoodie.table.secondary.indexes.metadata")
      .noDefaultValue()
      .sinceVersion("0.13.0")
      .withDocumentation("The metadata of secondary indexes");

  public static final ConfigProperty<String> RELATIVE_INDEX_DEFINITION_PATH = ConfigProperty
      .key("hoodie.table.index.defs.path")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("Relative path to table base path where the index definitions are stored");

  private static final String TABLE_CHECKSUM_FORMAT = "%s.%s"; // <database_name>.<table_name>

  static List<ConfigProperty<?>> definedTableConfigs() {
    Field[] fields = ReflectionUtils.getClass(HoodieTableConfig.class.getName()).getDeclaredFields();
    return Arrays.stream(fields)
        .filter(f -> f.getType().equals(ConfigProperty.class)
            && Modifier.isPublic(f.getModifiers())
            && Modifier.isStatic(f.getModifiers())
        )
        .map(f -> {
          try {
            return (ConfigProperty<?>) f.get(null);
          } catch (IllegalAccessException e) {
            throw new HoodieException("Error reading defined table configs, for " + f.getName(), e);
          }
        })
        .collect(Collectors.toList());
  }

  /**
   * Loads the table config from properties file.
   *
   * @param storage  The storage.
   * @param basePath The table base path.
   *
   * @return The reloaded table config.
   */
  public static HoodieTableConfig loadFromHoodieProps(HoodieStorage storage, String basePath) {
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    return new HoodieTableConfig(storage, metaPath);
  }

  /**
   * Loads the table config from properties file.
   *
   * @param storage  The storage.
   * @param metaPath The table metadata path.
   *
   * @return The reloaded table config.
   */
  public static HoodieTableConfig loadFromHoodieProps(HoodieStorage storage, StoragePath metaPath) {
    return new HoodieTableConfig(storage, metaPath);
  }

  public HoodieTableConfig(HoodieStorage storage, StoragePath metaPath) {
    super();
    StoragePath propertyPath = new StoragePath(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      this.props = fetchConfigs(storage, metaPath, HOODIE_PROPERTIES_FILE, HOODIE_PROPERTIES_FILE_BACKUP, MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);
    } catch (IOException e) {
      throw new HoodieIOException("Could not load properties from " + propertyPath, e);
    }
  }

  private static Properties getOrderedPropertiesWithTableChecksum(Properties props) {
    Properties orderedProps = new OrderedProperties(props);
    orderedProps.put(TABLE_CHECKSUM.key(), String.valueOf(generateChecksum(props)));
    return orderedProps;
  }

  /**
   * Write the properties to the given output stream and return the table checksum.
   *
   * @param props        - properties to be written
   * @param outputStream - output stream to which properties will be written
   * @param propertyPath - Path of the file where properties would be stored
   * @return return the table checksum
   * @throws IOException
   */
  private static String storeProperties(Properties props, OutputStream outputStream, StoragePath propertyPath) throws IOException {
    final String checksum;
    if (isValidChecksum(props)) {
      checksum = props.getProperty(TABLE_CHECKSUM.key());
      props.store(outputStream, "Updated at " + Instant.now());
    } else {
      Properties propsWithChecksum = getOrderedPropertiesWithTableChecksum(props);
      propsWithChecksum.store(outputStream, "Properties saved on " + Instant.now());
      checksum = propsWithChecksum.getProperty(TABLE_CHECKSUM.key());
      props.setProperty(TABLE_CHECKSUM.key(), checksum);
    }
    LOG.info("Created properties file at " + propertyPath);
    return checksum;
  }

  private static boolean isValidChecksum(Properties props) {
    return props.containsKey(TABLE_CHECKSUM.key()) && validateChecksum(props);
  }

  /**
   * For serializing and de-serializing.
   */
  public HoodieTableConfig() {
    super();
  }

  public static void recover(HoodieStorage storage, StoragePath metadataFolder) throws IOException {
    StoragePath cfgPath = new StoragePath(metadataFolder, HOODIE_PROPERTIES_FILE);
    StoragePath backupCfgPath = new StoragePath(metadataFolder, HOODIE_PROPERTIES_FILE_BACKUP);
    recoverIfNeeded(storage, cfgPath, backupCfgPath);
  }

  private static void modify(HoodieStorage storage, StoragePath metadataFolder, Properties modifyProps, BiConsumer<Properties, Properties> modifyFn) {
    StoragePath cfgPath = new StoragePath(metadataFolder, HOODIE_PROPERTIES_FILE);
    StoragePath backupCfgPath = new StoragePath(metadataFolder, HOODIE_PROPERTIES_FILE_BACKUP);
    try {
      // 0. do any recovery from prior attempts.
      recoverIfNeeded(storage, cfgPath, backupCfgPath);

      // 1. Read the existing config
      TypedProperties props = fetchConfigs(storage, metadataFolder, HOODIE_PROPERTIES_FILE, HOODIE_PROPERTIES_FILE_BACKUP, MAX_READ_RETRIES, READ_RETRY_DELAY_MSEC);

      // 2. backup the existing properties.
      try (OutputStream out = storage.create(backupCfgPath, false)) {
        storeProperties(props, out, backupCfgPath);
      }

      // 3. delete the properties file, reads will go to the backup, until we are done.
      deleteFile(storage, cfgPath);

      // 4. Upsert and save back.
      String checksum;
      try (OutputStream out = storage.create(cfgPath, true)) {
        modifyFn.accept(props, modifyProps);
        checksum = storeProperties(props, out, cfgPath);
      }

      // 4. verify and remove backup.
      try (InputStream in = storage.open(cfgPath)) {
        props.clear();
        props.load(in);
        if (!props.containsKey(TABLE_CHECKSUM.key()) || !props.getProperty(TABLE_CHECKSUM.key()).equals(checksum)) {
          // delete the properties file and throw exception indicating update failure
          // subsequent writes will recover and update, reads will go to the backup until then
          deleteFile(storage, cfgPath);
          throw new HoodieIOException("Checksum property missing or does not match.");
        }
      }

      // 5. delete the backup properties file
      deleteFile(storage, backupCfgPath);
    } catch (IOException e) {
      throw new HoodieIOException("Error updating table configs.", e);
    }
  }

  private static void deleteFile(HoodieStorage storage, StoragePath cfgPath) throws IOException {
    storage.deleteFile(cfgPath);
    LOG.info("Deleted properties file at " + cfgPath);
  }

  /**
   * Upserts the table config with the set of properties passed in. We implement a fail-safe backup protocol
   * here for safely updating with recovery and also ensuring the table config continues to be readable.
   */
  public static void update(HoodieStorage storage, StoragePath metadataFolder,
                            Properties updatedProps) {
    modify(storage, metadataFolder, updatedProps, ConfigUtils::upsertProperties);
  }

  public static void delete(HoodieStorage storage, StoragePath metadataFolder, Set<String> deletedProps) {
    Properties props = new Properties();
    deletedProps.forEach(p -> props.setProperty(p, ""));
    modify(storage, metadataFolder, props, ConfigUtils::deleteProperties);
  }

  /**
   * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties).
   *
   * TODO: this directory creation etc should happen in the HoodieTableMetaClient.
   */
  public static void create(HoodieStorage storage, StoragePath metadataFolder, Properties properties)
      throws IOException {
    if (!storage.exists(metadataFolder)) {
      storage.createDirectory(metadataFolder);
    }
    HoodieConfig hoodieConfig = HoodieConfig.copy(properties);
    StoragePath propertyPath = new StoragePath(metadataFolder, HOODIE_PROPERTIES_FILE);
    HoodieTableVersion tableVersion = getTableVersion(hoodieConfig);
    try (OutputStream outputStream = storage.create(propertyPath)) {
      if (!hoodieConfig.contains(NAME)) {
        throw new IllegalArgumentException(NAME.key() + " property needs to be specified");
      }
      hoodieConfig.setDefaultValue(TYPE);
      hoodieConfig.setDefaultValue(TIMELINE_HISTORY_PATH);
      hoodieConfig.setDefaultValue(TIMELINE_PATH);
      if (!hoodieConfig.contains(TIMELINE_LAYOUT_VERSION)) {
        // Use latest Version as default unless forced by client
        hoodieConfig.setValue(TIMELINE_LAYOUT_VERSION, TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (hoodieConfig.contains(BOOTSTRAP_BASE_PATH)) {
        if (tableVersion.greaterThan(HoodieTableVersion.SEVEN)) {
          hoodieConfig.setDefaultValue(BOOTSTRAP_INDEX_TYPE, BootstrapIndexType.getBootstrapIndexType(hoodieConfig).toString());
        } else {
          // Use the default bootstrap index class.
          hoodieConfig.setDefaultValue(BOOTSTRAP_INDEX_CLASS_NAME, BootstrapIndexType.getDefaultBootstrapIndexClassName(hoodieConfig));
        }
      }
      if (hoodieConfig.contains(TIMELINE_TIMEZONE)) {
        HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getString(TIMELINE_TIMEZONE)));
      }
      hoodieConfig.setDefaultValue(DROP_PARTITION_COLUMNS);

      dropInvalidConfigs(hoodieConfig);
      storeProperties(hoodieConfig.getProps(), outputStream, propertyPath);
    }
  }

  public static long generateChecksum(Properties props) {
    if (!props.containsKey(NAME.key())) {
      throw new IllegalArgumentException(NAME.key() + " property needs to be specified");
    }
    String table = props.getProperty(NAME.key());
    String database = props.getProperty(DATABASE_NAME.key(), "");
    return BinaryUtil.generateChecksum(getUTF8Bytes(String.format(TABLE_CHECKSUM_FORMAT, database, table)));
  }

  public static boolean validateChecksum(Properties props) {
    return Long.parseLong(props.getProperty(TABLE_CHECKSUM.key())) == generateChecksum(props);
  }

  static void dropInvalidConfigs(HoodieConfig config) {
    HoodieTableVersion tableVersion = getTableVersion(config);
    Map<String, ConfigProperty<?>> definedTableConfigs = HoodieTableConfig.definedTableConfigs()
        .stream().collect(Collectors.toMap(ConfigProperty::key, Function.identity()));
    List<String> invalidConfigs = config.getProps().keySet().stream()
        .map(k -> (String) k)
        // TODO: this can be eventually tightened to ensure all table configs are defined.
        .filter(key -> definedTableConfigs.containsKey(key)
            && !validateConfigVersion(definedTableConfigs.get(key), tableVersion))
        .collect(Collectors.toList());
    invalidConfigs.forEach(key -> {
      config.getProps().remove(key);
    });
  }

  static boolean validateConfigVersion(ConfigProperty<?> configProperty, HoodieTableVersion tableVersion) {
    // TODO: this can be tightened up, once all configs have a since version.
    if (!configProperty.getSinceVersion().isPresent()) {
      return true;
    }
    // validate that the table version is greater than or equal to the config version
    HoodieTableVersion firstVersion = HoodieTableVersion.fromReleaseVersion(configProperty.getSinceVersion().get());
    boolean valid = tableVersion.greaterThan(firstVersion) || tableVersion.equals(firstVersion);
    valid = valid || CONFIGS_REQUIRED_FOR_OLDER_VERSIONED_TABLES.contains(configProperty.key());
    if (!valid) {
      LOG.warn("Table version {} is lower than or equal to config's first version {}. Config {} will be ignored.",
          tableVersion, firstVersion, configProperty.key());
    }
    return valid;
  }

  /**
   * This function returns the partition fields joined by BaseKeyGenerator.FIELD_SEPARATOR. It will also
   * include the key generator partition type with the field. The key generator partition type is used for
   * Custom Key Generator.
   */
  public static Option<String> getPartitionFieldPropForKeyGenerator(HoodieConfig config) {
    return Option.ofNullable(config.getString(PARTITION_FIELDS));
  }

  /**
   * This function returns the partition fields joined by BaseKeyGenerator.FIELD_SEPARATOR. It will also
   * include the key generator partition type with the field. The key generator partition type is used for
   * Custom Key Generator.
   */
  public static Option<List<String>> getPartitionFieldsForKeyGenerator(HoodieConfig config) {
    return Option.ofNullable(config.getString(PARTITION_FIELDS)).map(field -> Arrays.asList(field.split(BaseKeyGenerator.FIELD_SEPARATOR)));
  }

  /**
   * This function returns the partition fields joined by BaseKeyGenerator.FIELD_SEPARATOR. It will
   * strip the partition key generator related info from the fields.
   */
  public static Option<String> getPartitionFieldProp(HoodieConfig config) {
    // With table version eight, the table config org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS
    // stores the corresponding partition type as well. This partition type is useful for CustomKeyGenerator
    // and CustomAvroKeyGenerator.
    return getPartitionFields(config).map(fields -> String.join(BaseKeyGenerator.FIELD_SEPARATOR, fields));
  }

  /**
   * This function returns the partition fields only. This method strips the key generator related
   * partition key types from the configured fields.
   */
  public static Option<String[]> getPartitionFields(HoodieConfig config) {
    if (contains(PARTITION_FIELDS, config)) {
      return Option.of(Arrays.stream(config.getString(PARTITION_FIELDS).split(BaseKeyGenerator.FIELD_SEPARATOR))
          .filter(p -> !p.isEmpty())
          .map(p -> getPartitionFieldWithoutKeyGenPartitionType(p, config))
          .collect(Collectors.toList()).toArray(new String[] {}));
    }
    return Option.empty();
  }

  /**
   * This function returns the partition fields only. The input partition field would contain partition
   * type corresponding to the custom key generator if table version is eight and if custom key
   * generator is configured. This function would strip the partition type and return the partition field.
   */
  public static String getPartitionFieldWithoutKeyGenPartitionType(String partitionField, HoodieConfig config) {
    return partitionField.split(BaseKeyGenerator.CUSTOM_KEY_GENERATOR_SPLIT_REGEX)[0];
  }

  /**
   * This function returns the hoodie.table.version from hoodie.properties file.
   */
  public static HoodieTableVersion getTableVersion(HoodieConfig config) {
    return contains(VERSION, config)
        ? HoodieTableVersion.fromVersionCode(config.getInt(VERSION))
        : VERSION.defaultValue();
  }

  /**
   * Read the table type from the table properties and if not found, return the default.
   */
  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(getStringOrDefault(TYPE));
  }

  public Option<TimelineLayoutVersion> getTimelineLayoutVersion() {
    return contains(TIMELINE_LAYOUT_VERSION)
        ? Option.of(new TimelineLayoutVersion(getInt(TIMELINE_LAYOUT_VERSION)))
        : Option.empty();
  }

  public HoodieTableFormat getTableFormat(TimelineLayoutVersion layoutVersion) {
    String tableFormat = getStringOrDefault(TABLE_FORMAT);
    if (!tableFormat.equals(NativeTableFormat.TABLE_FORMAT)) {
      ServiceLoader<HoodieTableFormat> loader = ServiceLoader.load(HoodieTableFormat.class);
      for (HoodieTableFormat tableFormatImpl : loader) {
        if (getString(TABLE_FORMAT).equals(tableFormatImpl.getName())) {
          tableFormatImpl.init(props);
          return tableFormatImpl;
        }
      }
    }
    return new NativeTableFormat(layoutVersion);
  }

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return getTableVersion(this);
  }

  /**
   * @return the hoodie.table.initial.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableInitialVersion() {
    return contains(INITIAL_VERSION)
            ? HoodieTableVersion.fromVersionCode(getInt(INITIAL_VERSION))
            : INITIAL_VERSION.defaultValue();
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    setValue(VERSION, Integer.toString(tableVersion.versionCode()));
    setValue(TIMELINE_LAYOUT_VERSION, Integer.toString(tableVersion.getTimelineLayoutVersion().getVersion()));
  }

  public void setInitialVersion(HoodieTableVersion initialVersion) {
    setValue(INITIAL_VERSION, Integer.toString(initialVersion.versionCode()));
  }

  public RecordMergeMode getRecordMergeMode() {
    return RecordMergeMode.getValue(getString(RECORD_MERGE_MODE));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    return HoodieRecordPayload.getPayloadClassName(this);
  }

  public String getLegacyPayloadClass() {
    return getStringOrDefault(LEGACY_PAYLOAD_CLASS_NAME, "");
  }

  public String getRecordMergeStrategyId() {
    return getString(RECORD_MERGE_STRATEGY_ID);
  }

  /**
   * Handle table config creation logic when creating a table for Table Version 9,
   * which is based on the logic of table version < 9, and then tuned for version 9 logic.
   * This approach fits the same behavior of upgrade from 8 to 9.
   */
  static Map<String, String> inferMergingConfigsForV9TableCreation(RecordMergeMode recordMergeMode,
                                                                   String payloadClassName,
                                                                   String recordMergeStrategyId,
                                                                   String orderingFieldName,
                                                                   HoodieTableVersion tableVersion) {
    Map<String, String> reconciledConfigs = new HashMap<>();
    if (tableVersion.lesserThan(HoodieTableVersion.NINE)) {
      throw new HoodieIOException("Unsupported flow for table versions less than 9");
    }

    // Step 1: Infer merging configs based on input information.
    // This step is important since it provides the same configs before we do table upgrade.
    // Then additional logic for table version 9 could be verified.
    Triple<RecordMergeMode, String, String> inferredConfigs = inferMergingConfigsForPreV9Table(
        recordMergeMode, payloadClassName, recordMergeStrategyId, orderingFieldName, tableVersion);
    recordMergeMode = inferredConfigs.getLeft();
    recordMergeStrategyId = inferredConfigs.getRight();

    // Step 2: Handle Version 9 specific logic.
    // CASE 0: For tables with special merger properties, e.g., with non-builtin mergers.
    // CASE 1: For tables using MERGE MODE, or CUSTOM builtin mergers.
    //   NOTE: Payload class should NOT be set for these cases.
    if (!BUILTIN_MERGE_STRATEGIES.contains(recordMergeStrategyId)
        || StringUtils.isNullOrEmpty(payloadClassName)) {
      reconciledConfigs.put(RECORD_MERGE_MODE.key(), recordMergeMode.name());
      reconciledConfigs.put(RECORD_MERGE_STRATEGY_ID.key(), recordMergeStrategyId);
    } else {
      // For tables using payload classes.
      //   CASE 2: Custom payload class. We set these properties explicitly.
      if (!PAYLOADS_UNDER_DEPRECATION.contains(payloadClassName)) {
        reconciledConfigs.put(RECORD_MERGE_MODE.key(), CUSTOM.toString());
        reconciledConfigs.put(PAYLOAD_CLASS_NAME.key(), payloadClassName);
        reconciledConfigs.put(RECORD_MERGE_STRATEGY_ID.key(), PAYLOAD_BASED_MERGE_STRATEGY_UUID);
      } else { // CASE 3: Payload classes are under deprecation.
        // Standard merging configs.
        // NOTE: We use LEGACY_PAYLOAD_CLASS_NAME instead of PAYLOAD_CLASS_NAME here.
        if (EVENT_TIME_ORDERING_PAYLOADS.contains(payloadClassName)) {
          reconciledConfigs.put(RECORD_MERGE_MODE.key(), EVENT_TIME_ORDERING.name());
          reconciledConfigs.put(LEGACY_PAYLOAD_CLASS_NAME.key(), payloadClassName);
          reconciledConfigs.put(RECORD_MERGE_STRATEGY_ID.key(), EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
        } else {
          reconciledConfigs.put(RECORD_MERGE_MODE.key(), COMMIT_TIME_ORDERING.name());
          reconciledConfigs.put(LEGACY_PAYLOAD_CLASS_NAME.key(), payloadClassName);
          reconciledConfigs.put(RECORD_MERGE_STRATEGY_ID.key(), COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
        }
        // Partial update mode config.
        // Certain payloads are migrated to non payload way from 1.1 Hudi binary.
        // Hence we need to set the right value for partial update mode for some of the cases.
        if (payloadClassName.equals(PartialUpdateAvroPayload.class.getName())
            || payloadClassName.equals(OverwriteNonDefaultsWithLatestAvroPayload.class.getName())) {
          reconciledConfigs.put(PARTIAL_UPDATE_MODE.key(), PartialUpdateMode.IGNORE_DEFAULTS.name());
        } else if (payloadClassName.equals(PostgresDebeziumAvroPayload.class.getName())) {
          reconciledConfigs.put(PARTIAL_UPDATE_MODE.key(), PartialUpdateMode.IGNORE_MARKERS.name());
        }
        // Additional custom merge properties.
        // Cretain payloads are migrated to non payload way from 1.1 Hudi binary and the reader might need certain properties for the
        // merge to function as expected. Handing such special cases here.
        if (payloadClassName.equals(PostgresDebeziumAvroPayload.class.getName())) {
          reconciledConfigs.put(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_CUSTOM_MARKER, DEBEZIUM_UNAVAILABLE_VALUE);
        } else if (payloadClassName.equals(AWSDmsAvroPayload.class.getName())) {
          reconciledConfigs.put(RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY, OP_FIELD);
          reconciledConfigs.put(RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER, DELETE_OPERATION_VALUE);
        }
      }
    }
    return reconciledConfigs;
  }

  /**
   * To be invoked for table creation flows or writer flows.
   * @return the merging configs to use.
   */
  public static Triple<RecordMergeMode, String, String> inferMergingConfigsForWrites(RecordMergeMode recordMergeMode,
                                                                                     String payloadClassName,
                                                                                     String recordMergeStrategyId,
                                                                                     String orderingFieldNamesAsString,
                                                                                     HoodieTableVersion tableVersion) {
    return inferMergingConfigsForPreV9Table(recordMergeMode, payloadClassName, recordMergeStrategyId, orderingFieldNamesAsString, tableVersion);
  }

  /**
   * Infers the merging behavior based on what the user sets (or doesn't set).
   * Validates that the user has not set an illegal combination of configs.
   * This function infers basic merging properties used by table version <= 8.
   */
  public static Triple<RecordMergeMode, String, String> inferMergingConfigsForPreV9Table(RecordMergeMode recordMergeMode,
                                                                                         String payloadClassName,
                                                                                         String recordMergeStrategyId,
                                                                                         String orderingFieldNamesAsString,
                                                                                         HoodieTableVersion tableVersion) {
    RecordMergeMode inferredRecordMergeMode;
    String inferredPayloadClassName;
    String inferredRecordMergeStrategyId;

    // Inferring record merge mode
    if (isNullOrEmpty(payloadClassName) && isNullOrEmpty(recordMergeStrategyId)) {
      // If nothing is set on record merge mode, payload class, or record merge strategy ID,
      // use the default merge mode determined by whether the ordering field name is set.
      inferredRecordMergeMode = recordMergeMode != null
          ? recordMergeMode
          : (isNullOrEmpty(orderingFieldNamesAsString) ? COMMIT_TIME_ORDERING : EVENT_TIME_ORDERING);
    } else {
      // Infer the merge mode from either the payload class or record merge strategy ID
      RecordMergeMode modeBasedOnPayload = inferRecordMergeModeFromPayloadClass(payloadClassName);
      RecordMergeMode modeBasedOnStrategyId = inferRecordMergeModeFromMergeStrategyId(recordMergeStrategyId);
      checkArgument(modeBasedOnPayload != null || modeBasedOnStrategyId != null,
          String.format("Cannot infer record merge mode from payload class (%s) or record merge "
              + "strategy ID (%s).", payloadClassName, recordMergeStrategyId));
      // TODO(HUDI-8925): once payload class name is not required, remove the check on
      //  modeBasedOnStrategyId
      if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
          && modeBasedOnStrategyId != CUSTOM && modeBasedOnPayload != null && modeBasedOnStrategyId != null) {
        checkArgument(modeBasedOnPayload.equals(modeBasedOnStrategyId),
            String.format("Configured payload class (%s) and record merge strategy ID (%s) conflict "
                    + "with each other. Please only set one of them in the write config.",
                payloadClassName, recordMergeStrategyId));
      }
      if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
        inferredRecordMergeMode = modeBasedOnStrategyId != null ? modeBasedOnStrategyId : modeBasedOnPayload;
      } else {
        inferredRecordMergeMode = modeBasedOnPayload != null ? modeBasedOnPayload : modeBasedOnStrategyId;
      }
    }
    if (recordMergeMode != null) {
      checkArgument(inferredRecordMergeMode == recordMergeMode,
          String.format("Configured record merge mode (%s) is inconsistent with payload class (%s) "
                  + "or record merge strategy ID (%s) configured. Please revisit the configs.",
              recordMergeMode, payloadClassName, recordMergeStrategyId));
    }

    // Check ordering field name based on record merge mode
    if (inferredRecordMergeMode == COMMIT_TIME_ORDERING) {
      if (nonEmpty(orderingFieldNamesAsString)) {
        LOG.warn("The precombine or ordering field ({}) is specified. COMMIT_TIME_ORDERING "
            + "merge mode does not use precombine or ordering field anymore.", orderingFieldNamesAsString);
      }
    } else if (inferredRecordMergeMode == EVENT_TIME_ORDERING) {
      if (isNullOrEmpty(orderingFieldNamesAsString)) {
        LOG.warn("The precombine or ordering field is not specified. EVENT_TIME_ORDERING "
            + "merge mode requires precombine or ordering field to be set for getting the "
            + "event time. Using commit time-based ordering now.");
      }
    }

    // Inferring payload class name
    inferredPayloadClassName = HoodieRecordPayload.getAvroPayloadForMergeMode(
        inferredRecordMergeMode, payloadClassName);
    // Inferring record merge strategy ID
    inferredRecordMergeStrategyId = HoodieRecordMerger.getRecordMergeStrategyId(
        inferredRecordMergeMode, inferredPayloadClassName, recordMergeStrategyId, tableVersion);

    // For custom merge mode, either payload class name or record merge strategy ID must be configured
    if (inferredRecordMergeMode == CUSTOM) {
      checkArgument(nonEmpty(inferredPayloadClassName) || nonEmpty(inferredRecordMergeStrategyId),
          "Either payload class name or record merge strategy ID must be configured "
              + "in CUSTOM merge mode.");
      if (PAYLOAD_BASED_MERGE_STRATEGY_UUID.equals(inferredRecordMergeStrategyId)) {
        checkArgument(nonEmpty(inferredPayloadClassName),
            "For payload class based merge strategy as a fallback, payload class name is "
                + "required to be set.");
      }
      // TODO(HUDI-8925): remove this once the payload class name is no longer required
      if (isNullOrEmpty(inferredPayloadClassName)) {
        inferredPayloadClassName = DEFAULT_PAYLOAD_CLASS_NAME;
      }
    }

    return Triple.of(inferredRecordMergeMode, inferredPayloadClassName, inferredRecordMergeStrategyId);
  }

  public static RecordMergeMode inferRecordMergeModeFromPayloadClass(String payloadClassName) {
    if (isNullOrEmpty(payloadClassName)) {
      return null;
    }

    if (DefaultHoodieRecordPayload.class.getName().equals(payloadClassName)
        || EventTimeAvroPayload.class.getName().equals(payloadClassName)) {
      // DefaultHoodieRecordPayload and EventTimeAvroPayload match with EVENT_TIME_ORDERING.
      return EVENT_TIME_ORDERING;
    } else if (payloadClassName.equals(OverwriteWithLatestAvroPayload.class.getName())) {
      // OverwriteWithLatestAvroPayload matches with COMMIT_TIME_ORDERING.
      return COMMIT_TIME_ORDERING;
    } else {
      return CUSTOM;
    }
  }

  static RecordMergeMode inferRecordMergeModeFromMergeStrategyId(String recordMergeStrategyId) {
    if (isNullOrEmpty(recordMergeStrategyId)) {
      return null;
    }
    if (recordMergeStrategyId.equals(EVENT_TIME_BASED_MERGE_STRATEGY_UUID)) {
      return EVENT_TIME_ORDERING;
    } else if (recordMergeStrategyId.equals(COMMIT_TIME_BASED_MERGE_STRATEGY_UUID)) {
      return COMMIT_TIME_ORDERING;
    } else {
      return CUSTOM;
    }
  }

  public List<String> getPreCombineFields() {
    return getPreCombineFieldsStr()
        .map(preCombine -> Arrays.stream(preCombine.split(",")).filter(StringUtils::nonEmpty).collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  public Option<String> getPreCombineFieldsStr() {
    return Option.ofNullable(getString(PRECOMBINE_FIELDS));
  }

  public Option<String[]> getRecordKeyFields() {
    String keyFieldsValue = getStringOrDefault(RECORDKEY_FIELDS, null);
    if (keyFieldsValue == null) {
      return Option.empty();
    } else {
      return Option.of(Arrays.stream(keyFieldsValue.split(","))
          .filter(p -> p.length() > 0).collect(Collectors.toList()).toArray(new String[] {}));
    }
  }

  public Option<String[]> getPartitionFields() {
    return getPartitionFields(this);
  }

  public boolean isTablePartitioned() {
    return getPartitionFields().map(pfs -> pfs.length > 0).orElse(false);
  }

  public Option<String> getSecondaryIndexesMetadata() {
    if (contains(SECONDARY_INDEXES_METADATA)) {
      return Option.of(getString(SECONDARY_INDEXES_METADATA));
    }

    return Option.empty();
  }

  /**
   * @returns the partition field prop.
   * @deprecated please use {@link #getPartitionFields()} instead
   */
  @Deprecated
  public String getPartitionFieldProp() {
    // NOTE: We're adding a stub returning empty string to stay compatible w/ pre-existing
    //       behavior until this method is fully deprecated
    return getPartitionFieldProp(this).orElse("");
  }

  public Option<String> getBootstrapBasePath() {
    return Option.ofNullable(getString(BOOTSTRAP_BASE_PATH));
  }

  public Option<Schema> getTableCreateSchema() {
    if (contains(CREATE_SCHEMA)) {
      return Option.of(new Schema.Parser().parse(getString(CREATE_SCHEMA)));
    } else {
      return Option.empty();
    }
  }

  /**
   * Read the database name.
   */
  public String getDatabaseName() {
    return getString(DATABASE_NAME);
  }

  /**
   * Read the table name.
   */
  public String getTableName() {
    return getString(NAME);
  }

  /**
   * Get the base file storage format.
   *
   * @return HoodieFileFormat for the base file Storage format
   */
  public HoodieFileFormat getBaseFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(BASE_FILE_FORMAT));
  }

  /**
   * Get the log Storage Format.
   *
   * @return HoodieFileFormat for the log Storage format
   */
  public HoodieFileFormat getLogFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(LOG_FILE_FORMAT));
  }

  /**
   * Get the relative path of legacy archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return getStringOrDefault(ARCHIVELOG_FOLDER);
  }

  /**
   * Get the relative path of timeline history folder under metafolder, for this table.
   */
  public String getTimelineHistoryPath() {
    return getStringOrDefault(TIMELINE_HISTORY_PATH);
  }

  /**
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getTimelinePath() {
    return getStringOrDefault(TIMELINE_PATH);
  }

  /**
   * @returns true is meta fields need to be populated. else returns false.
   */
  public boolean populateMetaFields() {
    return Boolean.parseBoolean(getStringOrDefault(POPULATE_META_FIELDS));
  }

  /**
   * @returns the record key field prop.
   */
  public String getRecordKeyFieldProp() {
    return getStringOrDefault(RECORDKEY_FIELDS, HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  /**
   * @returns the record key field prop.
   */
  public String getRawRecordKeyFieldProp() {
    return getStringOrDefault(RECORDKEY_FIELDS, null);
  }

  public boolean isCDCEnabled() {
    return getBooleanOrDefault(CDC_ENABLED);
  }

  public HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode() {
    return HoodieCDCSupplementalLoggingMode.valueOf(getStringOrDefault(CDC_SUPPLEMENTAL_LOGGING_MODE).toUpperCase());
  }

  public String getKeyGeneratorClassName() {
    return KeyGeneratorType.getKeyGeneratorClassName(this);
  }

  public HoodieTimelineTimeZone getTimelineTimezone() {
    return HoodieTimelineTimeZone.valueOf(getStringOrDefault(TIMELINE_TIMEZONE));
  }

  public String getHiveStylePartitioningEnable() {
    return getStringOrDefault(HIVE_STYLE_PARTITIONING_ENABLE);
  }

  public String getUrlEncodePartitioning() {
    return getStringOrDefault(URL_ENCODE_PARTITIONING);
  }

  public Boolean shouldDropPartitionColumns() {
    return getBooleanOrDefault(DROP_PARTITION_COLUMNS);
  }

  public boolean isMultipleBaseFileFormatsEnabled() {
    return getBooleanOrDefault(MULTIPLE_BASE_FILE_FORMATS_ENABLE);
  }

  public Set<String> getMetadataPartitionsInflight() {
    return new HashSet<>(StringUtils.split(
        getStringOrDefault(TABLE_METADATA_PARTITIONS_INFLIGHT, StringUtils.EMPTY_STRING),
        CONFIG_VALUES_DELIMITER));
  }

  public Set<String> getMetadataPartitions() {
    return new HashSet<>(
        StringUtils.split(getStringOrDefault(TABLE_METADATA_PARTITIONS, StringUtils.EMPTY_STRING),
            CONFIG_VALUES_DELIMITER));
  }

  public PartialUpdateMode getPartialUpdateMode() {
    if (getTableVersion().greaterThanOrEquals(HoodieTableVersion.NINE)) {
      return PartialUpdateMode.valueOf(getStringOrDefault(PARTIAL_UPDATE_MODE));
    } else {
      // For table version <= 8, partial update is not supported.
      return PartialUpdateMode.NONE;
    }
  }

  /**
   * @returns the index definition path.
   */
  public Option<String> getRelativeIndexDefinitionPath() {
    return Option.ofNullable(getString(RELATIVE_INDEX_DEFINITION_PATH));
  }

  /**
   * @returns true if metadata table has been created and is being used for this dataset, else returns false.
   */
  public boolean isMetadataTableAvailable() {
    return isMetadataPartitionAvailable(MetadataPartitionType.FILES);
  }

  /**
   * Checks if metadata table is enabled and the specified partition has been initialized.
   *
   * @param metadataPartitionType The metadata table partition type to check
   * @returns true if the specific partition has been initialized, else returns false.
   */
  public boolean isMetadataPartitionAvailable(MetadataPartitionType metadataPartitionType) {
    return getMetadataPartitions().contains(metadataPartitionType.getPartitionPath());
  }

  /**
   * Enables or disables the specified metadata table partition.
   *
   * @param partitionPath The partition
   * @param enabled       If true, the partition is enabled, else disabled
   */
  public void setMetadataPartitionState(HoodieTableMetaClient metaClient, String partitionPath, boolean enabled) {
    ValidationUtils.checkArgument(!partitionPath.contains(CONFIG_VALUES_DELIMITER),
        "Metadata Table partition path cannot contain a comma: " + partitionPath);
    Set<String> partitions = getMetadataPartitions();
    Set<String> partitionsInflight = getMetadataPartitionsInflight();
    if (enabled) {
      partitions.add(partitionPath);
      partitionsInflight.remove(partitionPath);
    } else if (partitionPath.equals(MetadataPartitionType.FILES.getPartitionPath())) {
      // file listing partition is required for all other partitions to work
      // Disabling file partition will also disable all partitions
      partitions.clear();
      partitionsInflight.clear();
    } else {
      partitions.remove(partitionPath);
      partitionsInflight.remove(partitionPath);
    }
    setValue(TABLE_METADATA_PARTITIONS, partitions.stream().sorted().collect(Collectors.joining(CONFIG_VALUES_DELIMITER)));
    setValue(TABLE_METADATA_PARTITIONS_INFLIGHT, partitionsInflight.stream().sorted().collect(Collectors.joining(CONFIG_VALUES_DELIMITER)));
    update(metaClient.getStorage(), metaClient.getMetaPath(), getProps());
    LOG.info("MDT {} partition {} has been {}", metaClient.getBasePath(), partitionPath, enabled ? "enabled" : "disabled");
  }

  /**
   * Enables the specified metadata table partition as inflight.
   *
   * @param partitionPaths The list of partitions to enable as inflight.
   */
  public void setMetadataPartitionsInflight(HoodieTableMetaClient metaClient, List<String> partitionPaths) {
    Set<String> partitionsInflight = getMetadataPartitionsInflight();
    partitionPaths.forEach(partitionPath -> {
      ValidationUtils.checkArgument(!partitionPath.contains(CONFIG_VALUES_DELIMITER),
          "Metadata Table partition path cannot contain a comma: " + partitionPath);
      partitionsInflight.add(partitionPath);
    });

    setValue(TABLE_METADATA_PARTITIONS_INFLIGHT, partitionsInflight.stream().sorted().collect(Collectors.joining(CONFIG_VALUES_DELIMITER)));
    update(metaClient.getStorage(), metaClient.getMetaPath(), getProps());
    LOG.info("MDT {} partitions {} have been set to inflight", metaClient.getBasePath(), partitionPaths);
  }

  public void setMetadataPartitionsInflight(HoodieTableMetaClient metaClient, MetadataPartitionType... partitionTypes) {
    setMetadataPartitionsInflight(metaClient, Arrays.stream(partitionTypes).map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList()));
  }

  /**
   * Clear {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS}
   * {@link HoodieTableConfig#TABLE_METADATA_PARTITIONS_INFLIGHT}.
   */
  public void clearMetadataPartitions(HoodieTableMetaClient metaClient) {
    setMetadataPartitionState(metaClient, MetadataPartitionType.FILES.getPartitionPath(), false);
  }

  /**
   * Returns the format to use for partition meta files.
   */
  public Option<HoodieFileFormat> getPartitionMetafileFormat() {
    if (getBooleanOrDefault(PARTITION_METAFILE_USE_BASE_FORMAT)) {
      return Option.of(getBaseFileFormat());
    }
    return Option.empty();
  }

  public Map<String, String> getTableMergeProperties() {
    return ConfigUtils.extractWithPrefix(this.props, RECORD_MERGE_PROPERTY_PREFIX);
  }

  public Map<String, String> propsMap() {
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  /**
   * @deprecated Use {@link #BASE_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final String HOODIE_RO_FILE_FORMAT_PROP_NAME = "hoodie.table.ro.file.format";
  /**
   * @deprecated Use {@link #LOG_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final String HOODIE_RT_FILE_FORMAT_PROP_NAME = "hoodie.table.rt.file.format";
  /**
   * @deprecated Use {@link #NAME} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TABLE_NAME_PROP_NAME = NAME.key();
  /**
   * @deprecated Use {@link #TYPE} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TABLE_TYPE_PROP_NAME = TYPE.key();
  /**
   * @deprecated Use {@link #VERSION} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TABLE_VERSION_PROP_NAME = VERSION.key();
  /**
   * @deprecated Use {@link #PRECOMBINE_FIELDS} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TABLE_PRECOMBINE_FIELD = PRECOMBINE_FIELDS.key();
  /**
   * @deprecated Use {@link #BASE_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final String HOODIE_BASE_FILE_FORMAT_PROP_NAME = BASE_FILE_FORMAT.key();
  /**
   * @deprecated Use {@link #LOG_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final String HOODIE_LOG_FILE_FORMAT_PROP_NAME = LOG_FILE_FORMAT.key();
  /**
   * @deprecated Use {@link #TIMELINE_LAYOUT_VERSION} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TIMELINE_LAYOUT_VERSION = TIMELINE_LAYOUT_VERSION.key();
  /**
   * @deprecated Use {@link #PAYLOAD_CLASS_NAME} and its methods.
   */
  @Deprecated
  public static final String HOODIE_PAYLOAD_CLASS_PROP_NAME = PAYLOAD_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #TIMELINE_HISTORY_PATH} and its methods.
   */
  @Deprecated
  public static final String HOODIE_ARCHIVELOG_FOLDER_PROP_NAME = TIMELINE_HISTORY_PATH.key();
  /**
   * @deprecated Use {@link #BOOTSTRAP_INDEX_CLASS_NAME} and its methods.
   */
  @Deprecated
  public static final String HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME = BOOTSTRAP_INDEX_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #BOOTSTRAP_BASE_PATH} and its methods.
   */
  @Deprecated
  public static final String HOODIE_BOOTSTRAP_BASE_PATH = BOOTSTRAP_BASE_PATH.key();
  /**
   * @deprecated Use {@link #TYPE} and its methods.
   */
  @Deprecated
  public static final HoodieTableType DEFAULT_TABLE_TYPE = TYPE.defaultValue();
  /**
   * @deprecated Use {@link #VERSION} and its methods.
   */
  @Deprecated
  public static final HoodieTableVersion DEFAULT_TABLE_VERSION = VERSION.defaultValue();
  /**
   * @deprecated Use {@link #BASE_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final HoodieFileFormat DEFAULT_BASE_FILE_FORMAT = BASE_FILE_FORMAT.defaultValue();
  /**
   * @deprecated Use {@link #LOG_FILE_FORMAT} and its methods.
   */
  @Deprecated
  public static final HoodieFileFormat DEFAULT_LOG_FILE_FORMAT = LOG_FILE_FORMAT.defaultValue();
  /**
   * @deprecated Use {@link #BOOTSTRAP_INDEX_CLASS_NAME} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_INDEX_CLASS = BOOTSTRAP_INDEX_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #TIMELINE_HISTORY_PATH} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_ARCHIVELOG_FOLDER = TIMELINE_HISTORY_PATH.defaultValue();
}
