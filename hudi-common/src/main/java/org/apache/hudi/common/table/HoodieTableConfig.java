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

import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during
 * initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.
 *
 * @see HoodieTableMetaClient
 * @since 0.3.0
 */
@ConfigClassProperty(name = "Table Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that persist across writes and read on a Hudi table "
        + " like  base, log file formats, table name, creation schema, table version layouts. "
        + " Configurations are loaded from hoodie.properties, these properties are usually set during "
        + "initializing a path as hoodie base path and rarely changes during "
        + "the lifetime of the table. Writers/Queries' configurations are validated against these "
        + " each time for compatibility.")
public class HoodieTableConfig extends HoodieConfig {

  private static final Logger LOG = LogManager.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_PROPERTIES_FILE_BACKUP = "hoodie.properties.backup";

  public static final ConfigProperty<String> NAME = ConfigProperty
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDocumentation("Table name that will be used for registering with Hive. Needs to be same across runs.");

  public static final ConfigProperty<HoodieTableType> TYPE = ConfigProperty
      .key("hoodie.table.type")
      .defaultValue(HoodieTableType.COPY_ON_WRITE)
      .withDocumentation("The table type for the underlying data, for this write. This can’t change between writes.");

  public static final ConfigProperty<HoodieTableVersion> VERSION = ConfigProperty
      .key("hoodie.table.version")
      .defaultValue(HoodieTableVersion.ZERO)
      .withDocumentation("Version of table, used for running upgrade/downgrade steps between releases with potentially "
          + "breaking/backwards compatible changes.");

  public static final ConfigProperty<String> PRECOMBINE_FIELD = ConfigProperty
      .key("hoodie.table.precombine.field")
      .noDefaultValue()
      .withDocumentation("Field used in preCombining before actual write. By default, when two records have the same key value, "
          + "the largest value for the precombine field determined by Object.compareTo(..), is picked.");

  public static final ConfigProperty<String> PARTITION_FIELDS = ConfigProperty
      .key("hoodie.table.partition.fields")
      .noDefaultValue()
      .withDocumentation("Fields used to partition the table. Concatenated values of these fields are used as "
          + "the partition path, by invoking toString()");

  public static final ConfigProperty<String> RECORDKEY_FIELDS = ConfigProperty
      .key("hoodie.table.recordkey.fields")
      .noDefaultValue()
      .withDocumentation("Columns used to uniquely identify the table. Concatenated values of these fields are used as "
          + " the record key component of HoodieKey.");

  public static final ConfigProperty<String> CREATE_SCHEMA = ConfigProperty
      .key("hoodie.table.create.schema")
      .noDefaultValue()
      .withDocumentation("Schema used when creating the table, for the first time.");

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

  public static final ConfigProperty<String> PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDocumentation("Payload class to use for performing compactions, i.e merge delta logs with current base file and then "
          + " produce a new base file.");

  public static final ConfigProperty<String> ARCHIVELOG_FOLDER = ConfigProperty
      .key("hoodie.archivelog.folder")
      .defaultValue("archived")
      .withDocumentation("path under the meta folder, to store archived timeline instants at.");

  public static final ConfigProperty<Boolean> BOOTSTRAP_INDEX_ENABLE = ConfigProperty
      .key("hoodie.bootstrap.index.enable")
      .defaultValue(true)
      .withDocumentation("Whether or not, this is a bootstrapped table, with bootstrap base data and an mapping index defined, default true.");

  public static final ConfigProperty<String> BOOTSTRAP_INDEX_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .withDocumentation("Implementation to use, for mapping base files to bootstrap base file, that contain actual data.");

  public static final ConfigProperty<String> BOOTSTRAP_BASE_PATH = ConfigProperty
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .withDocumentation("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final ConfigProperty<String> POPULATE_META_FIELDS = ConfigProperty
      .key("hoodie.populate.meta.fields")
      .defaultValue("true")
      .withDocumentation("When enabled, populates all meta fields. When disabled, no meta fields are populated "
          + "and incremental queries will not be functional. This is only meant to be used for append only/immutable data for batch processing");

  public static final ConfigProperty<String> KEY_GENERATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.table.keygenerator.class")
      .noDefaultValue()
      .withDocumentation("Key Generator class property for the hoodie table");

  public static final ConfigProperty<HoodieTimelineTimeZone> TIMELINE_TIMEZONE = ConfigProperty
      .key("hoodie.table.timeline.timezone")
      .defaultValue(HoodieTimelineTimeZone.LOCAL)
      .withDocumentation("User can set hoodie commit timeline timezone, such as utc, local and so on. local is default");

  public static final ConfigProperty<String> URL_ENCODE_PARTITIONING = KeyGeneratorOptions.URL_ENCODE_PARTITIONING;
  public static final ConfigProperty<String> HIVE_STYLE_PARTITIONING_ENABLE = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE;

  public static final String NO_OP_BOOTSTRAP_INDEX_CLASS = NoOpBootstrapIndex.class.getName();

  public HoodieTableConfig(FileSystem fs, String metaPath, String payloadClassName) {
    super();
    Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      fetchConfigs(fs, metaPath);
      if (contains(PAYLOAD_CLASS_NAME) && payloadClassName != null
          && !getString(PAYLOAD_CLASS_NAME).equals(payloadClassName)) {
        setValue(PAYLOAD_CLASS_NAME, payloadClassName);
        // FIXME(vc): wonder if this can be removed. Need to look into history.
        try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
          props.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not load Hoodie properties from " + propertyPath, e);
    }
    ValidationUtils.checkArgument(contains(TYPE) && contains(NAME),
        "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
  }

  /**
   * For serializing and de-serializing.
   */
  public HoodieTableConfig() {
    super();
  }

  private void fetchConfigs(FileSystem fs, String metaPath) throws IOException {
    Path cfgPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    try (FSDataInputStream is = fs.open(cfgPath)) {
      props.load(is);
    } catch (IOException ioe) {
      if (!fs.exists(cfgPath)) {
        LOG.warn("Run `table recover-configs` if config update/delete failed midway. Falling back to backed up configs.");
        // try the backup. this way no query ever fails if update fails midway.
        Path backupCfgPath = new Path(metaPath, HOODIE_PROPERTIES_FILE_BACKUP);
        try (FSDataInputStream is = fs.open(backupCfgPath)) {
          props.load(is);
        }
      } else {
        throw ioe;
      }
    }
  }

  public static void recover(FileSystem fs, Path metadataFolder) throws IOException {
    Path cfgPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    Path backupCfgPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE_BACKUP);
    recoverIfNeeded(fs, cfgPath, backupCfgPath);
  }

  static void recoverIfNeeded(FileSystem fs, Path cfgPath, Path backupCfgPath) throws IOException {
    if (!fs.exists(cfgPath)) {
      // copy over from backup
      try (FSDataInputStream in = fs.open(backupCfgPath);
           FSDataOutputStream out = fs.create(cfgPath, false)) {
        FileIOUtils.copy(in, out);
      }
    }
    // regardless, we don't need the backup anymore.
    fs.delete(backupCfgPath, false);
  }

  private static void upsertProperties(Properties current, Properties updated) {
    updated.forEach((k, v) -> current.setProperty(k.toString(), v.toString()));
  }

  private static void deleteProperties(Properties current, Properties deleted) {
    deleted.forEach((k, v) -> current.remove(k.toString()));
  }

  private static void modify(FileSystem fs, Path metadataFolder, Properties modifyProps, BiConsumer<Properties, Properties> modifyFn) {
    Path cfgPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    Path backupCfgPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE_BACKUP);
    try {
      // 0. do any recovery from prior attempts.
      recoverIfNeeded(fs, cfgPath, backupCfgPath);

      // 1. backup the existing properties.
      try (FSDataInputStream in = fs.open(cfgPath);
           FSDataOutputStream out = fs.create(backupCfgPath, false)) {
        FileIOUtils.copy(in, out);
      }
      /// 2. delete the properties file, reads will go to the backup, until we are done.
      fs.delete(cfgPath, false);
      // 3. read current props, upsert and save back.
      try (FSDataInputStream in = fs.open(backupCfgPath);
           FSDataOutputStream out = fs.create(cfgPath, true)) {
        Properties props = new Properties();
        props.load(in);
        modifyFn.accept(props, modifyProps);
        props.store(out, "Updated at " + System.currentTimeMillis());
      }
      // 4. verify and remove backup.
      // FIXME(vc): generate a hash for verification.
      fs.delete(backupCfgPath, false);
    } catch (IOException e) {
      throw new HoodieIOException("Error updating table configs.", e);
    }
  }

  /**
   * Upserts the table config with the set of properties passed in. We implement a fail-safe backup protocol
   * here for safely updating with recovery and also ensuring the table config continues to be readable.
   */
  public static void update(FileSystem fs, Path metadataFolder, Properties updatedProps) {
    modify(fs, metadataFolder, updatedProps, HoodieTableConfig::upsertProperties);
  }

  public static void delete(FileSystem fs, Path metadataFolder, Set<String> deletedProps) {
    Properties props = new Properties();
    deletedProps.forEach(p -> props.setProperty(p, ""));
    modify(fs, metadataFolder, props, HoodieTableConfig::deleteProperties);
  }

  /**
   * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties).
   */
  public static void create(FileSystem fs, Path metadataFolder, Properties properties)
      throws IOException {
    if (!fs.exists(metadataFolder)) {
      fs.mkdirs(metadataFolder);
    }
    HoodieConfig hoodieConfig = new HoodieConfig(properties);
    Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
      if (!hoodieConfig.contains(NAME)) {
        throw new IllegalArgumentException(NAME.key() + " property needs to be specified");
      }
      hoodieConfig.setDefaultValue(TYPE);
      if (hoodieConfig.getString(TYPE).equals(HoodieTableType.MERGE_ON_READ.name())) {
        hoodieConfig.setDefaultValue(PAYLOAD_CLASS_NAME);
      }
      hoodieConfig.setDefaultValue(ARCHIVELOG_FOLDER);
      if (!hoodieConfig.contains(TIMELINE_LAYOUT_VERSION)) {
        // Use latest Version as default unless forced by client
        hoodieConfig.setValue(TIMELINE_LAYOUT_VERSION, TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (hoodieConfig.contains(BOOTSTRAP_BASE_PATH)) {
        // Use the default bootstrap index class.
        hoodieConfig.setDefaultValue(BOOTSTRAP_INDEX_CLASS_NAME, getDefaultBootstrapIndexClass(properties));
      }
      if (hoodieConfig.contains(TIMELINE_TIMEZONE)) {
        HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.valueOf(hoodieConfig.getString(TIMELINE_TIMEZONE)));
      }
      hoodieConfig.getProps().store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
    }
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

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return contains(VERSION)
        ? HoodieTableVersion.versionFromCode(getInt(VERSION))
        : VERSION.defaultValue();
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    setValue(VERSION, Integer.toString(tableVersion.versionCode()));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(PAYLOAD_CLASS_NAME).replace("com.uber.hoodie",
        "org.apache.hudi");
  }

  public String getPreCombineField() {
    return getString(PRECOMBINE_FIELD);
  }

  public Option<String[]> getRecordKeyFields() {
    if (contains(RECORDKEY_FIELDS)) {
      return Option.of(Arrays.stream(getString(RECORDKEY_FIELDS).split(","))
          .filter(p -> p.length() > 0).collect(Collectors.toList()).toArray(new String[] {}));
    }
    return Option.empty();
  }

  public Option<String[]> getPartitionFields() {
    if (contains(PARTITION_FIELDS)) {
      return Option.of(Arrays.stream(getString(PARTITION_FIELDS).split(","))
          .filter(p -> p.length() > 0).collect(Collectors.toList()).toArray(new String[] {}));
    }
    return Option.empty();
  }

  /**
   * @returns the partition field prop.
   */
  public String getPartitionFieldProp() {
    return getString(PARTITION_FIELDS);
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getBootstrapIndexClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(BOOTSTRAP_INDEX_CLASS_NAME, getDefaultBootstrapIndexClass(props));
  }

  public static String getDefaultBootstrapIndexClass(Properties props) {
    HoodieConfig hoodieConfig = new HoodieConfig(props);
    String defaultClass = BOOTSTRAP_INDEX_CLASS_NAME.defaultValue();
    if (!hoodieConfig.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)) {
      defaultClass = NO_OP_BOOTSTRAP_INDEX_CLASS;
    }
    return defaultClass;
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
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return getStringOrDefault(ARCHIVELOG_FOLDER);
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

  public String getKeyGeneratorClassName() {
    return getString(KEY_GENERATOR_CLASS_NAME);
  }

  public String getHiveStylePartitioningEnable() {
    return getString(HIVE_STYLE_PARTITIONING_ENABLE);
  }

  public String getUrlEncodePartitioning() {
    return getString(URL_ENCODE_PARTITIONING);
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
   * @deprecated Use {@link #PRECOMBINE_FIELD} and its methods.
   */
  @Deprecated
  public static final String HOODIE_TABLE_PRECOMBINE_FIELD = PRECOMBINE_FIELD.key();
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
   * @deprecated Use {@link #ARCHIVELOG_FOLDER} and its methods.
   */
  @Deprecated
  public static final String HOODIE_ARCHIVELOG_FOLDER_PROP_NAME = ARCHIVELOG_FOLDER.key();
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
   * @deprecated Use {@link #PAYLOAD_CLASS_NAME} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_PAYLOAD_CLASS = PAYLOAD_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #BOOTSTRAP_INDEX_CLASS_NAME} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_INDEX_CLASS = BOOTSTRAP_INDEX_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #ARCHIVELOG_FOLDER} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_ARCHIVELOG_FOLDER = ARCHIVELOG_FOLDER.defaultValue();
}
