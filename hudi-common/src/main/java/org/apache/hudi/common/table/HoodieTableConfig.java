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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during
 * initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.
 *
 * @see HoodieTableMetaClient
 * @since 0.3.0
 */
public class HoodieTableConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
  public static final String HOODIE_TABLE_NAME_PROP_NAME = "hoodie.table.name";
  public static final String HOODIE_TABLE_TYPE_PROP_NAME = "hoodie.table.type";
  public static final String HOODIE_TABLE_VERSION_PROP_NAME = "hoodie.table.version";
  public static final String HOODIE_TABLE_PRECOMBINE_FIELD = "hoodie.table.precombine.field";

  @Deprecated
  public static final String HOODIE_RO_FILE_FORMAT_PROP_NAME = "hoodie.table.ro.file.format";
  @Deprecated
  public static final String HOODIE_RT_FILE_FORMAT_PROP_NAME = "hoodie.table.rt.file.format";
  public static final String HOODIE_BASE_FILE_FORMAT_PROP_NAME = "hoodie.table.base.file.format";
  public static final String HOODIE_LOG_FILE_FORMAT_PROP_NAME = "hoodie.table.log.file.format";
  public static final String HOODIE_TIMELINE_LAYOUT_VERSION = "hoodie.timeline.layout.version";
  public static final String HOODIE_PAYLOAD_CLASS_PROP_NAME = "hoodie.compaction.payload.class";
  public static final String HOODIE_ARCHIVELOG_FOLDER_PROP_NAME = "hoodie.archivelog.folder";
  public static final String HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME = "hoodie.bootstrap.index.class";
  public static final String HOODIE_BOOTSTRAP_BASE_PATH = "hoodie.bootstrap.base.path";

  public static final HoodieTableType DEFAULT_TABLE_TYPE = HoodieTableType.COPY_ON_WRITE;
  public static final HoodieTableVersion DEFAULT_TABLE_VERSION = HoodieTableVersion.ZERO;
  public static final HoodieFileFormat DEFAULT_BASE_FILE_FORMAT = HoodieFileFormat.PARQUET;
  public static final HoodieFileFormat DEFAULT_LOG_FILE_FORMAT = HoodieFileFormat.HOODIE_LOG;
  public static final String DEFAULT_PAYLOAD_CLASS = OverwriteWithLatestAvroPayload.class.getName();
  public static final String DEFAULT_BOOTSTRAP_INDEX_CLASS = HFileBootstrapIndex.class.getName();
  public static final String DEFAULT_ARCHIVELOG_FOLDER = "";

  private Properties props;

  public HoodieTableConfig(FileSystem fs, String metaPath, String payloadClassName) {
    Properties props = new Properties();
    Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      try (FSDataInputStream inputStream = fs.open(propertyPath)) {
        props.load(inputStream);
      }
      if (props.containsKey(HOODIE_PAYLOAD_CLASS_PROP_NAME) && payloadClassName != null
          && !props.getProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME).equals(payloadClassName)) {
        props.setProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME, payloadClassName);
        try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
          props.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not load Hoodie properties from " + propertyPath, e);
    }
    this.props = props;
    ValidationUtils.checkArgument(props.containsKey(HOODIE_TABLE_TYPE_PROP_NAME) && props.containsKey(HOODIE_TABLE_NAME_PROP_NAME),
        "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
  }

  public HoodieTableConfig(Properties props) {
    this.props = props;
  }

  /**
   * For serailizing and de-serializing.
   *
   * @deprecated
   */
  public HoodieTableConfig() {
  }

  /**
   * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties).
   */
  public static void createHoodieProperties(FileSystem fs, Path metadataFolder, Properties properties)
      throws IOException {
    if (!fs.exists(metadataFolder)) {
      fs.mkdirs(metadataFolder);
    }
    Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
      if (!properties.containsKey(HOODIE_TABLE_NAME_PROP_NAME)) {
        throw new IllegalArgumentException(HOODIE_TABLE_NAME_PROP_NAME + " property needs to be specified");
      }
      if (!properties.containsKey(HOODIE_TABLE_TYPE_PROP_NAME)) {
        properties.setProperty(HOODIE_TABLE_TYPE_PROP_NAME, DEFAULT_TABLE_TYPE.name());
      }
      if (properties.getProperty(HOODIE_TABLE_TYPE_PROP_NAME).equals(HoodieTableType.MERGE_ON_READ.name())
          && !properties.containsKey(HOODIE_PAYLOAD_CLASS_PROP_NAME)) {
        properties.setProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME, DEFAULT_PAYLOAD_CLASS);
      }
      if (!properties.containsKey(HOODIE_ARCHIVELOG_FOLDER_PROP_NAME)) {
        properties.setProperty(HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, DEFAULT_ARCHIVELOG_FOLDER);
      }
      if (!properties.containsKey(HOODIE_TIMELINE_LAYOUT_VERSION)) {
        // Use latest Version as default unless forced by client
        properties.setProperty(HOODIE_TIMELINE_LAYOUT_VERSION, TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (properties.containsKey(HOODIE_BOOTSTRAP_BASE_PATH) && !properties.containsKey(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME)) {
        // Use the default bootstrap index class.
        properties.setProperty(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME, DEFAULT_BOOTSTRAP_INDEX_CLASS);
      }
      properties.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
    }
  }

  /**
   * Read the table type from the table properties and if not found, return the default.
   */
  public HoodieTableType getTableType() {
    if (props.containsKey(HOODIE_TABLE_TYPE_PROP_NAME)) {
      return HoodieTableType.valueOf(props.getProperty(HOODIE_TABLE_TYPE_PROP_NAME));
    }
    return DEFAULT_TABLE_TYPE;
  }

  public Option<TimelineLayoutVersion> getTimelineLayoutVersion() {
    return props.containsKey(HOODIE_TIMELINE_LAYOUT_VERSION)
        ? Option.of(new TimelineLayoutVersion(Integer.valueOf(props.getProperty(HOODIE_TIMELINE_LAYOUT_VERSION))))
        : Option.empty();
  }

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return props.containsKey(HOODIE_TABLE_VERSION_PROP_NAME)
        ? HoodieTableVersion.versionFromCode(Integer.parseInt(props.getProperty(HOODIE_TABLE_VERSION_PROP_NAME)))
        : DEFAULT_TABLE_VERSION;
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    props.put(HOODIE_TABLE_VERSION_PROP_NAME, Integer.toString(tableVersion.versionCode()));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return props.getProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME, DEFAULT_PAYLOAD_CLASS).replace("com.uber.hoodie",
        "org.apache.hudi");
  }

  public String getPreCombineField() {
    return props.getProperty(HOODIE_TABLE_PRECOMBINE_FIELD);
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getBootstrapIndexClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return props.getProperty(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME, DEFAULT_BOOTSTRAP_INDEX_CLASS);
  }

  public Option<String> getBootstrapBasePath() {
    return Option.ofNullable(props.getProperty(HOODIE_BOOTSTRAP_BASE_PATH));
  }

  /**
   * Read the table name.
   */
  public String getTableName() {
    return props.getProperty(HOODIE_TABLE_NAME_PROP_NAME);
  }

  /**
   * Get the base file storage format.
   *
   * @return HoodieFileFormat for the base file Storage format
   */
  public HoodieFileFormat getBaseFileFormat() {
    if (props.containsKey(HOODIE_BASE_FILE_FORMAT_PROP_NAME)) {
      return HoodieFileFormat.valueOf(props.getProperty(HOODIE_BASE_FILE_FORMAT_PROP_NAME));
    }
    if (props.containsKey(HOODIE_RO_FILE_FORMAT_PROP_NAME)) {
      return HoodieFileFormat.valueOf(props.getProperty(HOODIE_RO_FILE_FORMAT_PROP_NAME));
    }
    return DEFAULT_BASE_FILE_FORMAT;
  }

  /**
   * Get the log Storage Format.
   *
   * @return HoodieFileFormat for the log Storage format
   */
  public HoodieFileFormat getLogFileFormat() {
    if (props.containsKey(HOODIE_LOG_FILE_FORMAT_PROP_NAME)) {
      return HoodieFileFormat.valueOf(props.getProperty(HOODIE_LOG_FILE_FORMAT_PROP_NAME));
    }
    if (props.containsKey(HOODIE_RT_FILE_FORMAT_PROP_NAME)) {
      return HoodieFileFormat.valueOf(props.getProperty(HOODIE_RT_FILE_FORMAT_PROP_NAME));
    }
    return DEFAULT_LOG_FILE_FORMAT;
  }

  /**
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return props.getProperty(HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, DEFAULT_ARCHIVELOG_FOLDER);
  }

  public Map<String, String> getProps() {
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  public Properties getProperties() {
    return props;
  }

  public static PropertyBuilder propertyBuilder() {
    return new PropertyBuilder();
  }

  public static class PropertyBuilder {
    private Properties baseProperties = new Properties();
    private HoodieTableType tableType;
    private String tableName;
    private String archiveLogFolder;
    private String payloadClassName;
    private Integer timelineLayoutVersion;
    private String baseFileFormat;
    private String preCombineField;
    private String bootstrapIndexClass;
    private String bootstrapBasePath;

    private PropertyBuilder() {

    }

    public PropertyBuilder setTableType(HoodieTableType tableType) {
      this.tableType = tableType;
      return this;
    }

    public PropertyBuilder setTableType(String tableType) {
      return setTableType(HoodieTableType.valueOf(tableType));
    }

    public PropertyBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public PropertyBuilder setArchiveLogFolder(String archiveLogFolder) {
      this.archiveLogFolder = archiveLogFolder;
      return this;
    }

    public PropertyBuilder setPayloadClassName(String payloadClassName) {
      this.payloadClassName = payloadClassName;
      return this;
    }

    public PropertyBuilder setPayloadClass(Class<? extends HoodieRecordPayload> payloadClass) {
      return setPayloadClassName(payloadClass.getName());
    }

    public PropertyBuilder setTimelineLayoutVersion(Integer timelineLayoutVersion) {
      this.timelineLayoutVersion = timelineLayoutVersion;
      return this;
    }

    public PropertyBuilder setBaseFileFormat(String baseFileFormat) {
      this.baseFileFormat = baseFileFormat;
      return this;
    }

    public PropertyBuilder setPreCombineField(String preCombineField) {
      this.preCombineField = preCombineField;
      return this;
    }

    public PropertyBuilder setBootstrapIndexClass(String bootstrapIndexClass) {
      this.bootstrapIndexClass = bootstrapIndexClass;
      return this;
    }

    public PropertyBuilder setBootstrapBasePath(String bootstrapBasePath) {
      this.bootstrapBasePath = bootstrapBasePath;
      return this;
    }

    public PropertyBuilder fromMetaClient(HoodieTableMetaClient metaClient) {
      return setTableType(metaClient.getTableType())
        .setTableName(metaClient.getTableConfig().getTableName())
        .setArchiveLogFolder(metaClient.getArchivePath())
        .setPayloadClassName(metaClient.getTableConfig().getPayloadClass());
    }

    public PropertyBuilder fromProperties(Properties properties) {
      this.baseProperties = properties;
      return this;
    }

    public Properties build() {
      ValidationUtils.checkArgument(tableType != null, "tableType is null");
      ValidationUtils.checkArgument(tableName != null, "tableName is null");

      Properties properties = new Properties();
      if (baseProperties != null) {
        properties.putAll(baseProperties);
      }
      properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
      properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
      properties.setProperty(HoodieTableConfig.HOODIE_TABLE_VERSION_PROP_NAME, String.valueOf(HoodieTableVersion.current().versionCode()));
      if (tableType == HoodieTableType.MERGE_ON_READ && payloadClassName != null) {
        properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, payloadClassName);
      }

      if (null != archiveLogFolder) {
        properties.put(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, archiveLogFolder);
      }

      if (null != timelineLayoutVersion) {
        properties.put(HoodieTableConfig.HOODIE_TIMELINE_LAYOUT_VERSION, String.valueOf(timelineLayoutVersion));
      }

      if (null != baseFileFormat) {
        properties.setProperty(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP_NAME, baseFileFormat.toUpperCase());
      }

      if (null != bootstrapIndexClass) {
        properties.put(HoodieTableConfig.HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME, bootstrapIndexClass);
      }

      if (null != bootstrapBasePath) {
        properties.put(HoodieTableConfig.HOODIE_BOOTSTRAP_BASE_PATH, bootstrapBasePath);
      }

      if (null != preCombineField) {
        properties.put(HoodieTableConfig.HOODIE_TABLE_PRECOMBINE_FIELD, preCombineField);
      }
      return properties;
    }

    /**
     * Init Table with the properties build by this builder.
     * @param configuration The hadoop config.
     * @param basePath The base path for hoodie table.
     * @return
     * @throws IOException
     */
    public HoodieTableMetaClient initTable(Configuration configuration, String basePath) throws IOException {
      return HoodieTableMetaClient.initTableAndGetMetaClient(configuration, basePath, build());
    }
  }
}
