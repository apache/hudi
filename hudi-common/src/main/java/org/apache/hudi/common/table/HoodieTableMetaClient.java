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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <code>HoodieTableMetaClient</code> allows to access meta-data about a hoodie table It returns meta-data about
 * commits, savepoints, compactions, cleanups as a <code>HoodieTimeline</code> Create an instance of the
 * <code>HoodieTableMetaClient</code> with FileSystem and basePath to start getting the meta-data.
 * <p>
 * All the timelines are computed lazily, once computed the timeline is cached and never refreshed. Use the
 * <code>HoodieTimeline.reload()</code> to refresh timelines.
 *
 * @see HoodieTimeline
 * @since 0.3.0
 */
public class HoodieTableMetaClient implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieTableMetaClient.class);
  public static final String METAFOLDER_NAME = ".hoodie";
  public static final String TEMPFOLDER_NAME = METAFOLDER_NAME + Path.SEPARATOR + ".temp";
  public static final String AUXILIARYFOLDER_NAME = METAFOLDER_NAME + Path.SEPARATOR + ".aux";
  public static final String BOOTSTRAP_INDEX_ROOT_FOLDER_PATH = AUXILIARYFOLDER_NAME + Path.SEPARATOR + ".bootstrap";
  public static final String HEARTBEAT_FOLDER_NAME = METAFOLDER_NAME + Path.SEPARATOR + ".heartbeat";
  public static final String ZINDEX_NAME = ".zindex";
  public static final String BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH = BOOTSTRAP_INDEX_ROOT_FOLDER_PATH
      + Path.SEPARATOR + ".partitions";
  public static final String BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH = BOOTSTRAP_INDEX_ROOT_FOLDER_PATH + Path.SEPARATOR
      + ".fileids";

  public static final String MARKER_EXTN = ".marker";

  private String basePath;
  private transient HoodieWrapperFileSystem fs;
  private String metaPath;
  private boolean loadActiveTimelineOnLoad;
  private SerializableConfiguration hadoopConf;
  private HoodieTableType tableType;
  private TimelineLayoutVersion timelineLayoutVersion;
  private HoodieTableConfig tableConfig;
  private HoodieActiveTimeline activeTimeline;
  private HoodieArchivedTimeline archivedTimeline;
  private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();

  private HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad,
                                ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion,
                                String payloadClassName) {
    LOG.info("Loading HoodieTableMetaClient from " + basePath);
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.hadoopConf = new SerializableConfiguration(conf);
    Path basePathDir = new Path(basePath);
    this.basePath = basePathDir.toString();
    this.metaPath = new Path(basePath, METAFOLDER_NAME).toString();
    Path metaPathDir = new Path(this.metaPath);
    this.fs = getFs();
    TableNotFoundException.checkTableValidity(fs, basePathDir, metaPathDir);
    this.tableConfig = new HoodieTableConfig(fs, metaPath, payloadClassName);
    this.tableType = tableConfig.getTableType();
    Option<TimelineLayoutVersion> tableConfigVersion = tableConfig.getTimelineLayoutVersion();
    if (layoutVersion.isPresent() && tableConfigVersion.isPresent()) {
      // Ensure layout version passed in config is not lower than the one seen in hoodie.properties
      ValidationUtils.checkArgument(layoutVersion.get().compareTo(tableConfigVersion.get()) >= 0,
          "Layout Version defined in hoodie properties has higher version (" + tableConfigVersion.get()
              + ") than the one passed in config (" + layoutVersion.get() + ")");
    }
    this.timelineLayoutVersion = layoutVersion.orElseGet(() -> tableConfig.getTimelineLayoutVersion().get());
    this.loadActiveTimelineOnLoad = loadActiveTimelineOnLoad;
    LOG.info("Finished Loading Table of type " + tableType + "(version=" + timelineLayoutVersion + ", baseFileFormat="
        + this.tableConfig.getBaseFileFormat() + ") from " + basePath);
    if (loadActiveTimelineOnLoad) {
      LOG.info("Loading Active commit timeline for " + basePath);
      getActiveTimeline();
    }
  }

  /**
   * For serializing and de-serializing.
   *
   * @deprecated
   */
  public HoodieTableMetaClient() {}

  public static HoodieTableMetaClient reload(HoodieTableMetaClient oldMetaClient) {
    return HoodieTableMetaClient.builder().setConf(oldMetaClient.hadoopConf.get()).setBasePath(oldMetaClient.basePath).setLoadActiveTimelineOnLoad(oldMetaClient.loadActiveTimelineOnLoad)
        .setConsistencyGuardConfig(oldMetaClient.consistencyGuardConfig).setLayoutVersion(Option.of(oldMetaClient.timelineLayoutVersion)).setPayloadClassName(null).build();
  }

  /**
   * This method is only used when this object is de-serialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    fs = null; // will be lazily inited
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  /**
   * @return Base path
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * @return Hoodie Table Type
   */
  public HoodieTableType getTableType() {
    return tableType;
  }

  /**
   * @return Meta path
   */
  public String getMetaPath() {
    return metaPath;
  }

  /**
   * @return z-index path
   */
  public String getZindexPath() {
    return new Path(metaPath, ZINDEX_NAME).toString();
  }

  /**
   * @return Temp Folder path
   */
  public String getTempFolderPath() {
    return basePath + Path.SEPARATOR + TEMPFOLDER_NAME;
  }

  /**
   * Returns Marker folder path.
   * 
   * @param instantTs Instant Timestamp
   * @return
   */
  public String getMarkerFolderPath(String instantTs) {
    return String.format("%s%s%s", getTempFolderPath(), Path.SEPARATOR, instantTs);
  }

  /**
   * @return Auxiliary Meta path
   */
  public String getMetaAuxiliaryPath() {
    return basePath + Path.SEPARATOR + AUXILIARYFOLDER_NAME;
  }

  /**
   * @return Heartbeat folder path.
   */
  public static String getHeartbeatFolderPath(String basePath) {
    return String.format("%s%s%s", basePath, Path.SEPARATOR, HEARTBEAT_FOLDER_NAME);
  }

  /**
   * @return Bootstrap Index By Partition Folder
   */
  public String getBootstrapIndexByPartitionFolderPath() {
    return basePath + Path.SEPARATOR + BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH;
  }

  /**
   * @return Bootstrap Index By Hudi File Id Folder
   */
  public String getBootstrapIndexByFileIdFolderNameFolderPath() {
    return basePath + Path.SEPARATOR + BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH;
  }

  /**
   * @return path where archived timeline is stored
   */
  public String getArchivePath() {
    String archiveFolder = tableConfig.getArchivelogFolder();
    return getMetaPath() + Path.SEPARATOR + archiveFolder;
  }

  /**
   * @return Table Config
   */
  public HoodieTableConfig getTableConfig() {
    return tableConfig;
  }

  public TimelineLayoutVersion getTimelineLayoutVersion() {
    return timelineLayoutVersion;
  }

  /**
   * Get the FS implementation for this table.
   */
  public HoodieWrapperFileSystem getFs() {
    if (fs == null) {
      FileSystem fileSystem = FSUtils.getFs(metaPath, hadoopConf.newCopy());
      ValidationUtils.checkArgument(!(fileSystem instanceof HoodieWrapperFileSystem),
          "File System not expected to be that of HoodieWrapperFileSystem");
      fs = new HoodieWrapperFileSystem(fileSystem,
          consistencyGuardConfig.isConsistencyCheckEnabled()
              ? new FailSafeConsistencyGuard(fileSystem, consistencyGuardConfig)
              : new NoOpConsistencyGuard());
    }
    return fs;
  }

  /**
   * Return raw file-system.
   * 
   * @return fs
   */
  public FileSystem getRawFs() {
    return getFs().getFileSystem();
  }

  public Configuration getHadoopConf() {
    return hadoopConf.get();
  }

  /**
   * Get the active instants as a timeline.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline getActiveTimeline() {
    if (activeTimeline == null) {
      activeTimeline = new HoodieActiveTimeline(this);
    }
    return activeTimeline;
  }

  /**
   * Reload ActiveTimeline and cache.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline reloadActiveTimeline() {
    activeTimeline = new HoodieActiveTimeline(this);
    return activeTimeline;
  }

  public ConsistencyGuardConfig getConsistencyGuardConfig() {
    return consistencyGuardConfig;
  }

  /**
   * Get the archived commits as a timeline. This is costly operation, as all data from the archived files are read.
   * This should not be used, unless for historical debugging purposes.
   *
   * @return Active commit timeline
   */
  public synchronized HoodieArchivedTimeline getArchivedTimeline() {
    if (archivedTimeline == null) {
      archivedTimeline = new HoodieArchivedTimeline(this);
    }
    return archivedTimeline;
  }

  /**
   * Validate table properties.
   * @param properties Properties from writeConfig.
   * @param operationType operation type to be executed.
   */
  public void validateTableProperties(Properties properties, WriteOperationType operationType) {
    // once meta fields are disabled, it cant be re-enabled for a given table.
    if (!getTableConfig().populateMetaFields()
        && Boolean.parseBoolean((String) properties.getOrDefault(HoodieTableConfig.POPULATE_META_FIELDS.key(), HoodieTableConfig.POPULATE_META_FIELDS.defaultValue()))) {
      throw new HoodieException(HoodieTableConfig.POPULATE_META_FIELDS.key() + " already disabled for the table. Can't be re-enabled back");
    }

    // meta fields can be disabled only with SimpleKeyGenerator
    if (!getTableConfig().populateMetaFields()
        && !properties.getProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.SimpleKeyGenerator")
        .equals("org.apache.hudi.keygen.SimpleKeyGenerator")) {
      throw new HoodieException("Only simple key generator is supported when meta fields are disabled. KeyGenerator used : "
          + properties.getProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key()));
    }
  }

  /**
   * Helper method to initialize a given path as a hoodie table with configs passed in as Properties.
   *
   * @return Instance of HoodieTableMetaClient
   */
  public static HoodieTableMetaClient initTableAndGetMetaClient(Configuration hadoopConf, String basePath,
      Properties props) throws IOException {
    LOG.info("Initializing " + basePath + " as hoodie table " + basePath);
    Path basePathDir = new Path(basePath);
    final FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    if (!fs.exists(basePathDir)) {
      fs.mkdirs(basePathDir);
    }
    Path metaPathDir = new Path(basePath, METAFOLDER_NAME);
    if (!fs.exists(metaPathDir)) {
      fs.mkdirs(metaPathDir);
    }

    // if anything other than default archive log folder is specified, create that too
    String archiveLogPropVal = new HoodieConfig(props).getStringOrDefault(HoodieTableConfig.ARCHIVELOG_FOLDER);
    if (!StringUtils.isNullOrEmpty(archiveLogPropVal)) {
      Path archiveLogDir = new Path(metaPathDir, archiveLogPropVal);
      if (!fs.exists(archiveLogDir)) {
        fs.mkdirs(archiveLogDir);
      }
    }

    // Always create temporaryFolder which is needed for finalizeWrite for Hoodie tables
    final Path temporaryFolder = new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);
    if (!fs.exists(temporaryFolder)) {
      fs.mkdirs(temporaryFolder);
    }

    // Always create auxiliary folder which is needed to track compaction workloads (stats and any metadata in future)
    final Path auxiliaryFolder = new Path(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    if (!fs.exists(auxiliaryFolder)) {
      fs.mkdirs(auxiliaryFolder);
    }

    initializeBootstrapDirsIfNotExists(hadoopConf, basePath, fs);
    HoodieTableConfig.create(fs, metaPathDir, props);
    // We should not use fs.getConf as this might be different from the original configuration
    // used to create the fs in unit tests
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    LOG.info("Finished initializing Table of type " + metaClient.getTableConfig().getTableType() + " from " + basePath);
    return metaClient;
  }

  public static void initializeBootstrapDirsIfNotExists(Configuration hadoopConf,
      String basePath, FileSystem fs) throws IOException {

    // Create bootstrap index by partition folder if it does not exist
    final Path bootstrap_index_folder_by_partition =
        new Path(basePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH);
    if (!fs.exists(bootstrap_index_folder_by_partition)) {
      fs.mkdirs(bootstrap_index_folder_by_partition);
    }


    // Create bootstrap index by partition folder if it does not exist
    final Path bootstrap_index_folder_by_fileids =
        new Path(basePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH);
    if (!fs.exists(bootstrap_index_folder_by_fileids)) {
      fs.mkdirs(bootstrap_index_folder_by_fileids);
    }
  }

  /**
   * Helper method to scan all hoodie-instant metafiles.
   *
   * @param fs The file system implementation for this table
   * @param metaPath The meta path where meta files are stored
   * @param nameFilter The name filter to filter meta files
   * @return An array of meta FileStatus
   * @throws IOException In case of failure
   */
  public static FileStatus[] scanFiles(FileSystem fs, Path metaPath, PathFilter nameFilter) throws IOException {
    return fs.listStatus(metaPath, nameFilter);
  }

  /**
   * @return {@code true} if any commits are found, else {@code false}.
   */
  public boolean isTimelineNonEmpty() {
    return getCommitsTimeline().filterCompletedInstants().getInstants().collect(Collectors.toList()).size() > 0;
  }

  /**
   * Get the commit timeline visible for this table.
   */
  public HoodieTimeline getCommitsTimeline() {
    switch (this.getTableType()) {
      case COPY_ON_WRITE:
        return getActiveTimeline().getCommitTimeline();
      case MERGE_ON_READ:
        // We need to include the parquet files written out in delta commits
        // Include commit action to be able to start doing a MOR over a COW table - no
        // migration required
        return getActiveTimeline().getCommitsTimeline();
      default:
        throw new HoodieException("Unsupported table type :" + this.getTableType());
    }
  }

  /**
   * Get the commit + pending-compaction timeline visible for this table. A RT filesystem view is constructed with this
   * timeline so that file-slice after pending compaction-requested instant-time is also considered valid. A RT
   * file-system view for reading must then merge the file-slices before and after pending compaction instant so that
   * all delta-commits are read.
   */
  public HoodieTimeline getCommitsAndCompactionTimeline() {
    switch (this.getTableType()) {
      case COPY_ON_WRITE:
        return getActiveTimeline().getCommitTimeline();
      case MERGE_ON_READ:
        return getActiveTimeline().getWriteTimeline();
      default:
        throw new HoodieException("Unsupported table type :" + this.getTableType());
    }
  }

  /**
   * Get the compacted commit timeline visible for this table.
   */
  public HoodieTimeline getCommitTimeline() {
    switch (this.getTableType()) {
      case COPY_ON_WRITE:
      case MERGE_ON_READ:
        // We need to include the parquet files written out in delta commits in tagging
        return getActiveTimeline().getCommitTimeline();
      default:
        throw new HoodieException("Unsupported table type :" + this.getTableType());
    }
  }

  /**
   * Gets the commit action type.
   */
  public String getCommitActionType() {
    return CommitUtils.getCommitActionType(this.getTableType());
  }

  /**
   * Helper method to scan all hoodie-instant metafiles and construct HoodieInstant objects.
   *
   * @param includedExtensions Included hoodie extensions
   * @param applyLayoutVersionFilters Depending on Timeline layout version, if there are multiple states for the same
   * action instant, only include the highest state
   * @return List of Hoodie Instants generated
   * @throws IOException in case of failure
   */
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(Set<String> includedExtensions,
      boolean applyLayoutVersionFilters) throws IOException {
    return scanHoodieInstantsFromFileSystem(new Path(metaPath), includedExtensions, applyLayoutVersionFilters);
  }

  /**
   * Helper method to scan all hoodie-instant metafiles and construct HoodieInstant objects.
   *
   * @param timelinePath MetaPath where instant files are stored
   * @param includedExtensions Included hoodie extensions
   * @param applyLayoutVersionFilters Depending on Timeline layout version, if there are multiple states for the same
   * action instant, only include the highest state
   * @return List of Hoodie Instants generated
   * @throws IOException in case of failure
   */
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(Path timelinePath, Set<String> includedExtensions,
      boolean applyLayoutVersionFilters) throws IOException {
    Stream<HoodieInstant> instantStream = Arrays.stream(
        HoodieTableMetaClient
            .scanFiles(getFs(), timelinePath, path -> {
              // Include only the meta files with extensions that needs to be included
              String extension = HoodieInstant.getTimelineFileExtension(path.getName());
              return includedExtensions.contains(extension);
            })).map(HoodieInstant::new);

    if (applyLayoutVersionFilters) {
      instantStream = TimelineLayout.getLayout(getTimelineLayoutVersion()).filterHoodieInstants(instantStream);
    }
    return instantStream.sorted().collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieTableMetaClient that = (HoodieTableMetaClient) o;
    return Objects.equals(basePath, that.basePath) && tableType == that.tableType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(basePath, tableType);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieTableMetaClient{");
    sb.append("basePath='").append(basePath).append('\'');
    sb.append(", metaPath='").append(metaPath).append('\'');
    sb.append(", tableType=").append(tableType);
    sb.append('}');
    return sb.toString();
  }

  public void initializeBootstrapDirsIfNotExists() throws IOException {
    initializeBootstrapDirsIfNotExists(getHadoopConf(), basePath, getFs());
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public void setMetaPath(String metaPath) {
    this.metaPath = metaPath;
  }

  public void setActiveTimeline(HoodieActiveTimeline activeTimeline) {
    this.activeTimeline = activeTimeline;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link HoodieTableMetaClient}.
   */
  public static class Builder {

    private Configuration conf;
    private String basePath;
    private boolean loadActiveTimelineOnLoad = false;
    private String payloadClassName = null;
    private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
    private Option<TimelineLayoutVersion> layoutVersion = Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION);

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder setLoadActiveTimelineOnLoad(boolean loadActiveTimelineOnLoad) {
      this.loadActiveTimelineOnLoad = loadActiveTimelineOnLoad;
      return this;
    }

    public Builder setPayloadClassName(String payloadClassName) {
      this.payloadClassName = payloadClassName;
      return this;
    }

    public Builder setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      this.consistencyGuardConfig = consistencyGuardConfig;
      return this;
    }

    public Builder setLayoutVersion(Option<TimelineLayoutVersion> layoutVersion) {
      this.layoutVersion = layoutVersion;
      return this;
    }

    public HoodieTableMetaClient build() {
      ValidationUtils.checkArgument(conf != null, "Configuration needs to be set to init HoodieTableMetaClient");
      ValidationUtils.checkArgument(basePath != null, "basePath needs to be set to init HoodieTableMetaClient");
      return new HoodieTableMetaClient(conf, basePath,
          loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, payloadClassName);
    }
  }

  public static PropertyBuilder withPropertyBuilder() {
    return new PropertyBuilder();
  }

  public static class PropertyBuilder {

    private HoodieTableType tableType;
    private String tableName;
    private String tableCreateSchema;
    private String recordKeyFields;
    private String archiveLogFolder;
    private String payloadClassName;
    private Integer timelineLayoutVersion;
    private String baseFileFormat;
    private String preCombineField;
    private String partitionFields;
    private String bootstrapIndexClass;
    private String bootstrapBasePath;
    private Boolean bootstrapIndexEnable;
    private Boolean populateMetaFields;
    private String keyGeneratorClassProp;
    private Boolean hiveStylePartitioningEnable;
    private Boolean urlEncodePartitioning;

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

    public PropertyBuilder setTableCreateSchema(String tableCreateSchema) {
      this.tableCreateSchema = tableCreateSchema;
      return this;
    }

    public PropertyBuilder setRecordKeyFields(String recordKeyFields) {
      this.recordKeyFields = recordKeyFields;
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

    public PropertyBuilder setPartitionFields(String partitionFields) {
      this.partitionFields = partitionFields;
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

    public PropertyBuilder setBootstrapIndexEnable(Boolean bootstrapIndexEnable) {
      this.bootstrapIndexEnable = bootstrapIndexEnable;
      return this;
    }

    public PropertyBuilder setPopulateMetaFields(boolean populateMetaFields) {
      this.populateMetaFields = populateMetaFields;
      return this;
    }

    public PropertyBuilder setKeyGeneratorClassProp(String keyGeneratorClassProp) {
      this.keyGeneratorClassProp = keyGeneratorClassProp;
      return this;
    }

    public PropertyBuilder setHiveStylePartitioningEnable(Boolean hiveStylePartitioningEnable) {
      this.hiveStylePartitioningEnable = hiveStylePartitioningEnable;
      return this;
    }

    public PropertyBuilder setUrlEncodePartitioning(Boolean urlEncodePartitioning) {
      this.urlEncodePartitioning = urlEncodePartitioning;
      return this;
    }

    public PropertyBuilder fromMetaClient(HoodieTableMetaClient metaClient) {
      return setTableType(metaClient.getTableType())
        .setTableName(metaClient.getTableConfig().getTableName())
        .setArchiveLogFolder(metaClient.getArchivePath())
        .setPayloadClassName(metaClient.getTableConfig().getPayloadClass());
    }

    public PropertyBuilder fromProperties(Properties properties) {
      HoodieConfig hoodieConfig = new HoodieConfig(properties);
      if (hoodieConfig.contains(HoodieTableConfig.NAME)) {
        setTableName(hoodieConfig.getString(HoodieTableConfig.NAME));
      }
      if (hoodieConfig.contains(HoodieTableConfig.TYPE)) {
        setTableType(hoodieConfig.getString(HoodieTableConfig.TYPE));
      }
      if (hoodieConfig.contains(HoodieTableConfig.ARCHIVELOG_FOLDER)) {
        setArchiveLogFolder(
            hoodieConfig.getString(HoodieTableConfig.ARCHIVELOG_FOLDER));
      }
      if (hoodieConfig.contains(HoodieTableConfig.PAYLOAD_CLASS_NAME)) {
        setPayloadClassName(
            hoodieConfig.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME));
      }
      if (hoodieConfig.contains(HoodieTableConfig.TIMELINE_LAYOUT_VERSION)) {
        setTimelineLayoutVersion(hoodieConfig.getInt(HoodieTableConfig.TIMELINE_LAYOUT_VERSION));
      }
      if (hoodieConfig.contains(HoodieTableConfig.BASE_FILE_FORMAT)) {
        setBaseFileFormat(
            hoodieConfig.getString(HoodieTableConfig.BASE_FILE_FORMAT));
      }
      if (hoodieConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME)) {
        setBootstrapIndexClass(
            hoodieConfig.getString(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME));
      }
      if (hoodieConfig.contains(HoodieTableConfig.BOOTSTRAP_BASE_PATH)) {
        setBootstrapBasePath(hoodieConfig.getString(HoodieTableConfig.BOOTSTRAP_BASE_PATH));
      }

      if (hoodieConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE)) {
        setBootstrapIndexEnable(hoodieConfig.getBoolean(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE));
      }

      if (hoodieConfig.contains(HoodieTableConfig.PRECOMBINE_FIELD)) {
        setPreCombineField(hoodieConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD));
      }
      if (hoodieConfig.contains(HoodieTableConfig.PARTITION_FIELDS)) {
        setPartitionFields(
            hoodieConfig.getString(HoodieTableConfig.PARTITION_FIELDS));
      }
      if (hoodieConfig.contains(HoodieTableConfig.RECORDKEY_FIELDS)) {
        setRecordKeyFields(hoodieConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS));
      }
      if (hoodieConfig.contains(HoodieTableConfig.CREATE_SCHEMA)) {
        setTableCreateSchema(hoodieConfig.getString(HoodieTableConfig.CREATE_SCHEMA));
      }
      if (hoodieConfig.contains(HoodieTableConfig.POPULATE_META_FIELDS)) {
        setPopulateMetaFields(hoodieConfig.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS));
      }
      if (hoodieConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)) {
        setKeyGeneratorClassProp(hoodieConfig.getString(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME));
      }
      if (hoodieConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE)) {
        setHiveStylePartitioningEnable(hoodieConfig.getBoolean(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
      }
      if (hoodieConfig.contains(HoodieTableConfig.URL_ENCODE_PARTITIONING)) {
        setUrlEncodePartitioning(hoodieConfig.getBoolean(HoodieTableConfig.URL_ENCODE_PARTITIONING));
      }
      return this;
    }

    public Properties build() {
      ValidationUtils.checkArgument(tableType != null, "tableType is null");
      ValidationUtils.checkArgument(tableName != null, "tableName is null");

      HoodieTableConfig tableConfig = new HoodieTableConfig();
      tableConfig.setValue(HoodieTableConfig.NAME, tableName);
      tableConfig.setValue(HoodieTableConfig.TYPE, tableType.name());
      tableConfig.setValue(HoodieTableConfig.VERSION,
          String.valueOf(HoodieTableVersion.current().versionCode()));
      if (tableType == HoodieTableType.MERGE_ON_READ && payloadClassName != null) {
        tableConfig.setValue(HoodieTableConfig.PAYLOAD_CLASS_NAME, payloadClassName);
      }

      if (null != tableCreateSchema) {
        tableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA, tableCreateSchema);
      }

      if (!StringUtils.isNullOrEmpty(archiveLogFolder)) {
        tableConfig.setValue(HoodieTableConfig.ARCHIVELOG_FOLDER, archiveLogFolder);
      } else {
        tableConfig.setDefaultValue(HoodieTableConfig.ARCHIVELOG_FOLDER);
      }

      if (null != timelineLayoutVersion) {
        tableConfig.setValue(HoodieTableConfig.TIMELINE_LAYOUT_VERSION,
            String.valueOf(timelineLayoutVersion));
      }

      if (null != baseFileFormat) {
        tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, baseFileFormat.toUpperCase());
      }

      if (null != bootstrapIndexClass) {
        tableConfig.setValue(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME, bootstrapIndexClass);
      }

      if (null != bootstrapIndexEnable) {
        tableConfig.setValue(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE, Boolean.toString(bootstrapIndexEnable));
      }

      if (null != bootstrapBasePath) {
        tableConfig.setValue(HoodieTableConfig.BOOTSTRAP_BASE_PATH, bootstrapBasePath);
      }

      if (null != preCombineField) {
        tableConfig.setValue(HoodieTableConfig.PRECOMBINE_FIELD, preCombineField);
      }

      if (null != partitionFields) {
        tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, partitionFields);
      }
      if (null != recordKeyFields) {
        tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, recordKeyFields);
      }
      if (null != populateMetaFields) {
        tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, Boolean.toString(populateMetaFields));
      }
      if (null != keyGeneratorClassProp) {
        tableConfig.setValue(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGeneratorClassProp);
      }
      if (null != hiveStylePartitioningEnable) {
        tableConfig.setValue(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE, Boolean.toString(hiveStylePartitioningEnable));
      }
      if (null != urlEncodePartitioning) {
        tableConfig.setValue(HoodieTableConfig.URL_ENCODE_PARTITIONING, Boolean.toString(urlEncodePartitioning));
      }
      return tableConfig.getProps();
    }

    /**
     * Init Table with the properties build by this builder.
     *
     * @param configuration The hadoop config.
     * @param basePath The base path for hoodie table.
     */
    public HoodieTableMetaClient initTable(Configuration configuration, String basePath)
        throws IOException {
      return HoodieTableMetaClient.initTableAndGetMetaClient(configuration, basePath, build());
    }
  }
}
