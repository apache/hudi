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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
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

import java.io.File;
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
  public static final String TEMPFOLDER_NAME = METAFOLDER_NAME + File.separator + ".temp";
  public static final String AUXILIARYFOLDER_NAME = METAFOLDER_NAME + File.separator + ".aux";
  public static final String BOOTSTRAP_INDEX_ROOT_FOLDER_PATH = AUXILIARYFOLDER_NAME + File.separator + ".bootstrap";

  public static final String BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH = BOOTSTRAP_INDEX_ROOT_FOLDER_PATH
      + File.separator + ".partitions";
  public static final String BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH = BOOTSTRAP_INDEX_ROOT_FOLDER_PATH + File.separator
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

  public HoodieTableMetaClient(Configuration conf, String basePath) {
    // Do not load any timeline by default
    this(conf, basePath, false);
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, String payloadClassName) {
    this(conf, basePath, false, ConsistencyGuardConfig.newBuilder().build(), Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION),
        payloadClassName);
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad,
                               ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion) {
    this(conf, basePath, loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, null);
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad) {
    this(conf, basePath, loadActiveTimelineOnLoad, ConsistencyGuardConfig.newBuilder().build(), Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION), null);
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad,
                               ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion,
                               String payloadClassName) {
    LOG.info("Loading HoodieTableMetaClient from " + basePath);
    this.basePath = basePath;
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.hadoopConf = new SerializableConfiguration(conf);
    Path basePathDir = new Path(this.basePath);
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
    return new HoodieTableMetaClient(oldMetaClient.hadoopConf.get(), oldMetaClient.basePath,
        oldMetaClient.loadActiveTimelineOnLoad, oldMetaClient.consistencyGuardConfig,
        Option.of(oldMetaClient.timelineLayoutVersion), null);
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
   * @return Temp Folder path
   */
  public String getTempFolderPath() {
    return basePath + File.separator + TEMPFOLDER_NAME;
  }

  /**
   * Returns Marker folder path.
   * 
   * @param instantTs Instant Timestamp
   * @return
   */
  public String getMarkerFolderPath(String instantTs) {
    return String.format("%s%s%s", getTempFolderPath(), File.separator, instantTs);
  }

  /**
   * @return Auxiliary Meta path
   */
  public String getMetaAuxiliaryPath() {
    return basePath + File.separator + AUXILIARYFOLDER_NAME;
  }

  /**
   * @return Bootstrap Index By Partition Folder
   */
  public String getBootstrapIndexByPartitionFolderPath() {
    return basePath + File.separator + BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH;
  }

  /**
   * @return Bootstrap Index By Hudi File Id Folder
   */
  public String getBootstrapIndexByFileIdFolderNameFolderPath() {
    return basePath + File.separator + BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH;
  }

  /**
   * @return path where archived timeline is stored
   */
  public String getArchivePath() {
    String archiveFolder = tableConfig.getArchivelogFolder();
    if (archiveFolder.equals(HoodieTableConfig.DEFAULT_ARCHIVELOG_FOLDER)) {
      return getMetaPath();
    } else {
      return getMetaPath() + "/" + archiveFolder;
    }
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
   * Helper method to initialize a table, with given basePath, tableType, name, archiveFolder, payloadClass and
   * base file format.
   */
  public static HoodieTableMetaClient initTableTypeWithBootstrap(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                                 String tableName, String archiveLogFolder, String payloadClassName,
                                                                 String baseFileFormat, String bootstrapIndexClass,
                                                                 String bootstrapBasePath) throws IOException {
    return initTableType(hadoopConf, basePath, tableType, tableName,
        archiveLogFolder, payloadClassName, null, baseFileFormat, bootstrapIndexClass, bootstrapBasePath);
  }

  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                    String tableName, String archiveLogFolder, String payloadClassName,
                                                    String baseFileFormat) throws IOException {
    return initTableType(hadoopConf, basePath, tableType, tableName,
        archiveLogFolder, payloadClassName, null, baseFileFormat, null, null);
  }

  /**
   * Used primarily by tests, examples.
   */
  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                    String tableName, String payloadClassName) throws IOException {
    return initTableType(hadoopConf, basePath, tableType, tableName, null, payloadClassName,
        null, null, null, null);
  }

  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                    String tableName, String archiveLogFolder, String payloadClassName,
                                                    Integer timelineLayoutVersion) throws IOException {
    return initTableType(hadoopConf, basePath, tableType, tableName, archiveLogFolder, payloadClassName,
        timelineLayoutVersion, null, null, null);
  }

  private static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                     String tableName, String archiveLogFolder, String payloadClassName,
                                                     Integer timelineLayoutVersion, String baseFileFormat,
                                                     String bootstrapIndexClass, String bootstrapBasePath) throws IOException {
    Properties properties = new Properties();
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

    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
  }

  /**
   * Helper method to initialize a given path as a hoodie table with configs passed in as as Properties.
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
    String archiveLogPropVal = props.getProperty(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME,
        HoodieTableConfig.DEFAULT_ARCHIVELOG_FOLDER);
    if (!archiveLogPropVal.equals(HoodieTableConfig.DEFAULT_ARCHIVELOG_FOLDER)) {
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
    HoodieTableConfig.createHoodieProperties(fs, metaPathDir, props);
    // We should not use fs.getConf as this might be different from the original configuration
    // used to create the fs in unit tests
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
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
        return getActiveTimeline().getCommitsAndCompactionTimeline();
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
    switch (this.getTableType()) {
      case COPY_ON_WRITE:
        return HoodieActiveTimeline.COMMIT_ACTION;
      case MERGE_ON_READ:
        return HoodieActiveTimeline.DELTA_COMMIT_ACTION;
      default:
        throw new HoodieException("Could not commit on unknown table type " + this.getTableType());
    }
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
}
