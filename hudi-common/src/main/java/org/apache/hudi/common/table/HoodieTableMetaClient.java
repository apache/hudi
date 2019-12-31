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

import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.io.storage.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FailSafeConsistencyGuard;
import org.apache.hudi.common.util.NoOpConsistencyGuard;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.DatasetNotFoundException;
import org.apache.hudi.exception.HoodieException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableMetaClient.class);
  public static String METAFOLDER_NAME = ".hoodie";
  public static String TEMPFOLDER_NAME = METAFOLDER_NAME + File.separator + ".temp";
  public static String AUXILIARYFOLDER_NAME = METAFOLDER_NAME + File.separator + ".aux";
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

  public HoodieTableMetaClient(Configuration conf, String basePath) throws DatasetNotFoundException {
    // Do not load any timeline by default
    this(conf, basePath, false);
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad) {
    this(conf, basePath, loadActiveTimelineOnLoad, ConsistencyGuardConfig.newBuilder().build(),
        Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION));
  }

  public HoodieTableMetaClient(Configuration conf, String basePath, boolean loadActiveTimelineOnLoad,
      ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion)
      throws DatasetNotFoundException {
    LOG.info("Loading HoodieTableMetaClient from {}", basePath);
    this.basePath = basePath;
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.hadoopConf = new SerializableConfiguration(conf);
    Path basePathDir = new Path(this.basePath);
    this.metaPath = new Path(basePath, METAFOLDER_NAME).toString();
    Path metaPathDir = new Path(this.metaPath);
    this.fs = getFs();
    DatasetNotFoundException.checkValidDataset(fs, basePathDir, metaPathDir);
    this.tableConfig = new HoodieTableConfig(fs, metaPath);
    this.tableType = tableConfig.getTableType();
    this.timelineLayoutVersion = layoutVersion.orElse(tableConfig.getTimelineLayoutVersion());
    this.loadActiveTimelineOnLoad = loadActiveTimelineOnLoad;
    LOG.info("Finished Loading Table of type {}(version={}) from {}", tableType, timelineLayoutVersion, basePath);
    if (loadActiveTimelineOnLoad) {
      LOG.info("Loading Active commit timeline for {}", basePath);
      getActiveTimeline();
    }
  }

  /**
   * For serailizing and de-serializing.
   *
   * @deprecated
   */
  public HoodieTableMetaClient() {}

  public static HoodieTableMetaClient reload(HoodieTableMetaClient oldMetaClient) {
    return new HoodieTableMetaClient(oldMetaClient.hadoopConf.get(), oldMetaClient.basePath,
        oldMetaClient.loadActiveTimelineOnLoad, oldMetaClient.consistencyGuardConfig,
        Option.of(oldMetaClient.timelineLayoutVersion));
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
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
      Preconditions.checkArgument(!(fileSystem instanceof HoodieWrapperFileSystem),
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
   * @return
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
   * Helper method to initialize a dataset, with given basePath, tableType, name, archiveFolder.
   */
  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath, String tableType,
      String tableName, String archiveLogFolder) throws IOException {
    return initTableType(hadoopConf, basePath, HoodieTableType.valueOf(tableType), tableName,
        archiveLogFolder, null, null);
  }

  /**
   * Helper method to initialize a given path, as a given storage type and table name.
   */
  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath,
      HoodieTableType tableType, String tableName, String payloadClassName) throws IOException {
    return initTableType(hadoopConf, basePath, tableType, tableName, null, payloadClassName, null);
  }

  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath,
      HoodieTableType tableType, String tableName, String archiveLogFolder, String payloadClassName,
      Integer timelineLayoutVersion) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
    if (tableType == HoodieTableType.MERGE_ON_READ && payloadClassName != null) {
      properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, payloadClassName);
    }

    if (null != archiveLogFolder) {
      properties.put(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, archiveLogFolder);
    }

    if (null != timelineLayoutVersion) {
      properties.put(HoodieTableConfig.HOODIE_TIMELINE_LAYOUT_VERSION, String.valueOf(timelineLayoutVersion));
    }
    return HoodieTableMetaClient.initDatasetAndGetMetaClient(hadoopConf, basePath, properties);
  }

  /**
   * Helper method to initialize a given path as a hoodie dataset with configs passed in as as Properties.
   *
   * @return Instance of HoodieTableMetaClient
   */
  public static HoodieTableMetaClient initDatasetAndGetMetaClient(Configuration hadoopConf, String basePath,
      Properties props) throws IOException {
    LOG.info("Initializing {} as hoodie dataset {}", basePath, basePath);
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

    HoodieTableConfig.createHoodieProperties(fs, metaPathDir, props);
    // We should not use fs.getConf as this might be different from the original configuration
    // used to create the fs in unit tests
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
    LOG.info("Finished initializing Table of type {} from {}", metaClient.getTableConfig().getTableType(), basePath);
    return metaClient;
  }

  // HELPER METHODS TO CREATE META FILE NAMES
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
        // Include commit action to be able to start doing a MOR over a COW dataset - no
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
        throw new HoodieException("Could not commit on unknown storage type " + this.getTableType());
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
              String extension = FSUtils.getFileExtension(path.getName());
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

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public void setMetaPath(String metaPath) {
    this.metaPath = metaPath;
  }

  public void setActiveTimeline(HoodieActiveTimeline activeTimeline) {
    this.activeTimeline = activeTimeline;
  }

  public void setTableConfig(HoodieTableConfig tableConfig) {
    this.tableConfig = tableConfig;
  }
}
