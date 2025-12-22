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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableMetaClient.class);
  public static final String METAFOLDER_NAME = ".hoodie";
  public static final String TEMPFOLDER_NAME = METAFOLDER_NAME + StoragePath.SEPARATOR + ".temp";
  public static final String AUXILIARYFOLDER_NAME = METAFOLDER_NAME + StoragePath.SEPARATOR + ".aux";
  public static final String BOOTSTRAP_INDEX_ROOT_FOLDER_PATH = AUXILIARYFOLDER_NAME + StoragePath.SEPARATOR + ".bootstrap";
  public static final String SAMPLE_WRITES_FOLDER_PATH = AUXILIARYFOLDER_NAME + StoragePath.SEPARATOR + ".sample_writes";
  public static final String HEARTBEAT_FOLDER_NAME = METAFOLDER_NAME + StoragePath.SEPARATOR + ".heartbeat";
  public static final String METADATA_TABLE_FOLDER_PATH = METAFOLDER_NAME + StoragePath.SEPARATOR + "metadata";
  public static final String HASHING_METADATA_FOLDER_NAME =
      ".bucket_index" + StoragePath.SEPARATOR + "consistent_hashing_metadata";
  public static final String BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH = BOOTSTRAP_INDEX_ROOT_FOLDER_PATH
      + StoragePath.SEPARATOR + ".partitions";
  public static final String BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH =
      BOOTSTRAP_INDEX_ROOT_FOLDER_PATH + StoragePath.SEPARATOR + ".fileids";

  public static final String LOCKS_FOLDER_NAME = METAFOLDER_NAME + StoragePath.SEPARATOR + ".locks";

  public static final String SCHEMA_FOLDER_NAME = ".schema";

  public static final String MARKER_EXTN = ".marker";

  // In-memory cache for archived timeline based on the start instant time
  // Only one entry should be present in this map
  private final Map<String, HoodieArchivedTimeline> archivedTimelineMap = new HashMap<>();

  protected StoragePath basePath;
  protected StoragePath metaPath;

  private transient HoodieStorage storage;
  private boolean loadActiveTimelineOnLoad;
  protected StorageConfiguration<?> storageConf;
  private HoodieTableType tableType;
  private TimelineLayoutVersion timelineLayoutVersion;
  protected HoodieTableConfig tableConfig;
  protected HoodieActiveTimeline activeTimeline;
  private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
  private FileSystemRetryConfig fileSystemRetryConfig = FileSystemRetryConfig.newBuilder().build();
  protected HoodieMetaserverConfig metaserverConfig;

  /**
   * Instantiate HoodieTableMetaClient.
   * Can only be called if table already exists
   */
  protected HoodieTableMetaClient(HoodieStorage storage, String basePath, boolean loadActiveTimelineOnLoad,
                                  ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion,
                                  String payloadClassName, String recordMergerStrategy, FileSystemRetryConfig fileSystemRetryConfig) {
    LOG.info("Loading HoodieTableMetaClient from " + basePath);
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.fileSystemRetryConfig = fileSystemRetryConfig;
    this.storageConf = storage.getConf();
    this.storage = storage;
    this.basePath = new StoragePath(basePath);
    this.metaPath = new StoragePath(basePath, METAFOLDER_NAME);
    TableNotFoundException.checkTableValidity(this.storage, this.basePath, metaPath);
    this.tableConfig = new HoodieTableConfig(this.storage, metaPath, payloadClassName, recordMergerStrategy);
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
  public HoodieTableMetaClient() {
  }

  public static HoodieTableMetaClient reload(HoodieTableMetaClient oldMetaClient) {
    return HoodieTableMetaClient.builder()
        .setStorage(oldMetaClient.getStorage())
        .setBasePath(oldMetaClient.basePath.toString())
        .setLoadActiveTimelineOnLoad(oldMetaClient.loadActiveTimelineOnLoad)
        .setConsistencyGuardConfig(oldMetaClient.consistencyGuardConfig)
        .setLayoutVersion(Option.of(oldMetaClient.timelineLayoutVersion))
        .setPayloadClassName(null)
        .setRecordMergerStrategy(null)
        .setFileSystemRetryConfig(oldMetaClient.fileSystemRetryConfig).build();
  }

  /**
   * This method is only used when this object is de-serialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    storage = null; // will be lazily initialized
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  /**
   * Returns base path of the table
   */
  public StoragePath getBasePath() {
    return basePath; // this invocation is cached
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
  public StoragePath getMetaPath() {
    return metaPath;
  }

  /**
   * @return schema folder path
   */
  public String getSchemaFolderName() {
    return new StoragePath(metaPath, SCHEMA_FOLDER_NAME).toString();
  }

  /**
   * @return Hashing metadata base path
   */
  public String getHashingMetadataPath() {
    return new StoragePath(metaPath, HASHING_METADATA_FOLDER_NAME).toString();
  }

  /**
   * @return Temp Folder path
   */
  public String getTempFolderPath() {
    return basePath + StoragePath.SEPARATOR + TEMPFOLDER_NAME;
  }

  /**
   * Returns Marker folder path.
   *
   * @param instantTs Instant Timestamp
   * @return
   */
  public String getMarkerFolderPath(String instantTs) {
    return String.format("%s%s%s", getTempFolderPath(), StoragePath.SEPARATOR, instantTs);
  }

  /**
   * @return Auxiliary Meta path
   */
  public String getMetaAuxiliaryPath() {
    return basePath + StoragePath.SEPARATOR + AUXILIARYFOLDER_NAME;
  }

  /**
   * @return Heartbeat folder path.
   */
  public static String getHeartbeatFolderPath(String basePath) {
    return String.format("%s%s%s", basePath, StoragePath.SEPARATOR, HEARTBEAT_FOLDER_NAME);
  }

  /**
   * @return Bootstrap Index By Partition Folder
   */
  public String getBootstrapIndexByPartitionFolderPath() {
    return basePath + StoragePath.SEPARATOR + BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH;
  }

  /**
   * @return Bootstrap Index By Hudi File Id Folder
   */
  public String getBootstrapIndexByFileIdFolderNameFolderPath() {
    return basePath + StoragePath.SEPARATOR + BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH;
  }

  /**
   * @return path where archived timeline is stored
   */
  public String getArchivePath() {
    String archiveFolder = tableConfig.getArchivelogFolder();
    return getMetaPath() + StoragePath.SEPARATOR + archiveFolder;
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

  public Boolean isMetadataTable() {
    return HoodieTableMetadata.isMetadataTable(getBasePath());
  }

  public HoodieStorage getStorage() {
    if (storage == null) {
      storage = getStorage(metaPath, getStorageConf(), consistencyGuardConfig, fileSystemRetryConfig);
    }
    return storage;
  }

  private static HoodieStorage getStorage(StoragePath path,
                                          StorageConfiguration<?> storageConf,
                                          ConsistencyGuardConfig consistencyGuardConfig,
                                          FileSystemRetryConfig fileSystemRetryConfig) {
    HoodieStorage newStorage = HoodieStorageUtils.getStorage(path, storageConf);
    ConsistencyGuard consistencyGuard = consistencyGuardConfig.isConsistencyCheckEnabled()
        ? new FailSafeConsistencyGuard(newStorage, consistencyGuardConfig)
        : new NoOpConsistencyGuard();

    return getIOFactory(newStorage).getStorage(path,
        fileSystemRetryConfig.isFileSystemActionRetryEnable(),
        fileSystemRetryConfig.getMaxRetryIntervalMs(),
        fileSystemRetryConfig.getMaxRetryNumbers(),
        fileSystemRetryConfig.getInitialRetryIntervalMs(),
        fileSystemRetryConfig.getRetryExceptions(),
        consistencyGuard);
  }

  public void setHoodieStorage(HoodieStorage storage) {
    this.storage = storage;
  }

  public HoodieStorage getRawHoodieStorage() {
    return getStorage().getRawStorage();
  }

  public StorageConfiguration<?> getStorageConf() {
    return storageConf;
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

  public FileSystemRetryConfig getFileSystemRetryConfig() {
    return fileSystemRetryConfig;
  }

  /**
   * Get the archived commits as a timeline. This is costly operation, as all data from the archived files are read.
   * This should not be used, unless for historical debugging purposes.
   *
   * @return Archived commit timeline
   */
  public synchronized HoodieArchivedTimeline getArchivedTimeline() {
    return getArchivedTimeline(StringUtils.EMPTY_STRING);
  }

  public HoodieMetaserverConfig getMetaserverConfig() {
    if (metaserverConfig == null) {
      metaserverConfig = new HoodieMetaserverConfig();
    }
    return metaserverConfig;
  }

  /**
   * Returns the cached archived timeline from startTs (inclusive).
   *
   * @param startTs The start instant time (inclusive) of the archived timeline.
   * @return the archived timeline.
   */
  public HoodieArchivedTimeline getArchivedTimeline(String startTs) {
    return getArchivedTimeline(startTs, true);
  }

  /**
   * Returns the cached archived timeline if using in-memory cache or a fresh new archived
   * timeline if not using cache, from startTs (inclusive).
   * <p>
   * Instantiating an archived timeline is costly operation if really early startTs is
   * specified.
   * <p>
   * This method is not thread safe.
   *
   * @param startTs  The start instant time (inclusive) of the archived timeline.
   * @param useCache Whether to use in-memory cache.
   * @return the archived timeline based on the arguments.
   */
  public HoodieArchivedTimeline getArchivedTimeline(String startTs, boolean useCache) {
    if (useCache) {
      if (!archivedTimelineMap.containsKey(startTs)) {
        // Only keep one entry in the map
        archivedTimelineMap.clear();
        archivedTimelineMap.put(startTs, instantiateArchivedTimeline(startTs));
      }
      return archivedTimelineMap.get(startTs);
    }
    return instantiateArchivedTimeline(startTs);
  }

  private HoodieArchivedTimeline instantiateArchivedTimeline(String startTs) {
    return StringUtils.isNullOrEmpty(startTs)
        ? new HoodieArchivedTimeline(this)
        : new HoodieArchivedTimeline(this, startTs);
  }

  /**
   * Validate table properties.
   *
   * @param properties Properties from writeConfig.
   */
  public void validateTableProperties(Properties properties) {
    // Once meta fields are disabled, it cant be re-enabled for a given table.
    if (!getTableConfig().populateMetaFields()
        && Boolean.parseBoolean((String) properties.getOrDefault(HoodieTableConfig.POPULATE_META_FIELDS.key(), HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString()))) {
      throw new HoodieException(HoodieTableConfig.POPULATE_META_FIELDS.key() + " already disabled for the table. Can't be re-enabled back");
    }

    // Meta fields can be disabled only when either {@code SimpleKeyGenerator}, {@code ComplexKeyGenerator}, {@code NonpartitionedKeyGenerator} is used
    if (!getTableConfig().populateMetaFields()) {
      String keyGenClass = properties.getProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      if (!keyGenClass.equals("org.apache.hudi.keygen.SimpleKeyGenerator")
          && !keyGenClass.equals("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
          && !keyGenClass.equals("org.apache.hudi.keygen.ComplexKeyGenerator")) {
        throw new HoodieException("Only simple, non-partitioned or complex key generator are supported when meta-fields are disabled. Used: " + keyGenClass);
      }
    }

    //Check to make sure it's not a COW table with consistent hashing bucket index
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      String indexType = properties.getProperty("hoodie.index.type");
      if (indexType != null && indexType.equals("BUCKET")) {
        String bucketEngine = properties.getProperty("hoodie.index.bucket.engine");
        if (bucketEngine != null && bucketEngine.equals("CONSISTENT_HASHING")) {
          throw new HoodieException("Consistent hashing bucket index does not work with COW table. Use simple bucket index or an MOR table.");
        }
      }
    }
  }

  /**
   * Helper method to initialize a given path as a hoodie table with configs passed in as Properties.
   *
   * @return Instance of HoodieTableMetaClient
   */
  public static HoodieTableMetaClient initTableAndGetMetaClient(StorageConfiguration<?> storageConf, String basePath,
                                                                Properties props) throws IOException {
    return initTableAndGetMetaClient(storageConf, new StoragePath(basePath), props);
  }

  public static HoodieTableMetaClient initTableAndGetMetaClient(StorageConfiguration<?> storageConf, StoragePath basePath,
                                                                Properties props) throws IOException {
    initTableMetaClient(storageConf, basePath, props);
    // We should not use fs.getConf as this might be different from the original configuration
    // used to create the fs in unit tests
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath)
            .setMetaserverConfig(props)
            .build();
    LOG.info("Finished initializing Table of type " + metaClient.getTableConfig().getTableType() + " from " + basePath);
    return metaClient;
  }

  private static void initTableMetaClient(StorageConfiguration<?> storageConf, StoragePath basePath,
                                          Properties props) throws IOException {
    LOG.info("Initializing " + basePath + " as hoodie table " + basePath);
    final HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    if (!storage.exists(basePath)) {
      storage.createDirectory(basePath);
    }
    StoragePath metaPathDir = new StoragePath(basePath, METAFOLDER_NAME);
    if (!storage.exists(metaPathDir)) {
      storage.createDirectory(metaPathDir);
    }
    // create schema folder
    StoragePath schemaPathDir = new StoragePath(metaPathDir, SCHEMA_FOLDER_NAME);
    if (!storage.exists(schemaPathDir)) {
      storage.createDirectory(schemaPathDir);
    }

    // if anything other than default archive log folder is specified, create that too
    String archiveLogPropVal = new HoodieConfig(props).getStringOrDefault(HoodieTableConfig.ARCHIVELOG_FOLDER);
    if (!StringUtils.isNullOrEmpty(archiveLogPropVal)) {
      StoragePath archiveLogDir = new StoragePath(metaPathDir, archiveLogPropVal);
      if (!storage.exists(archiveLogDir)) {
        storage.createDirectory(archiveLogDir);
      }
    }

    // Always create temporaryFolder which is needed for finalizeWrite for Hoodie tables
    final StoragePath temporaryFolder = new StoragePath(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);
    if (!storage.exists(temporaryFolder)) {
      storage.createDirectory(temporaryFolder);
    }

    // Always create auxiliary folder which is needed to track compaction workloads (stats and any metadata in future)
    final StoragePath auxiliaryFolder = new StoragePath(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    if (!storage.exists(auxiliaryFolder)) {
      storage.createDirectory(auxiliaryFolder);
    }

    initializeBootstrapDirsIfNotExists(basePath, storage);
    HoodieTableConfig.create(storage, metaPathDir, props);
  }

  public static void initializeBootstrapDirsIfNotExists(StoragePath basePath, HoodieStorage storage) throws IOException {

    // Create bootstrap index by partition folder if it does not exist
    final StoragePath bootstrap_index_folder_by_partition =
        new StoragePath(basePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH);
    if (!storage.exists(bootstrap_index_folder_by_partition)) {
      storage.createDirectory(bootstrap_index_folder_by_partition);
    }


    // Create bootstrap index by partition folder if it does not exist
    final StoragePath bootstrap_index_folder_by_fileids =
        new StoragePath(basePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_BY_FILE_ID_FOLDER_PATH);
    if (!storage.exists(bootstrap_index_folder_by_fileids)) {
      storage.createDirectory(bootstrap_index_folder_by_fileids);
    }
  }

  /**
   * Helper method to scan all hoodie-instant metafiles.
   *
   * @param storage    The file system implementation for this table
   * @param metaPath   The meta path where meta files are stored
   * @param nameFilter The name filter to filter meta files
   * @return An array of meta FileStatus
   * @throws IOException In case of failure
   */
  public static List<StoragePathInfo> scanFiles(HoodieStorage storage, StoragePath metaPath,
                                                StoragePathFilter nameFilter) throws IOException {
    return storage.listDirectEntries(metaPath, nameFilter);
  }

  /**
   * @return {@code true} if any commits are found, else {@code false}.
   */
  public boolean isTimelineNonEmpty() {
    return !getCommitsTimeline().filterCompletedInstants().empty();
  }

  /**
   * Get the commit timeline visible for this table.
   */
  public HoodieTimeline getCommitsTimeline() {
    switch (this.getTableType()) {
      case COPY_ON_WRITE:
        return getActiveTimeline().getCommitAndReplaceTimeline();
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
        return getActiveTimeline().getCommitAndReplaceTimeline();
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
        return getActiveTimeline().getCommitAndReplaceTimeline();
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
   * @param includedExtensions        Included hoodie extensions
   * @param applyLayoutVersionFilters Depending on Timeline layout version, if there are multiple states for the same
   *                                  action instant, only include the highest state
   * @return List of Hoodie Instants generated
   * @throws IOException in case of failure
   */
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(Set<String> includedExtensions,
                                                              boolean applyLayoutVersionFilters) throws IOException {
    return scanHoodieInstantsFromFileSystem(metaPath, includedExtensions, applyLayoutVersionFilters);
  }

  /**
   * Helper method to scan all hoodie-instant metafiles and construct HoodieInstant objects.
   *
   * @param timelinePath              MetaPath where instant files are stored
   * @param includedExtensions        Included hoodie extensions
   * @param applyLayoutVersionFilters Depending on Timeline layout version, if there are multiple states for the same
   *                                  action instant, only include the highest state
   * @return List of Hoodie Instants generated
   * @throws IOException in case of failure
   */
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(StoragePath timelinePath, Set<String> includedExtensions,
                                                              boolean applyLayoutVersionFilters) throws IOException {
    Stream<HoodieInstant> instantStream =
        HoodieTableMetaClient
            .scanFiles(getStorage(), timelinePath, path -> {
              // Include only the meta files with extensions that needs to be included
              String extension = HoodieInstant.getTimelineFileExtension(path.getName());
              return includedExtensions.contains(extension);
            }).stream().map(HoodieInstant::new);

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
    initializeBootstrapDirsIfNotExists(basePath, getStorage());
  }

  private static HoodieTableMetaClient newMetaClient(HoodieStorage storage, String basePath, boolean loadActiveTimelineOnLoad,
                                                     ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion,
                                                     String payloadClassName, String recordMergerStrategy, FileSystemRetryConfig fileSystemRetryConfig, HoodieMetaserverConfig metaserverConfig) {
    return metaserverConfig.isMetaserverEnabled()
        ? (HoodieTableMetaClient) ReflectionUtils.loadClass("org.apache.hudi.common.table.HoodieTableMetaserverClient",
        new Class<?>[] {HoodieStorage.class, String.class, ConsistencyGuardConfig.class, String.class,
            FileSystemRetryConfig.class, Option.class, Option.class, HoodieMetaserverConfig.class},
        storage, basePath, consistencyGuardConfig, recordMergerStrategy, fileSystemRetryConfig,
        Option.ofNullable(metaserverConfig.getDatabaseName()), Option.ofNullable(metaserverConfig.getTableName()), metaserverConfig)
        : new HoodieTableMetaClient(storage, basePath,
        loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, payloadClassName, recordMergerStrategy, fileSystemRetryConfig);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link HoodieTableMetaClient}.
   */
  public static class Builder {

    private StorageConfiguration<?> conf;
    private HoodieStorage storage;
    private String basePath;
    private boolean loadActiveTimelineOnLoad = false;
    private String payloadClassName = null;
    private String recordMergerStrategy = null;
    private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
    private FileSystemRetryConfig fileSystemRetryConfig = FileSystemRetryConfig.newBuilder().build();
    private HoodieMetaserverConfig metaserverConfig = HoodieMetaserverConfig.newBuilder().build();
    private Option<TimelineLayoutVersion> layoutVersion = Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION);

    public Builder setConf(StorageConfiguration<?> conf) {
      this.conf = conf;
      return this;
    }

    public Builder setStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    public Builder setBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder setBasePath(StoragePath basePath) {
      this.basePath = basePath.toString();
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

    public Builder setRecordMergerStrategy(String recordMergerStrategy) {
      this.recordMergerStrategy = recordMergerStrategy;
      return this;
    }

    public Builder setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      this.consistencyGuardConfig = consistencyGuardConfig;
      return this;
    }

    public Builder setFileSystemRetryConfig(FileSystemRetryConfig fileSystemRetryConfig) {
      this.fileSystemRetryConfig = fileSystemRetryConfig;
      return this;
    }

    public Builder setLayoutVersion(Option<TimelineLayoutVersion> layoutVersion) {
      this.layoutVersion = layoutVersion;
      return this;
    }

    public Builder setMetaserverConfig(Properties props) {
      this.metaserverConfig = new HoodieMetaserverConfig.Builder().fromProperties(props).build();
      return this;
    }

    public Builder setMetaserverConfig(Map<String, String> map) {
      Properties properties = new Properties();
      properties.putAll(map);
      return setMetaserverConfig(properties);
    }

    public HoodieTableMetaClient build() {
      ValidationUtils.checkArgument(conf != null || storage != null,
          "Storage configuration or HoodieStorage needs to be set to init HoodieTableMetaClient");
      ValidationUtils.checkArgument(basePath != null, "basePath needs to be set to init HoodieTableMetaClient");
      if (storage == null) {
        storage = getStorage(new StoragePath(basePath), conf, consistencyGuardConfig, fileSystemRetryConfig);
      }
      return newMetaClient(storage, basePath,
          loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, payloadClassName,
          recordMergerStrategy, fileSystemRetryConfig, metaserverConfig);
    }
  }

  public static PropertyBuilder withPropertyBuilder() {
    return new PropertyBuilder();
  }

  /**
   * Builder for {@link Properties}.
   */
  public static class PropertyBuilder {

    private HoodieTableType tableType;
    private String databaseName;
    private String tableName;
    private String tableCreateSchema;
    private String recordKeyFields;
    private String archiveLogFolder;
    private String payloadClassName;
    private String recordMergerStrategy;
    private Integer timelineLayoutVersion;
    private String baseFileFormat;
    private String preCombineField;
    private String partitionFields;
    private Boolean cdcEnabled;
    private String cdcSupplementalLoggingMode;
    private String bootstrapIndexClass;
    private String bootstrapBasePath;
    private Boolean bootstrapIndexEnable;
    private Boolean populateMetaFields;
    private String keyGeneratorClassProp;
    private Boolean hiveStylePartitioningEnable;
    private Boolean urlEncodePartitioning;
    private HoodieTimelineTimeZone commitTimeZone;
    private Boolean partitionMetafileUseBaseFormat;
    private Boolean shouldDropPartitionColumns;
    private String metadataPartitions;
    private String inflightMetadataPartitions;
    private String secondaryIndexesMetadata;

    /**
     * Persist the configs that is written at the first time, and should not be changed.
     * Like KeyGenerator's configs.
     */
    private Properties others = new Properties();

    private PropertyBuilder() {

    }

    public PropertyBuilder setTableType(HoodieTableType tableType) {
      this.tableType = tableType;
      return this;
    }

    public PropertyBuilder setTableType(String tableType) {
      return setTableType(HoodieTableType.valueOf(tableType));
    }

    public PropertyBuilder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
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

    public PropertyBuilder setRecordMergerStrategy(String recordMergerStrategy) {
      this.recordMergerStrategy = recordMergerStrategy;
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

    public PropertyBuilder setCDCEnabled(boolean cdcEnabled) {
      this.cdcEnabled = cdcEnabled;
      return this;
    }

    public PropertyBuilder setCDCSupplementalLoggingMode(String cdcSupplementalLoggingMode) {
      this.cdcSupplementalLoggingMode = cdcSupplementalLoggingMode.toUpperCase();
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

    public PropertyBuilder setCommitTimezone(HoodieTimelineTimeZone timelineTimeZone) {
      this.commitTimeZone = timelineTimeZone;
      return this;
    }

    public PropertyBuilder setPartitionMetafileUseBaseFormat(Boolean useBaseFormat) {
      this.partitionMetafileUseBaseFormat = useBaseFormat;
      return this;
    }

    public PropertyBuilder setShouldDropPartitionColumns(Boolean shouldDropPartitionColumns) {
      this.shouldDropPartitionColumns = shouldDropPartitionColumns;
      return this;
    }

    public PropertyBuilder setMetadataPartitions(String partitions) {
      this.metadataPartitions = partitions;
      return this;
    }

    public PropertyBuilder setInflightMetadataPartitions(String partitions) {
      this.inflightMetadataPartitions = partitions;
      return this;
    }

    public PropertyBuilder setSecondaryIndexesMetadata(String secondaryIndexesMetadata) {
      this.secondaryIndexesMetadata = secondaryIndexesMetadata;
      return this;
    }

    public PropertyBuilder set(Map<String, Object> props) {
      for (ConfigProperty<String> configProperty : HoodieTableConfig.PERSISTED_CONFIG_LIST) {
        if (containsConfigProperty(props, configProperty)) {
          String value = getStringWithAltKeys(props, configProperty);
          if (value != null) {
            this.others.put(configProperty.key(), value);
          }
        }
      }
      return this;
    }

    public PropertyBuilder fromMetaClient(HoodieTableMetaClient metaClient) {
      return setTableType(metaClient.getTableType())
          .setTableName(metaClient.getTableConfig().getTableName())
          .setArchiveLogFolder(metaClient.getArchivePath())
          .setPayloadClassName(metaClient.getTableConfig().getPayloadClass())
          .setRecordMergerStrategy(metaClient.getTableConfig().getRecordMergerStrategy());
    }

    public PropertyBuilder fromProperties(Properties properties) {
      HoodieConfig hoodieConfig = new HoodieConfig(properties);

      for (ConfigProperty<String> configProperty : HoodieTableConfig.PERSISTED_CONFIG_LIST) {
        if (hoodieConfig.contains(configProperty)) {
          String value = hoodieConfig.getString(configProperty);
          if (value != null) {
            this.others.put(configProperty.key(), value);
          }
        }
      }

      if (hoodieConfig.contains(HoodieTableConfig.DATABASE_NAME)) {
        setDatabaseName(hoodieConfig.getString(HoodieTableConfig.DATABASE_NAME));
      }
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
      if (hoodieConfig.contains(HoodieTableConfig.RECORD_MERGER_STRATEGY)) {
        setRecordMergerStrategy(
            hoodieConfig.getString(HoodieTableConfig.RECORD_MERGER_STRATEGY));
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
      if (hoodieConfig.contains(HoodieTableConfig.CDC_ENABLED)) {
        setCDCEnabled(hoodieConfig.getBoolean(HoodieTableConfig.CDC_ENABLED));
      }
      if (hoodieConfig.contains(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE)) {
        setCDCSupplementalLoggingMode(hoodieConfig.getString(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE));
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
      if (hoodieConfig.contains(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT)) {
        setPartitionMetafileUseBaseFormat(hoodieConfig.getBoolean(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT));
      }
      if (hoodieConfig.contains(HoodieTableConfig.DROP_PARTITION_COLUMNS)) {
        setShouldDropPartitionColumns(hoodieConfig.getBoolean(HoodieTableConfig.DROP_PARTITION_COLUMNS));
      }
      if (hoodieConfig.contains(HoodieTableConfig.TABLE_METADATA_PARTITIONS)) {
        setMetadataPartitions(hoodieConfig.getString(HoodieTableConfig.TABLE_METADATA_PARTITIONS));
      }
      if (hoodieConfig.contains(HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT)) {
        setInflightMetadataPartitions(hoodieConfig.getString(HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT));
      }
      if (hoodieConfig.contains(HoodieTableConfig.SECONDARY_INDEXES_METADATA)) {
        setSecondaryIndexesMetadata(hoodieConfig.getString(HoodieTableConfig.SECONDARY_INDEXES_METADATA));
      }
      return this;
    }

    public Properties build() {
      ValidationUtils.checkArgument(tableType != null, "tableType is null");
      ValidationUtils.checkArgument(tableName != null, "tableName is null");

      HoodieTableConfig tableConfig = new HoodieTableConfig();

      tableConfig.setAll(others);

      if (databaseName != null) {
        tableConfig.setValue(HoodieTableConfig.DATABASE_NAME, databaseName);
      }
      tableConfig.setValue(HoodieTableConfig.NAME, tableName);
      tableConfig.setValue(HoodieTableConfig.TYPE, tableType.name());
      tableConfig.setValue(HoodieTableConfig.VERSION,
          String.valueOf(HoodieTableVersion.current().versionCode()));
      if (tableType == HoodieTableType.MERGE_ON_READ && payloadClassName != null) {
        tableConfig.setValue(HoodieTableConfig.PAYLOAD_CLASS_NAME, payloadClassName);
      }
      if (tableType == HoodieTableType.MERGE_ON_READ && recordMergerStrategy != null) {
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGER_STRATEGY, recordMergerStrategy);
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

      if (StringUtils.nonEmpty(preCombineField)) {
        tableConfig.setValue(HoodieTableConfig.PRECOMBINE_FIELD, preCombineField);
      }

      if (null != partitionFields) {
        tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, partitionFields);
      }
      if (null != recordKeyFields) {
        tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, recordKeyFields);
      }
      if (null != cdcEnabled) {
        tableConfig.setValue(HoodieTableConfig.CDC_ENABLED, Boolean.toString(cdcEnabled));
        if (cdcEnabled && null != cdcSupplementalLoggingMode) {
          tableConfig.setValue(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE, cdcSupplementalLoggingMode);
        }
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
      if (null != commitTimeZone) {
        tableConfig.setValue(HoodieTableConfig.TIMELINE_TIMEZONE, commitTimeZone.toString());
      }
      if (null != partitionMetafileUseBaseFormat) {
        tableConfig.setValue(HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT, partitionMetafileUseBaseFormat.toString());
      }
      if (null != shouldDropPartitionColumns) {
        tableConfig.setValue(HoodieTableConfig.DROP_PARTITION_COLUMNS, Boolean.toString(shouldDropPartitionColumns));
      }
      if (null != metadataPartitions) {
        tableConfig.setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS, metadataPartitions);
      }
      if (null != inflightMetadataPartitions) {
        tableConfig.setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT, inflightMetadataPartitions);
      }
      if (null != secondaryIndexesMetadata) {
        tableConfig.setValue(HoodieTableConfig.SECONDARY_INDEXES_METADATA, secondaryIndexesMetadata);
      }
      return tableConfig.getProps();
    }

    /**
     * Init Table with the properties build by this builder.
     *
     * @param configuration The storage configuration.
     * @param basePath      The base path for hoodie table.
     */
    public HoodieTableMetaClient initTable(StorageConfiguration<?> configuration, String basePath)
        throws IOException {
      return HoodieTableMetaClient.initTableAndGetMetaClient(configuration, basePath, build());
    }

    public HoodieTableMetaClient initTable(StorageConfiguration<?> configuration, StoragePath basePath) throws IOException {
      return HoodieTableMetaClient.initTableAndGetMetaClient(configuration, basePath, build());
    }

  }
}
