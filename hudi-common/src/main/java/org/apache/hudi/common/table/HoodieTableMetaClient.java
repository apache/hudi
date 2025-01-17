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
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
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

import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.VERSION;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

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
  public static final String TIMELINEFOLDER_NAME = "timeline";
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

  public static final String SCHEMA_FOLDER_NAME = ".schema";

  public static final String MARKER_EXTN = ".marker";

  public static final String INDEX_DEFINITION_FOLDER_NAME = ".index_defs";
  public static final String INDEX_DEFINITION_FILE_NAME = "index.json";

  public static final String COMMIT_TIME_KEY = "commitTime";

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
  private TimelineLayout timelineLayout;
  private StoragePath timelinePath;
  private StoragePath timelineHistoryPath;
  protected HoodieTableConfig tableConfig;
  protected HoodieActiveTimeline activeTimeline;
  private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
  private FileSystemRetryConfig fileSystemRetryConfig = FileSystemRetryConfig.newBuilder().build();
  protected HoodieMetaserverConfig metaserverConfig;
  private HoodieTimeGeneratorConfig timeGeneratorConfig;
  private Option<HoodieIndexMetadata> indexMetadataOpt = Option.empty();

  /**
   * Instantiate HoodieTableMetaClient.
   * Can only be called if table already exists
   */
  protected HoodieTableMetaClient(HoodieStorage storage, String basePath, boolean loadActiveTimelineOnLoad,
                                ConsistencyGuardConfig consistencyGuardConfig, Option<TimelineLayoutVersion> layoutVersion,
                                RecordMergeMode recordMergeMode, String payloadClassName, String recordMergerStrategy,
                                  HoodieTimeGeneratorConfig timeGeneratorConfig, FileSystemRetryConfig fileSystemRetryConfig) {
    LOG.info("Loading HoodieTableMetaClient from " + basePath);
    this.timeGeneratorConfig = timeGeneratorConfig;
    this.consistencyGuardConfig = consistencyGuardConfig;
    this.fileSystemRetryConfig = fileSystemRetryConfig;
    this.storageConf = storage.getConf();
    this.storage = storage;
    this.basePath = new StoragePath(basePath);
    this.metaPath = new StoragePath(basePath, METAFOLDER_NAME);
    this.tableConfig = new HoodieTableConfig(this.storage, metaPath, recordMergeMode, payloadClassName, recordMergerStrategy);
    this.indexMetadataOpt = getIndexMetadata();
    this.tableType = tableConfig.getTableType();
    Option<TimelineLayoutVersion> tableConfigVersion = tableConfig.getTimelineLayoutVersion();
    if (layoutVersion.isPresent() && tableConfigVersion.isPresent()) {
      // Ensure layout version passed in config is not lower than the one seen in hoodie.properties
      checkArgument(layoutVersion.get().compareTo(tableConfigVersion.get()) >= 0,
          "Layout Version defined in hoodie properties has higher version (" + tableConfigVersion.get()
              + ") than the one passed in config (" + layoutVersion.get() + ")");
    } else if (layoutVersion.isEmpty() && tableConfigVersion.isEmpty()) {
      throw new TableNotFoundException("Table does not exist");
    }
    this.timelineLayoutVersion = layoutVersion.orElseGet(tableConfigVersion::get);
    this.timelineLayout = TimelineLayout.fromVersion(timelineLayoutVersion);
    this.timelinePath = timelineLayout.getTimelinePathProvider().getTimelinePath(tableConfig, this.basePath);
    this.timelineHistoryPath = timelineLayout.getTimelinePathProvider().getTimelineHistoryPath(tableConfig, this.basePath);
    this.loadActiveTimelineOnLoad = loadActiveTimelineOnLoad;
    LOG.info("Finished Loading Table of type " + tableType + "(version=" + timelineLayoutVersion + ") from " + basePath);
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

  public String getIndexDefinitionPath() {
    return tableConfig.getRelativeIndexDefinitionPath()
        .map(definitionPath -> new StoragePath(basePath, definitionPath).toString())
        .orElseGet(() -> metaPath + StoragePath.SEPARATOR + HoodieTableMetaClient.INDEX_DEFINITION_FOLDER_NAME
            + StoragePath.SEPARATOR + HoodieTableMetaClient.INDEX_DEFINITION_FILE_NAME);
  }

  /**
   * Builds expression index definition and writes to index definition file.
   * Support mutable and immutable index definition. Only col stats is mutable, while all others are immutable.
   * Inacse of immutable index definition, we could only create or delete the definition.
   * Incase of mutable (col stats), list of source columns (or list of columns to index) could also change.
   * @return true if index definition is updated.
   */
  public boolean buildIndexDefinition(HoodieIndexDefinition indexDefinition) {
    String indexName = indexDefinition.getIndexName();
    boolean isIndexDefnImmutable = !indexDefinition.getIndexName().equals(PARTITION_NAME_COLUMN_STATS); // only col stats is mutable.
    if (isIndexDefnImmutable) {
      checkState(
          !indexMetadataOpt.isPresent() || (!indexMetadataOpt.get().getIndexDefinitions().containsKey(indexName)),
          "Index metadata is already present");
    }
    String indexMetaPath = getIndexDefinitionPath();
    boolean updateIndexDefn = true;
    if (indexMetadataOpt.isPresent()) {
      if (isIndexDefnImmutable) {
        indexMetadataOpt.get().getIndexDefinitions().put(indexName, indexDefinition);
      } else {
        // if index defn is mutable, lets check for difference and only update if required.
        if (indexMetadataOpt.get().getIndexDefinitions().containsKey(indexName)) {
          if (!indexMetadataOpt.get().getIndexDefinitions().get(indexName).getSourceFields().equals(indexDefinition.getSourceFields())) {
            LOG.info(String.format("List of columns to index is changing. Old value %s. New value %s",
                indexMetadataOpt.get().getIndexDefinitions().get(indexName).getSourceFields(), indexDefinition.getSourceFields()));
            indexMetadataOpt.get().getIndexDefinitions().put(indexName, indexDefinition);
          } else {
            updateIndexDefn = false;
          }
        } else {
          indexMetadataOpt.get().getIndexDefinitions().put(indexName, indexDefinition);
        }
      }
    } else {
      Map<String, HoodieIndexDefinition> indexDefinitionMap = new HashMap<>();
      indexDefinitionMap.put(indexName, indexDefinition);
      indexMetadataOpt = Option.of(new HoodieIndexMetadata(indexDefinitionMap));
    }
    if (updateIndexDefn) {
      try {
        FileIOUtils.createFileInPath(storage, new StoragePath(indexMetaPath), Option.of(getUTF8Bytes(indexMetadataOpt.get().toJson())));
      } catch (IOException e) {
        throw new HoodieIOException("Could not write expression index metadata at path: " + indexMetaPath, e);
      }
    }
    return updateIndexDefn;
  }

  /**
   * Deletes index definition and writes to index definition file.
   *
   * @param indexName Name of the index
   */
  public void deleteIndexDefinition(String indexName) {
    checkState(indexMetadataOpt.isPresent(), "Index metadata is not present");
    indexMetadataOpt.get().getIndexDefinitions().remove(indexName);
    String indexMetaPath = getIndexDefinitionPath();
    try {
      FileIOUtils.createFileInPath(storage, new StoragePath(indexMetaPath), Option.of(getUTF8Bytes(indexMetadataOpt.get().toJson())));
    } catch (IOException e) {
      throw new HoodieIOException("Could not write expression index metadata at path: " + indexMetaPath, e);
    }
  }

  /**
   * Returns Option of {@link HoodieIndexMetadata} from index definition file if present, else returns empty Option.
   */
  public Option<HoodieIndexMetadata> getIndexMetadata() {
    if (indexMetadataOpt.isPresent() && !indexMetadataOpt.get().getIndexDefinitions().isEmpty()) {
      return indexMetadataOpt;
    }
    if (tableConfig.getRelativeIndexDefinitionPath().isPresent() && StringUtils.nonEmpty(tableConfig.getRelativeIndexDefinitionPath().get())) {
      StoragePath indexDefinitionPath =
          new StoragePath(basePath, tableConfig.getRelativeIndexDefinitionPath().get());
      try {
        Option<byte[]> bytesOpt = FileIOUtils.readDataFromPath(storage, indexDefinitionPath, true);
        if (bytesOpt.isPresent()) {
          return Option.of(HoodieIndexMetadata.fromJson(new String(bytesOpt.get())));
        } else {
          return Option.of(new HoodieIndexMetadata());
        }
      } catch (IOException e) {
        throw new HoodieIOException("Could not load expression index metadata at path: " + tableConfig.getRelativeIndexDefinitionPath().get(), e);
      }
    }
    return Option.empty();
  }

  public void updateIndexMetadata(HoodieIndexMetadata newExpressionIndexMetadata, String indexMetaPath) {
    this.indexMetadataOpt = Option.of(newExpressionIndexMetadata);
    try {
      // update the index metadata file as well
      FileIOUtils.createFileInPath(storage, new StoragePath(indexMetaPath), Option.of(getUTF8Bytes(indexMetadataOpt.get().toJson())));
    } catch (IOException e) {
      throw new HoodieIOException("Could not write expression index metadata at path: " + indexMetaPath, e);
    }
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
        .setTimeGeneratorConfig(oldMetaClient.timeGeneratorConfig)
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

  public StoragePath getTimelinePath() {
    return timelinePath;
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
  public StoragePath getArchivePath() {
    return timelineHistoryPath;
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

  public TimelineLayout getTimelineLayout() {
    return timelineLayout;
  }

  public Boolean isMetadataTable() {
    return HoodieTableMetadata.isMetadataTable(getBasePath());
  }

  public HoodieStorage getStorage() {
    if (storage == null) {
      storage = getStorage(metaPath);
    }
    return storage;
  }

  public HoodieStorage getStorage(StoragePath storagePath) {
    return getStorage(storagePath, getStorageConf(), consistencyGuardConfig, fileSystemRetryConfig);
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

  public void setStorage(HoodieStorage storage) {
    this.storage = storage;
  }

  public HoodieStorage getRawStorage() {
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
      activeTimeline = timelineLayout.getTimelineFactory().createActiveTimeline(this);
    }
    return activeTimeline;
  }

  /**
   * Reload ActiveTimeline and cache.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline reloadActiveTimeline() {
    activeTimeline = timelineLayout.getTimelineFactory().createActiveTimeline(this);
    return activeTimeline;
  }

  /**
   * Reload the table config properties.
   */
  public synchronized void reloadTableConfig() {
    this.tableConfig = HoodieTableConfig.loadFromHoodieProps(this.storage, metaPath);
    reloadTimelineLayoutAndPath();
  }

  /**
   * Reload the timeline layout info.
   */
  private void reloadTimelineLayoutAndPath() {
    this.timelineLayoutVersion = tableConfig.getTimelineLayoutVersion().get();
    this.timelineLayout = TimelineLayout.fromVersion(timelineLayoutVersion);
    this.timelinePath = timelineLayout.getTimelinePathProvider().getTimelinePath(tableConfig, basePath);
    this.timelineHistoryPath = timelineLayout.getTimelinePathProvider().getTimelineHistoryPath(tableConfig, basePath);
  }

  /**
   * Returns next instant time in the correct format. Lock is enabled by default.
   */
  public String createNewInstantTime() {
    return createNewInstantTime(true);
  }

  /**
   * Returns next instant time in the correct format.
   *
   * @param shouldLock whether the lock should be enabled to get the instant time.
   */
  public String createNewInstantTime(boolean shouldLock) {
    TimeGenerator timeGenerator = TimeGenerators
        .getTimeGenerator(timeGeneratorConfig, storageConf);
    return TimelineUtils.generateInstantTime(shouldLock, timeGenerator);
  }

  public HoodieTimeGeneratorConfig getTimeGeneratorConfig() {
    return timeGeneratorConfig;
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
        ? timelineLayout.getTimelineFactory().createArchivedTimeline(this)
        : timelineLayout.getTimelineFactory().createArchivedTimeline(this, startTs);
  }

  public static void createTableLayoutOnStorage(StorageConfiguration<?> storageConf,
                                                StoragePath basePath,
                                                Properties props,
                                                Integer timelineLayout,
                                                boolean shouldCreateTableConfig) throws IOException {
    LOG.info("Initializing {} as hoodie table", basePath);
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

    // if anything other than default timeline path is specified, create that too
    String timelinePropVal = new HoodieConfig(props).getStringOrDefault(TIMELINE_PATH);
    StoragePath timelineDir = metaPathDir;
    timelineLayout = timelineLayout == null ? TimelineLayoutVersion.CURR_VERSION : timelineLayout;
    if (!StringUtils.isNullOrEmpty(timelinePropVal) && TimelineLayoutVersion.VERSION_2.equals(timelineLayout)) {
      timelineDir = new StoragePath(metaPathDir, timelinePropVal);
      if (!storage.exists(timelineDir)) {
        storage.createDirectory(timelineDir);
      }
    }

    // if anything other than default timeline history path is specified, create that too
    String archiveLogPropVal = new HoodieConfig(props).getStringOrDefault(HoodieTableConfig.TIMELINE_HISTORY_PATH);
    if (!StringUtils.isNullOrEmpty(archiveLogPropVal) && TimelineLayoutVersion.VERSION_2.equals(timelineLayout)) {
      StoragePath archiveLogDir = new StoragePath(timelineDir, archiveLogPropVal);
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
    if (shouldCreateTableConfig) {
      HoodieTableConfig.create(storage, metaPathDir, props);
    }
  }

  public static void initializeBootstrapDirsIfNotExists(StoragePath basePath, HoodieStorage storage) throws IOException {

    // Create bootstrap index by partition folder if it does not exist
    final StoragePath bootstrap_index_folder_by_partition =
        new StoragePath(basePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_BY_PARTITION_FOLDER_PATH);
    if (!storage.exists(bootstrap_index_folder_by_partition)) {
      storage.createDirectory(bootstrap_index_folder_by_partition);
    }


    // Create bootstrap index by file-id folder if it does not exist
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
   * @param timelinePath              MetaPath where instant files are stored
   * @param includedExtensions        Included hoodie extensions
   * @param applyLayoutVersionFilters Depending on Timeline layout version, if there are multiple states for the same
   *                                  action instant, only include the highest state
   * @return List of Hoodie Instants generated
   * @throws IOException in case of failure
   */
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(StoragePath timelinePath, Set<String> includedExtensions,
                                                              boolean applyLayoutVersionFilters) throws IOException {
    final InstantGenerator instantGenerator = timelineLayout.getInstantGenerator();
    Stream<HoodieInstant> instantStream =
        HoodieTableMetaClient
            .scanFiles(getStorage(), timelinePath, path -> {
              // Include only the meta files with extensions that needs to be included
              String extension = timelineLayout.getInstantFileNameParser().getTimelineFileExtension(path.getName());
              return includedExtensions.contains(extension);
            }).stream().map(instantGenerator::createNewInstant);

    if (applyLayoutVersionFilters) {
      instantStream = TimelineLayout.fromVersion(getTimelineLayoutVersion()).filterHoodieInstants(instantStream);
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
                                                     RecordMergeMode recordMergeMode, String payloadClassName, String recordMergerStrategy,
                                                     HoodieTimeGeneratorConfig timeGeneratorConfig, FileSystemRetryConfig fileSystemRetryConfig,
                                                     HoodieMetaserverConfig metaserverConfig) {
    if (metaserverConfig.isMetaserverEnabled()) {
      return (HoodieTableMetaClient) ReflectionUtils.loadClass("org.apache.hudi.common.table.HoodieTableMetaserverClient",
          new Class<?>[] {HoodieStorage.class, String.class, ConsistencyGuardConfig.class, RecordMergeMode.class, String.class, String.class, HoodieTimeGeneratorConfig.class,
              FileSystemRetryConfig.class, Option.class, Option.class, HoodieMetaserverConfig.class},
          storage, basePath, consistencyGuardConfig, recordMergeMode, payloadClassName, recordMergerStrategy, timeGeneratorConfig, fileSystemRetryConfig,
          Option.ofNullable(metaserverConfig.getDatabaseName()), Option.ofNullable(metaserverConfig.getTableName()), metaserverConfig);
    } else {
      return new HoodieTableMetaClient(storage, basePath, loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, recordMergeMode, payloadClassName, recordMergerStrategy,
          timeGeneratorConfig, fileSystemRetryConfig);
    }
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
    private RecordMergeMode recordMergeMode = null;
    private String recordMergerStrategy = null;
    private HoodieTimeGeneratorConfig timeGeneratorConfig = null;
    private ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
    private FileSystemRetryConfig fileSystemRetryConfig = FileSystemRetryConfig.newBuilder().build();
    private HoodieMetaserverConfig metaserverConfig = HoodieMetaserverConfig.newBuilder().build();
    private Option<TimelineLayoutVersion> layoutVersion = Option.empty();

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

    public Builder setRecordMergeMode(RecordMergeMode recordMergeMode) {
      this.recordMergeMode = recordMergeMode;
      return this;
    }

    public Builder setTimeGeneratorConfig(HoodieTimeGeneratorConfig timeGeneratorConfig) {
      this.timeGeneratorConfig = timeGeneratorConfig;
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
      checkArgument(conf != null || storage != null,
          "Storage configuration or HoodieStorage needs to be set to init HoodieTableMetaClient");
      checkArgument(basePath != null, "basePath needs to be set to init HoodieTableMetaClient");
      if (timeGeneratorConfig == null) {
        timeGeneratorConfig = HoodieTimeGeneratorConfig.newBuilder().withPath(basePath).build();
      }
      if (storage == null) {
        storage = getStorage(new StoragePath(basePath), conf, consistencyGuardConfig, fileSystemRetryConfig);
      }
      return newMetaClient(storage, basePath,
          loadActiveTimelineOnLoad, consistencyGuardConfig, layoutVersion, recordMergeMode, payloadClassName,
          recordMergerStrategy, timeGeneratorConfig, fileSystemRetryConfig, metaserverConfig);
    }
  }

  public InstantGenerator getInstantGenerator() {
    return getTimelineLayout().getInstantGenerator();
  }

  public InstantFileNameGenerator getInstantFileNameGenerator() {
    return getTimelineLayout().getInstantFileNameGenerator();
  }

  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp) {
    return getInstantGenerator().createNewInstant(state, action, timestamp);
  }

  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime) {
    return getInstantGenerator().createNewInstant(state, action, timestamp, completionTime);
  }

  public HoodieInstant createNewInstant(HoodieInstant.State state, String action, String timestamp, String completionTime, boolean isLegacy) {
    return getInstantGenerator().createNewInstant(state, action, timestamp, completionTime, isLegacy);
  }

  public HoodieInstant createNewInstant(StoragePathInfo pathInfo) {
    return getInstantGenerator().createNewInstant(pathInfo);
  }

  public InstantFileNameParser getInstantFileNameParser() {
    return getTimelineLayout().getInstantFileNameParser();
  }

  public CommitMetadataSerDe getCommitMetadataSerDe() {
    return getTimelineLayout().getCommitMetadataSerDe();
  }

  public static TableBuilder newTableBuilder() {
    return new TableBuilder();
  }

  /**
   * Builder for {@link Properties}.
   */
  public static class TableBuilder {

    private HoodieTableType tableType;
    private String databaseName;
    private String tableName;
    private String tableCreateSchema;
    private HoodieTableVersion tableVersion;
    private String recordKeyFields;
    private String secondaryKeyFields;
    private String archiveLogFolder;
    private RecordMergeMode recordMergeMode;
    private String payloadClassName;
    private String recordMergerStrategyId;
    private Integer timelineLayoutVersion;
    private String timelinePath;
    private String timelineHistoryPath;
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
    private String keyGeneratorType;
    private Boolean hiveStylePartitioningEnable;
    private Boolean urlEncodePartitioning;
    private HoodieTimelineTimeZone commitTimeZone;
    private Boolean partitionMetafileUseBaseFormat;
    private Boolean shouldDropPartitionColumns;
    private String metadataPartitions;
    private String inflightMetadataPartitions;
    private String secondaryIndexesMetadata;
    private Boolean multipleBaseFileFormatsEnabled;

    private String indexDefinitionPath;

    /**
     * Persist the configs that is written at the first time, and should not be changed.
     * Like KeyGenerator's configs.
     */
    private Properties others = new Properties();

    TableBuilder() {
    }

    public TableBuilder setTableType(HoodieTableType tableType) {
      this.tableType = tableType;
      return this;
    }

    public TableBuilder setTableType(String tableType) {
      return setTableType(HoodieTableType.valueOf(tableType));
    }

    public TableBuilder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public TableBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableBuilder setTableVersion(HoodieTableVersion tableVersion) {
      this.tableVersion = tableVersion;
      // TimelineLayoutVersion is an internal setting which will be consistent with table version.
      setTimelineLayoutVersion(tableVersion.getTimelineLayoutVersion().getVersion());
      return this;
    }

    public TableBuilder setTimelinePath(String timelinePath) {
      this.timelinePath = timelinePath;
      return this;
    }

    public TableBuilder setTimelineHistoryPath(String timelineHistoryPath) {
      this.timelineHistoryPath = timelineHistoryPath;
      return this;
    }

    public TableBuilder setTableVersion(int tableVersion) {
      return setTableVersion(HoodieTableVersion.fromVersionCode(tableVersion));
    }

    public TableBuilder setTableCreateSchema(String tableCreateSchema) {
      this.tableCreateSchema = tableCreateSchema;
      return this;
    }

    public TableBuilder setRecordKeyFields(String recordKeyFields) {
      this.recordKeyFields = recordKeyFields;
      return this;
    }

    public TableBuilder setSecondaryKeyFields(String secondaryKeyFields) {
      this.secondaryKeyFields = secondaryKeyFields;
      return this;
    }

    public TableBuilder setArchiveLogFolder(String archiveLogFolder) {
      this.archiveLogFolder = archiveLogFolder;
      return this;
    }

    public TableBuilder setRecordMergeMode(RecordMergeMode recordMergeMode) {
      this.recordMergeMode = recordMergeMode;
      return this;
    }

    public TableBuilder setPayloadClassName(String payloadClassName) {
      this.payloadClassName = payloadClassName;
      return this;
    }

    public TableBuilder setRecordMergeStrategyId(String recordMergerStrategyId) {
      this.recordMergerStrategyId = recordMergerStrategyId;
      return this;
    }

    public TableBuilder setPayloadClass(Class<? extends HoodieRecordPayload> payloadClass) {
      return setPayloadClassName(payloadClass.getName());
    }

    public TableBuilder setTimelineLayoutVersion(Integer timelineLayoutVersion) {
      this.timelineLayoutVersion = timelineLayoutVersion;
      return this;
    }

    public TableBuilder setBaseFileFormat(String baseFileFormat) {
      this.baseFileFormat = baseFileFormat;
      return this;
    }

    public TableBuilder setPreCombineField(String preCombineField) {
      this.preCombineField = preCombineField;
      return this;
    }

    public TableBuilder setPartitionFields(String partitionFields) {
      this.partitionFields = partitionFields;
      return this;
    }

    public TableBuilder setCDCEnabled(boolean cdcEnabled) {
      this.cdcEnabled = cdcEnabled;
      return this;
    }

    public TableBuilder setCDCSupplementalLoggingMode(String cdcSupplementalLoggingMode) {
      this.cdcSupplementalLoggingMode = cdcSupplementalLoggingMode.toUpperCase();
      return this;
    }

    public TableBuilder setBootstrapIndexClass(String bootstrapIndexClass) {
      this.bootstrapIndexClass = bootstrapIndexClass;
      return this;
    }

    public TableBuilder setBootstrapBasePath(String bootstrapBasePath) {
      this.bootstrapBasePath = bootstrapBasePath;
      return this;
    }

    public TableBuilder setBootstrapIndexEnable(Boolean bootstrapIndexEnable) {
      this.bootstrapIndexEnable = bootstrapIndexEnable;
      return this;
    }

    public TableBuilder setPopulateMetaFields(boolean populateMetaFields) {
      this.populateMetaFields = populateMetaFields;
      return this;
    }

    public TableBuilder setKeyGeneratorClassProp(String keyGeneratorClassProp) {
      this.keyGeneratorClassProp = keyGeneratorClassProp;
      return this;
    }

    public TableBuilder setKeyGeneratorType(String keyGeneratorType) {
      this.keyGeneratorType = keyGeneratorType;
      return this;
    }

    public TableBuilder setHiveStylePartitioningEnable(Boolean hiveStylePartitioningEnable) {
      this.hiveStylePartitioningEnable = hiveStylePartitioningEnable;
      return this;
    }

    public TableBuilder setUrlEncodePartitioning(Boolean urlEncodePartitioning) {
      this.urlEncodePartitioning = urlEncodePartitioning;
      return this;
    }

    public TableBuilder setCommitTimezone(HoodieTimelineTimeZone timelineTimeZone) {
      this.commitTimeZone = timelineTimeZone;
      return this;
    }

    public TableBuilder setPartitionMetafileUseBaseFormat(Boolean useBaseFormat) {
      this.partitionMetafileUseBaseFormat = useBaseFormat;
      return this;
    }

    public TableBuilder setShouldDropPartitionColumns(Boolean shouldDropPartitionColumns) {
      this.shouldDropPartitionColumns = shouldDropPartitionColumns;
      return this;
    }

    public TableBuilder setMetadataPartitions(String partitions) {
      this.metadataPartitions = partitions;
      return this;
    }

    public TableBuilder setInflightMetadataPartitions(String partitions) {
      this.inflightMetadataPartitions = partitions;
      return this;
    }

    public TableBuilder setSecondaryIndexesMetadata(String secondaryIndexesMetadata) {
      this.secondaryIndexesMetadata = secondaryIndexesMetadata;
      return this;
    }

    public TableBuilder setMultipleBaseFileFormatsEnabled(Boolean multipleBaseFileFormatsEnabled) {
      this.multipleBaseFileFormatsEnabled = multipleBaseFileFormatsEnabled;
      return this;
    }

    public TableBuilder setIndexDefinitionPath(String indexDefinitionPath) {
      this.indexDefinitionPath = indexDefinitionPath;
      return this;
    }

    public TableBuilder set(Map<String, Object> props) {
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

    public TableBuilder fromMetaClient(HoodieTableMetaClient metaClient) {
      return setTableType(metaClient.getTableType())
          .setTableName(metaClient.getTableConfig().getTableName())
          .setTableVersion(metaClient.getTableConfig().getTableVersion())
          .setTimelinePath(metaClient.getTableConfig().getTimelinePath())
          .setArchiveLogFolder(metaClient.getTableConfig().getTimelineHistoryPath())
          .setRecordMergeMode(metaClient.getTableConfig().getRecordMergeMode())
          .setPayloadClassName(metaClient.getTableConfig().getPayloadClass())
          .setRecordMergeStrategyId(metaClient.getTableConfig().getRecordMergeStrategyId());
    }

    public TableBuilder fromProperties(Properties properties) {
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

      if (hoodieConfig.contains(VERSION)) {
        setTableVersion(hoodieConfig.getInt(VERSION));
      }

      if (hoodieConfig.contains(TIMELINE_PATH)) {
        setTimelinePath(hoodieConfig.getString(TIMELINE_PATH));
      }

      if (hoodieConfig.contains(HoodieTableConfig.TIMELINE_HISTORY_PATH)) {
        setTimelineHistoryPath(hoodieConfig.getString(HoodieTableConfig.TIMELINE_HISTORY_PATH));
      }

      if (hoodieConfig.contains(HoodieTableConfig.TYPE)) {
        setTableType(hoodieConfig.getString(HoodieTableConfig.TYPE));
      }
      if (hoodieConfig.contains(HoodieTableConfig.ARCHIVELOG_FOLDER)) {
        setArchiveLogFolder(
            hoodieConfig.getString(HoodieTableConfig.ARCHIVELOG_FOLDER));
      }
      if (hoodieConfig.contains(HoodieTableConfig.RECORD_MERGE_MODE)) {
        setRecordMergeMode(
            RecordMergeMode.getValue(hoodieConfig.getString(HoodieTableConfig.RECORD_MERGE_MODE)));
      }
      if (hoodieConfig.contains(HoodieTableConfig.PAYLOAD_CLASS_NAME)) {
        setPayloadClassName(hoodieConfig.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME));
      }
      if (hoodieConfig.contains(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID)) {
        setRecordMergeStrategyId(
            hoodieConfig.getString(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID));
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
      if (hoodieConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE)) {
        setBootstrapIndexClass(BootstrapIndexType.valueOf(hoodieConfig.getString(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE)).getClassName());
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
      } else if (hoodieConfig.contains(HoodieTableConfig.KEY_GENERATOR_TYPE)) {
        setKeyGeneratorClassProp(KeyGeneratorType.valueOf(hoodieConfig.getString(HoodieTableConfig.KEY_GENERATOR_TYPE)).getClassName());
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
      if (hoodieConfig.contains(HoodieTableConfig.MULTIPLE_BASE_FILE_FORMATS_ENABLE)) {
        setMultipleBaseFileFormatsEnabled(hoodieConfig.getBoolean(HoodieTableConfig.MULTIPLE_BASE_FILE_FORMATS_ENABLE));
      }
      if (hoodieConfig.contains(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH)) {
        setIndexDefinitionPath(hoodieConfig.getString(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH));
      }
      return this;
    }

    @VisibleForTesting
    public Properties build() {
      checkArgument(tableType != null, "tableType is null");
      checkArgument(tableName != null, "tableName is null");

      HoodieTableConfig tableConfig = new HoodieTableConfig();

      tableConfig.setAll(others);

      if (databaseName != null) {
        tableConfig.setValue(HoodieTableConfig.DATABASE_NAME, databaseName);
      }
      tableConfig.setValue(HoodieTableConfig.NAME, tableName);
      tableConfig.setValue(HoodieTableConfig.TYPE, tableType.name());

      if (null != tableVersion) {
        tableConfig.setTableVersion(tableVersion);
        tableConfig.setInitialVersion(tableVersion);
      } else {
        tableConfig.setTableVersion(HoodieTableVersion.current());
        tableConfig.setInitialVersion(HoodieTableVersion.current());
      }

      Triple<RecordMergeMode, String, String> mergeConfigs =
          HoodieTableConfig.inferCorrectMergingBehavior(recordMergeMode, payloadClassName, recordMergerStrategyId);
      tableConfig.setValue(RECORD_MERGE_MODE, mergeConfigs.getLeft().name());
      tableConfig.setValue(PAYLOAD_CLASS_NAME.key(), mergeConfigs.getMiddle());
      tableConfig.setValue(RECORD_MERGE_STRATEGY_ID, mergeConfigs.getRight());

      if (null != tableCreateSchema) {
        tableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA, tableCreateSchema);
      }

      if (!StringUtils.isNullOrEmpty(archiveLogFolder)) {
        tableConfig.setValue(HoodieTableConfig.ARCHIVELOG_FOLDER, archiveLogFolder);
      } else {
        tableConfig.setDefaultValue(HoodieTableConfig.ARCHIVELOG_FOLDER);
      }

      if (!StringUtils.isNullOrEmpty(timelinePath)) {
        tableConfig.setValue(HoodieTableConfig.TIMELINE_PATH, timelinePath);
      } else {
        tableConfig.setDefaultValue(HoodieTableConfig.TIMELINE_PATH);
      }

      if (!StringUtils.isNullOrEmpty(timelineHistoryPath)) {
        tableConfig.setValue(HoodieTableConfig.TIMELINE_HISTORY_PATH, timelineHistoryPath);
      } else {
        tableConfig.setDefaultValue(HoodieTableConfig.TIMELINE_HISTORY_PATH);
      }

      if (null != timelineLayoutVersion) {
        tableConfig.setValue(HoodieTableConfig.TIMELINE_LAYOUT_VERSION,
            String.valueOf(timelineLayoutVersion));
      }

      if (null != baseFileFormat) {
        tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, baseFileFormat.toUpperCase());
      }

      if (null != bootstrapIndexClass) {
        tableConfig.setValue(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE, BootstrapIndexType.fromClassName(bootstrapIndexClass).name());
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
        tableConfig.setValue(HoodieTableConfig.KEY_GENERATOR_TYPE, KeyGeneratorType.fromClassName(keyGeneratorClassProp).name());
      } else if (null != keyGeneratorType) {
        tableConfig.setValue(HoodieTableConfig.KEY_GENERATOR_TYPE, keyGeneratorType);
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
      if (null != multipleBaseFileFormatsEnabled) {
        tableConfig.setValue(HoodieTableConfig.MULTIPLE_BASE_FILE_FORMATS_ENABLE, Boolean.toString(multipleBaseFileFormatsEnabled));
      }
      if (null != indexDefinitionPath) {
        tableConfig.setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH, indexDefinitionPath);
      }
      return tableConfig.getProps();
    }

    public HoodieTableMetaClient initTable(StorageConfiguration<?> configuration, String basePath) throws IOException {
      return initTable(configuration, new StoragePath(basePath));
    }

    public HoodieTableMetaClient initTable(StorageConfiguration<?> storageConf, StoragePath basePath) throws IOException {
      Properties props = build();
      createTableLayoutOnStorage(storageConf, basePath, props, timelineLayoutVersion, true);
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath)
          .setMetaserverConfig(props)
          .build();
      LOG.info("Finished initializing Table of type {} from {}", metaClient.getTableConfig().getTableType(), basePath);
      return metaClient;
    }
  }
}
