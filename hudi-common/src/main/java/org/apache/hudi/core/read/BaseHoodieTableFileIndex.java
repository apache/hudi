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

package org.apache.hudi.core.read;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.expression.Expression;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.serialization.HoodieFileSliceSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.HOODIE_FILE_INDEX_MAX_MEMORY;
import static org.apache.hudi.common.config.HoodieCommonConfig.HOODIE_FILE_INDEX_SPILLABLE_MEMORY;
import static org.apache.hudi.common.config.HoodieCommonConfig.HOODIE_FILE_INDEX_USE_SPILLABLE_MAP;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
import static org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.START_COMMIT_EARLIEST;
import static org.apache.hudi.common.table.timeline.TimelineUtils.validateTimestampAsOf;

/**
 * Common (engine-agnostic) File Index implementation enabling individual query engines to
 * list Hudi Table contents based on the
 *
 * <ul>
 *   <li>Table type (MOR, COW)</li>
 *   <li>Query type (snapshot, read_optimized, incremental)</li>
 *   <li>Query instant/range</li>
 * </ul>
 */
@Slf4j
public abstract class BaseHoodieTableFileIndex implements AutoCloseable {

  // NOTE: Constants of the structural weigher used for HOODIE_FILE_INDEX_MAX_MEMORY; see
  //       [[estimateFileSliceCacheWeight]] for how they were calibrated.
  private static final int FILE_SLICE_CACHE_ENTRY_OVERHEAD_BYTES = 256;
  private static final int FILE_SLICE_CACHE_BYTES_PER_FILE = 960;
  // NOTE: The PartitionPath key holds the partition path once, as a String.
  private static final int FILE_SLICE_CACHE_KEY_BYTES_PER_PATH_CHAR = 2;
  // NOTE: Every described file holds its own full path several times over -- as the file object's own path String,
  //       and inside its StoragePathInfo's StoragePath, whose URI keeps the parsed components and whose toString is
  //       cached. Measured at ~5.4 bytes per character of the full path; charged at 6 so that the error from a long
  //       path runs towards over-charging.
  private static final int FILE_SLICE_CACHE_BYTES_PER_PATH_CHAR = 6;

  @Getter(AccessLevel.PROTECTED)
  private final String[] partitionColumns;

  protected final HoodieMetadataConfig metadataConfig;
  private final TypedProperties configProperties;
  @Getter
  private final HoodieTableQueryType queryType;
  private final Option<String> specifiedQueryInstant;
  private final Option<String> incrementalQueryStartTime;
  private final Option<String> incrementalQueryEndTime;
  @Getter(AccessLevel.PROTECTED)
  private final List<StoragePath> queryPaths;

  protected final boolean shouldIncludePendingCommits;
  private final boolean shouldValidateInstant;
  protected final boolean useLatestBaseFilesPathFilterForListing;

  // The `shouldListLazily` variable controls how we initialize/refresh the TableFileIndex:
  //  - non-lazy/eager listing (shouldListLazily=false):  all partitions and file slices will be loaded eagerly during initialization.
  //  - lazy listing (shouldListLazily=true): partitions listing will be done lazily with the knowledge from query predicate on partition
  //        columns. And file slices fetching only happens for partitions satisfying the given filter.
  //
  // In SparkSQL, `shouldListLazily` is controlled by `HoodieFileIndex`'s listing mode, i.e. option
  // `hoodie.datasource.read.file.index.listing.mode` (`DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE`).
  // In lazy listing case, if no predicate on partition is provided, all partitions will still be loaded.
  private final boolean shouldListLazily;

  @Getter
  private final StoragePath basePath;

  private final HoodieTableMetaClient metaClient;
  private final String tableName;
  private final HoodieEngineContext engineContext;
  private final boolean isCompletionTimeBasedQuery;

  private final transient FileStatusCache fileStatusCache;

  // NOTE: Individual partitions are always cached in full: meaning that if partition is cached
  //       it will hold all the file-slices residing w/in the partition.
  //
  //       Unless the spillable map is in use this is the map view of [[fileSliceCache]], i.e. a Caffeine cache, and
  //       is therefore a genuinely thread-safe ConcurrentMap. In the lazy-listing case its retention is additionally
  //       bounded by [[HOODIE_FILE_INDEX_MAX_MEMORY]], because there the resident set is an accumulation of the
  //       working sets of every query this index has ever served and the index itself can live for the whole session.
  //       In the eager-listing case the resident set is, by construction, exactly the table's partitions as of the
  //       last refresh, so it does not grow with the session and is left unbounded.
  private transient volatile Map<PartitionPath, List<FileSlice>> cachedAllInputFileSlices = new HashMap<>();

  // NOTE: The Caffeine cache that [[cachedAllInputFileSlices]] is the [[Cache#asMap]] view of. Nothing in production
  //       reads it -- all use goes through that view -- but the view deliberately exposes no way back to the cache's
  //       own API, which is what the tests need in order to assert on the eviction policy and its weighted size.
  //       Null until the first refresh, and stale -- i.e. still referring to the previous Caffeine cache -- once a
  //       refresh has switched this index over to the spillable map, since that branch replaces
  //       [[cachedAllInputFileSlices]] without touching this field. So it is only meaningful alongside
  //       [[cachedAllInputFileSlices]], never on its own.
  @VisibleForTesting
  private transient volatile Cache<PartitionPath, List<FileSlice>> fileSliceCache = null;

  // NOTE: The total size (in bytes) of the files of each partition this index has listed since its last refresh,
  //       keyed by relative partition path. This is deliberately NOT derived from [[cachedAllInputFileSlices]]: that
  //       map is bounded and evicts, whereas [[getTotalCachedFilesSize]] backs Spark's HoodieFileIndex.sizeInBytes
  //       and through it spark.sql.autoBroadcastJoinThreshold -- so a figure that shrank when a partition happened to
  //       be evicted would let a huge table be broadcast, and would differ between two planning calls for the same
  //       query.
  //
  //       This ledger is intentionally left unbounded, since bounding it is precisely what would make the figure
  //       wrong again. Its cost is therefore worth stating plainly rather than waving away: it is O(distinct
  //       partitions listed since the last refresh) -- [[doRefresh]] replaces it outright -- and holds one boxed long
  //       per partition plus that partition's ConcurrentHashMap node and key reference. Measured with
  //       ObjectSizeCalculator over a real ConcurrentHashMap, that is ~110 bytes per partition for short local
  //       partition paths ("dt=1") and ~174 bytes for 66-character object-store paths: 1.05 MiB and 1.66 MiB at
  //       10000 partitions, 114 MiB and 168 MiB at 1000000. So it is flat in how wide the table is -- against 4.4 MiB
  //       and 173.7 MiB for the file-slice lists of those same 10000 partitions at one and at a hundred file groups
  //       each -- but it does scale with how long the partition paths are, and it is not negligible for a table with
  //       millions of partitions.
  //
  //       The key is the same String instance the caller's PartitionPath holds, so in the eager-listing path it costs
  //       only the reference. In the lazy + partition-predicate path, though (see SparkHoodieTableFileIndex's pruning
  //       of `cachedAllPartitionPaths`), the caller's PartitionPath may be dropped while this ledger keeps its entry,
  //       and the ledger then becomes the sole holder of that String.
  private transient volatile Map<String, Long> cachedPartitionFileSizes = new ConcurrentHashMap<>();

  // NOTE: It always contains either all partition paths, or null if it is not initialized yet
  private transient volatile List<PartitionPath> cachedAllPartitionPaths = null;

  private transient HoodieTableMetadata tableMetadata = null;

  /**
   * @param engineContext                                Hudi engine-specific context
   * @param metaClient                                  Hudi table's meta-client
   * @param configProperties                            unifying configuration (in the form of generic properties)
   * @param queryType                                   target query type
   * @param queryPaths                                  target DFS paths being queried
   * @param useLatestBaseFilesPathFilterForListing      memory optimization on the driver while fetching read optimized results
   * @param specifiedQueryInstant                       instant as of which table is being queried
   * @param shouldIncludePendingCommits                 flags whether file-index should exclude any pending operations
   * @param shouldValidateInstant                       flags to validate whether query instant is present in the timeline
   * @param fileStatusCache                             transient cache of fetched [[FileStatus]]es
   * @param incrementalQueryStartTime                   start completion time for incremental query (optional)
   * @param incrementalQueryEndTime                     end completion time for incremental query (optional)
   */
  public BaseHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  HoodieTableQueryType queryType,
                                  List<StoragePath> queryPaths,
                                  Option<String> specifiedQueryInstant,
                                  boolean useLatestBaseFilesPathFilterForListing,
                                  boolean shouldIncludePendingCommits,
                                  boolean shouldValidateInstant,
                                  FileStatusCache fileStatusCache,
                                  boolean shouldListLazily,
                                  Option<String> incrementalQueryStartTime,
                                  Option<String> incrementalQueryEndTime) {
    this.partitionColumns = metaClient.getTableConfig().getPartitionFields()
        .orElseGet(() -> new String[0]);

    // Disable metadata when ro_path_filter is enabled.
    this.metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(configProperties)
        .enable(configProperties.getBoolean(ENABLE.key(), DEFAULT_METADATA_ENABLE_FOR_READERS)
            && HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient)
            && !useLatestBaseFilesPathFilterForListing)
        .build();

    this.queryType = queryType;
    this.queryPaths = queryPaths;
    this.useLatestBaseFilesPathFilterForListing = useLatestBaseFilesPathFilterForListing;
    this.specifiedQueryInstant = specifiedQueryInstant;
    this.shouldIncludePendingCommits = shouldIncludePendingCommits;
    this.shouldValidateInstant = shouldValidateInstant;
    this.shouldListLazily = shouldListLazily;
    this.incrementalQueryStartTime = incrementalQueryStartTime;
    this.incrementalQueryEndTime = incrementalQueryEndTime;

    this.basePath = metaClient.getBasePath();

    this.metaClient = metaClient;
    this.tableName = metaClient.getTableConfig().getTableName();
    this.engineContext = engineContext;
    this.fileStatusCache = fileStatusCache;
    this.configProperties = configProperties;
    HoodieTableVersion tableVersion = HoodieTableVersion.fromVersionCode(configProperties.getInteger("hoodie.datasource.read.incr.table.version",
        metaClient.getTableConfig().getTableVersion().versionCode()));
    this.isCompletionTimeBasedQuery = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT);
    doRefresh();
  }

  protected abstract Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath);

  /**
   * Returns latest completed instant as seen by this instance of the file-index
   */
  public Option<HoodieInstant> getLatestCompletedInstant() {
    return getActiveTimeline().filterCompletedInstants().lastInstant();
  }

  public int getFileSlicesCount() {
    return getAllInputFileSlices().values().stream()
        .mapToInt(List::size).sum();
  }

  @Override
  public void close() throws Exception {
    resetTableMetadata(null);
  }

  /**
   * Returns all partition paths matching the ones explicitly provided by the query (if any)
   */
  public List<PartitionPath> getAllQueryPartitionPaths() {
    if (cachedAllPartitionPaths == null) {
      List<String> queryRelativePartitionPaths = queryPaths.stream()
          .map(path -> FSUtils.getRelativePartitionPath(basePath, path))
          .collect(Collectors.toList());

      this.cachedAllPartitionPaths = listPartitionPaths(queryRelativePartitionPaths);
    }

    return cachedAllPartitionPaths;
  }

  /**
   * Returns all listed file-slices w/in the partition paths returned by {@link #getAllQueryPartitionPaths()}
   */
  protected Map<PartitionPath, List<FileSlice>> getAllInputFileSlices() {
    // NOTE: We always hand out a freshly assembled snapshot rather than the cache itself: the cache is mutated (and
    //       evicted from) by every subsequent query, so returning it would alias the caller's result to live state.
    return getInputFileSlices(getAllQueryPartitionPaths().toArray(new PartitionPath[0]));
  }

  /**
   * Get input file slices for the given partitions. Partitions that are already cached are served from the cache;
   * the rest are listed and then cached.
   *
   * <p>The returned map always holds an entry for every requested partition. Note that it is assembled from values
   * this method is already holding on to, and never by reading the cache back after loading: the cache is bounded,
   * so an entry may be evicted at any point after it has been read or inserted -- including by this very call, when
   * a single query's working set exceeds the bound.
   */
  protected Map<PartitionPath, List<FileSlice>> getInputFileSlices(PartitionPath... partitions) {
    Map<PartitionPath, List<FileSlice>> queriedFileSlices = new HashMap<>(partitions.length);
    List<PartitionPath> missingPartitions = new ArrayList<>();
    for (PartitionPath partition : partitions) {
      // NOTE: Individual partitions are always cached in full, therefore if a partition is cached it holds all the
      //       file-slices residing w/in it
      List<FileSlice> cachedFileSlices = cachedAllInputFileSlices.get(partition);
      if (cachedFileSlices != null) {
        queriedFileSlices.put(partition, cachedFileSlices);
      } else {
        missingPartitions.add(partition);
      }
    }

    if (!missingPartitions.isEmpty()) {
      // NOTE: Everything still missing is listed in a single call. Listing (and caching) the missing partitions one
      //       at a time would let a query whose working set exceeds the cache's bound evict its own earlier
      //       partitions and then re-list them, so that every repetition of such a query degenerated into one bulk
      //       listing followed by a storm of single-partition metadata lookups -- permanently, since the same thing
      //       happens again on the next repetition.
      Map<PartitionPath, List<FileSlice>> loadedFileSlices = loadFileSlicesForPartitions(missingPartitions);
      for (PartitionPath partition : missingPartitions) {
        List<FileSlice> fileSlices = loadedFileSlices.getOrDefault(partition, Collections.emptyList());
        queriedFileSlices.put(partition, fileSlices);
        cachedAllInputFileSlices.put(partition, fileSlices);
        cachedPartitionFileSizes.put(partition.getPath(),
            fileSlices.stream().mapToLong(BaseHoodieTableFileIndex::fileSliceSize).sum());
      }
    }

    return queriedFileSlices;
  }

  private Map<PartitionPath, List<FileSlice>> loadFileSlicesForPartitions(List<PartitionPath> partitions) {
    if (partitions.isEmpty()) {
      return Collections.emptyMap();
    }

    if (specifiedQueryInstant.isPresent() && !shouldIncludePendingCommits) {
      validateTimestampAsOf(metaClient, specifiedQueryInstant.get());
    }

    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<HoodieInstant> latestInstant = activeTimeline.lastInstant();
    Option<String> queryInstant = specifiedQueryInstant.or(() -> latestInstant.map(HoodieInstant::requestedTime));
    validate(activeTimeline, queryInstant);

    HoodieTimer timer = HoodieTimer.start();
    List<StoragePathInfo> allFiles = listPartitionPathFiles(partitions, activeTimeline);
    long elapsedMs = timer.endTimer();
    log.info("[table={}] HoodieFileIndex.listPartitionPathFiles took {} ms ({} partitions, queryInstant={})",
        tableName, elapsedMs, partitions.size(),
        queryInstant.orElse("N/A"));

    // ROPathFilter optimization is only applicable for COW tables with snapshot queries
    // For MOR tables with READ_OPTIMIZED queries, we also only need base files
    if (useLatestBaseFilesPathFilterForListing
        && !shouldIncludePendingCommits
        && (metaClient.getTableConfig().getTableType() == HoodieTableType.COPY_ON_WRITE
            || queryType == HoodieTableQueryType.READ_OPTIMIZED)) {
      return generatePartitionFileSlicesPostROTablePathFilter(partitions, allFiles);
    }
    return filterFiles(partitions, activeTimeline, allFiles, queryInstant);
  }

  /**
   * Generates FileSlices from the filtered files returned by ROPathFilter.
   * This is a fast path that avoids constructing a full HoodieTableFileSystemView.
   * Only applicable for COW tables since ROPathFilter only returns base files.
   *
   * @param partitions List of partitions to process
   * @param allFiles   Files already filtered by ROPathFilter
   * @return Map of PartitionPath to list of FileSlices
   */
  private Map<PartitionPath, List<FileSlice>> generatePartitionFileSlicesPostROTablePathFilter(
      List<PartitionPath> partitions, List<StoragePathInfo> allFiles) {
    // Group files by partition path, then by file group ID
    Map<String, PartitionPath> partitionsMap = new HashMap<>();
    partitions.forEach(p -> partitionsMap.put(p.path, p));
    Map<PartitionPath, List<FileSlice>> partitionToFileSlices = new HashMap<>();
    // Pre-populate so partitions with no files still appear in the result map.
    // Without this, the caller's Collectors.toMap(identity, cache::get) NPEs on empty partitions
    // because cache.get returns null and toMap rejects null values. This matches the contract
    // already honored by filterFiles, which iterates over partitions rather than over files.
    partitions.forEach(p -> partitionToFileSlices.put(p, Collections.emptyList()));

    for (StoragePathInfo pathInfo : allFiles) {
      // Create FileSlice obj from StoragePathInfo.
      String relPartitionPath = FSUtils.getRelativePartitionPath(basePath, pathInfo.getPath().getParent());
      HoodieBaseFile baseFile = new HoodieBaseFile(pathInfo);
      // Use relative partition path for FileSlice - consistent with HoodieTableFileSystemView
      FileSlice fileSlice = new FileSlice(relPartitionPath, baseFile.getCommitTime(), baseFile.getFileId());
      fileSlice.setBaseFile(baseFile);

      // Add the FileSlice to partitionToFileSlices
      PartitionPath partitionPathObj = partitionsMap.get(relPartitionPath);
      if (partitionPathObj != null) {
        List<FileSlice> fileSlices = partitionToFileSlices.get(partitionPathObj);
        if (fileSlices.isEmpty()) {
          fileSlices = new ArrayList<>();
          partitionToFileSlices.put(partitionPathObj, fileSlices);
        }
        fileSlices.add(fileSlice);
      } else {
        log.warn("Could not find partition path object for relative path: {}. Skipping file: {}",
            relPartitionPath, pathInfo.getPath());
      }
    }
    return partitionToFileSlices;
  }

  private Map<PartitionPath, List<FileSlice>> filterFiles(List<PartitionPath> partitions,
                                                                            HoodieTimeline activeTimeline,
                                                                            List<StoragePathInfo> allFiles,
                                                                            Option<String> queryInstant) {
    HoodieTimer timer = HoodieTimer.start();
    try (HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles)) {
      // NOTE: For MOR table, when the compaction is inflight, we need to not only fetch the
      // latest slices, but also include the base and log files of the second-last version of
      // the file slice in the same file group as the latest file slice that is under compaction.
      // This logic is realized by `AbstractTableFileSystemView::getLatestMergedFileSlicesBeforeOrOn`
      // API.  Note that for COW table, the merging logic of two slices does not happen as there
      // is no compaction, thus there is no performance impact.
      HoodieTableFileSystemView finalFileSystemView = fileSystemView;
      return partitions.stream().collect(
          Collectors.toMap(
              Function.identity(),
              partitionPath ->
                  queryInstant.map(instant ->
                          finalFileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath.path, queryInstant.get())
                      )
                      .orElseGet(() -> finalFileSystemView.getLatestFileSlices(partitionPath.path))
                      .collect(Collectors.toList())
          ));
    } finally {
      long elapsedMs = timer.endTimer();
      log.debug("[table={}] HoodieFileIndex.filterFiles took {} ms ({} partitions, {} files, queryInstant={})",
          tableName, elapsedMs,
          partitions.size(), allFiles.size(), queryInstant.orElse("N/A"));
    }
  }

  protected List<PartitionPath> listPartitionPaths(List<String> relativePartitionPaths,
                                                   Types.RecordType partitionFields,
                                                   Expression partitionColumnPredicates,
                                                   List<Object> partitionPredicateExpressions) {
    List<String> matchedPartitionPaths;
    HoodieTimer timer = HoodieTimer.start();
    try {
      matchedPartitionPaths = tableMetadata.getPartitionPathWithPathPrefixUsingFilterExpression(relativePartitionPaths,
          partitionFields, partitionColumnPredicates, partitionPredicateExpressions);
    } catch (IOException e) {
      throw new HoodieIOException("On " + tableName + " Error fetching partition paths", e);
    } finally {
      long elapsedMs = timer.endTimer();
      log.debug("[table={}] HoodieFileIndex.listPartitionPaths took {} ms ({} partition paths, with predicate filtering)",
          tableName, elapsedMs, relativePartitionPaths.size());
    }

    // Convert partition's path into partition descriptor
    return matchedPartitionPaths.stream()
        .map(this::convertToPartitionPath)
        .collect(Collectors.toList());
  }

  protected List<PartitionPath> listPartitionPaths(List<String> relativePartitionPaths) {
    List<String> matchedPartitionPaths;
    try {
      if (isPartitionedTable()) {
        if (queryType == HoodieTableQueryType.INCREMENTAL
            && incrementalQueryStartTime.isPresent()
            && !START_COMMIT_EARLIEST.equalsIgnoreCase(incrementalQueryStartTime.get())
            && !isBeforeTimelineStarts()) {
          HoodieTimeline timelineToQuery = findInstantsInRange();
          matchedPartitionPaths = TimelineUtils.getWrittenPartitions(timelineToQuery);
        } else {
          HoodieTimer timer = HoodieTimer.start();
          try {
            matchedPartitionPaths = tableMetadata.getPartitionPathWithPathPrefixes(relativePartitionPaths);
          } finally {
            long elapsedMs = timer.endTimer();
            log.debug("[table={}] HoodieFileIndex.listPartitionPaths took {} ms ({} partition paths)",
                tableName, elapsedMs, relativePartitionPaths.size());
          }
        }
      } else {
        matchedPartitionPaths = Collections.singletonList(StringUtils.EMPTY_STRING);
      }
    } catch (IOException e) {
      throw new HoodieIOException("On " + tableName + " Error fetching partition paths", e);
    }

    // Convert partition's path into partition descriptor
    return matchedPartitionPaths.stream()
        .map(this::convertToPartitionPath)
        .collect(Collectors.toList());
  }

  private boolean isBeforeTimelineStarts() {
    if (isCompletionTimeBasedQuery) {
      return metaClient.getActiveTimeline().isBeforeTimelineStartsByCompletionTime(incrementalQueryStartTime.get());
    } else {
      return metaClient.getActiveTimeline().isBeforeTimelineStarts(incrementalQueryStartTime.get());
    }
  }

  private HoodieTimeline findInstantsInRange() {
    if (isCompletionTimeBasedQuery) {
      return metaClient.getActiveTimeline().getWriteTimeline()
          .findInstantsInRangeByCompletionTime(incrementalQueryStartTime.get(), incrementalQueryEndTime.orElse(String.valueOf(Long.MAX_VALUE)));
    } else {
      return metaClient.getActiveTimeline().getWriteTimeline()
          .findInstantsInRange(incrementalQueryStartTime.get(), incrementalQueryEndTime.orElse(String.valueOf(Long.MAX_VALUE)));
    }
  }

  protected void refresh() {
    fileStatusCache.invalidate();
    doRefresh();
  }

  private boolean isPartitionedTable() {
    return partitionColumns.length > 0 || HoodieTableMetadata.isMetadataTable(basePath);
  }

  protected HoodieTimeline getActiveTimeline() {
    // NOTE: We have to use commits and compactions timeline, to make sure that we're properly
    //       handling the following case: when records are inserted into the new log-file w/in the file-group
    //       that is under the pending compaction process, new log-file will bear the compaction's instant (on the
    //       timeline) in its name, as opposed to the base-file's commit instant. To make sure we're not filtering
    //       such log-file we have to _always_ include pending compaction instants into consideration
    // TODO(HUDI-3302) re-evaluate whether we should filter any commits in here
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline();
    if (shouldIncludePendingCommits) {
      return timeline;
    } else {
      return timeline.filterCompletedAndCompactionInstants();
    }
  }

  private Object[] getPartitionColumnValues(String[] partitionColumns, String partitionPath) {
    Object[] partitionColumnValues = parsePartitionColumnValues(partitionColumns, partitionPath);
    if (shouldListLazily && partitionColumnValues.length != partitionColumns.length) {
      throw new HoodieException("Failed to parse partition column values from the partition-path:"
          + " likely non-encoded slashes being used in partition column's values. You can try to"
          + " work this around by switching listing mode to eager");
    }

    return partitionColumnValues;
  }

  /**
   * Load partition paths and it's files under the query table path.
   */
  private List<StoragePathInfo> listPartitionPathFiles(List<PartitionPath> partitions,
                                                       HoodieTimeline activeTimeline) {
    List<StoragePath> partitionPaths = partitions.stream()
        // NOTE: We're using [[createPathUnsafe]] to create Hadoop's [[Path]] objects
        //       instances more efficiently, provided that
        //          - We're using already normalized relative paths
        //          - Its scope limited to [[FileStatusCache]]
        .map(partition -> new StoragePath(partition.path))
        .collect(Collectors.toList());

    // Lookup in cache first
    Map<StoragePath, List<StoragePathInfo>> cachedPartitionPaths =
        partitionPaths.parallelStream()
            .map(partitionPath -> Pair.of(partitionPath, fileStatusCache.get(partitionPath)))
            .filter(partitionPathFilesPair -> partitionPathFilesPair.getRight().isPresent())
            .collect(Collectors.toMap(Pair::getKey, p -> p.getRight().get()));

    Set<StoragePath> missingPartitionPaths =
        CollectionUtils.diffSet(partitionPaths, cachedPartitionPaths.keySet());

    if (missingPartitionPaths.isEmpty()) {
      return cachedPartitionPaths.values().stream()
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }

    // NOTE: We're constructing a mapping of absolute form of the partition-path into
    //       its relative one, such that we don't need to reconstruct these again later on
    Map<String, StoragePath> missingPartitionPathsMap = missingPartitionPaths.stream()
        .collect(Collectors.toMap(
            relativePartitionPath -> new StoragePath(basePath, relativePartitionPath.toString()).toString(),
            Function.identity()
        ));

    HoodieTimer timer = HoodieTimer.start();
    try {
      log.debug("[table={}] HoodieFileIndex.listPartitionPathFiles cache miss: {} of {} partitions uncached, loading them.",
          tableName, missingPartitionPaths.size(), partitionPaths.size());
      Map<String, List<StoragePathInfo>> fetchedPartitionsMap;
      fetchedPartitionsMap = tableMetadata.getAllFilesInPartitions(
          missingPartitionPathsMap.keySet(), getPartitionPathFilter(activeTimeline));

      // Ingest newly fetched partitions into cache
      fetchedPartitionsMap.forEach((absolutePath, files) -> {
        StoragePath relativePath = missingPartitionPathsMap.get(absolutePath);
        fileStatusCache.put(relativePath, files);
      });

      List<StoragePathInfo> result = new ArrayList<>();
      result.addAll(cachedPartitionPaths.values().stream()
          .flatMap(e -> e.stream()).collect(Collectors.toList()));
      result.addAll(fetchedPartitionsMap.values().stream()
          .flatMap(e -> e.stream()).collect(Collectors.toList()));

      return result;
    } catch (IOException e) {
      throw new HoodieIOException("On " + tableName + " Failed to list partition paths", e);
    } finally {
      long elapsedMs = timer.endTimer();
      log.debug("[table={}] HoodieFileIndex.getAllFilesInPartitions took {} ms ({} uncached partitions)",
          tableName, elapsedMs, missingPartitionPaths.size());
    }
  }

  protected Option<StoragePathFilter> getPartitionPathFilter(HoodieTimeline activeTimeline) {
    return Option.empty();
  }

  private void doRefresh() {
    HoodieTimer timer = HoodieTimer.start();

    resetTableMetadata(createMetadataTable(engineContext));

    // Make sure we reload active timeline
    metaClient.reloadActiveTimeline();

    // Reset it to null to trigger re-loading of all partition path
    this.cachedAllPartitionPaths = null;
    // Reset to force reload file slices inside partitions
    try {
      if (ConfigUtils.getBooleanWithAltKeys(configProperties, HOODIE_FILE_INDEX_USE_SPILLABLE_MAP)) {
        long spillableMemory = ConfigUtils.getLongWithAltKeys(configProperties, HOODIE_FILE_INDEX_SPILLABLE_MEMORY);
        String spillablePath = ConfigUtils.getStringWithAltKeys(configProperties, HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH, true);
        DiskMapType diskMapType = ConfigUtils.getRawValueWithAltKeys(configProperties, SPILLABLE_DISK_MAP_TYPE, true);
        boolean isBitCaskCompressionEnabled = ConfigUtils.getBooleanWithAltKeys(configProperties, DISK_MAP_BITCASK_COMPRESSION_ENABLED);
        this.cachedAllInputFileSlices = new ExternalSpillableMap<>(
            spillableMemory, spillablePath, new DefaultSizeEstimator<>(), new DefaultSizeEstimator<>(), diskMapType,
            new HoodieFileSliceSerializer(), isBitCaskCompressionEnabled, this.getClass().getName());
      } else {
        long maxCachedMemory = ConfigUtils.getLongWithAltKeys(configProperties, HOODIE_FILE_INDEX_MAX_MEMORY);
        // NOTE: Maintenance (i.e. eviction) runs on the calling thread, so that the cache's footprint is back within
        //       budget by the time a write returns rather than whenever a pool thread gets round to it.
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().executor(Runnable::run);
        // NOTE: The bound is only applied for lazy listing. With eager listing the cache is (re-)populated with
        //       exactly the table's partitions on every refresh, so it cannot grow with the lifetime of the index,
        //       and bounding it would only force partitions the query provably needs to be re-listed.
        Cache<PartitionPath, List<FileSlice>> cache = (shouldListLazily && maxCachedMemory > 0)
            ? cacheBuilder.maximumWeight(maxCachedMemory)
                .weigher(BaseHoodieTableFileIndex::estimateFileSliceCacheWeight)
                .<PartitionPath, List<FileSlice>>build()
            : cacheBuilder.build();
        this.fileSliceCache = cache;
        this.cachedAllInputFileSlices = cache.asMap();
      }
      this.cachedPartitionFileSizes = new ConcurrentHashMap<>();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to refresh file index", e);
    }
    if (!shouldListLazily) {
      getInputFileSlices(getAllQueryPartitionPaths().toArray(new PartitionPath[0]));
    }

    long elapsedMs = timer.endTimer();
    log.info("[table={}] HoodieFileIndex.doRefresh took {} ms", tableName, elapsedMs);
  }

  private void validate(HoodieTimeline activeTimeline, Option<String> queryInstant) {
    if (shouldValidateInstant) {
      if (queryInstant.isPresent() && !activeTimeline.containsInstant(queryInstant.get())) {
        throw new HoodieIOException(String.format("Query instant (%s) not found in the timeline", queryInstant.get()));
      }
    }
  }

  private boolean canParsePartitionValues() {
    // If we failed to parse partition-values from the partition-path partition value will be
    // represented as {@code InternalRow.empty}. In that case we try to recourse to query table
    // as a non-partitioned one instead of failing
    //
    // NOTE: In case of lazy listing, we can't validate upfront whether we'd be able to parse
    //       partition-values and such fallback unfortunately won't be functional.
    //       This method has to return stable response once corresponding file index is initialized,
    //       and can't change its value afterwards
    return shouldListLazily || cachedAllPartitionPaths.stream().allMatch(p -> p.values.length > 0);
  }

  protected long getTotalCachedFilesSize() {
    // NOTE: Read off the per-partition ledger rather than off the (bounded, evicting) file-slice cache, so that the
    //       figure covers every partition this index has listed and does not shrink when one of them is evicted.
    return cachedPartitionFileSizes.values().stream().mapToLong(Long::longValue).sum();
  }

  protected boolean areAllFileSlicesCached() {
    // Loop over partition paths to check if all partitions are initialized.
    return areAllPartitionPathsCached()
        && cachedAllPartitionPaths.stream().allMatch(p -> cachedAllInputFileSlices.containsKey(p));
  }

  public boolean areAllPartitionPathsCached() {
    // If the partition paths is not fully initialized yet, then the file slices are also not fully initialized.
    return cachedAllPartitionPaths != null;
  }

  protected boolean shouldReadAsPartitionedTable() {
    return (partitionColumns.length > 0 && canParsePartitionValues()) || HoodieTableMetadata.isMetadataTable(basePath);
  }

  protected PartitionPath convertToPartitionPath(String partitionPath) {
    Object[] partitionColumnValues = getPartitionColumnValues(partitionColumns, partitionPath);
    return new PartitionPath(partitionPath, partitionColumnValues);
  }

  private static long fileSliceSize(FileSlice fileSlice) {
    long logFileSize = fileSlice.getLogFiles().map(HoodieLogFile::getFileSize)
        .filter(s -> s > 0)
        .reduce(0L, Long::sum);

    return fileSlice.getBaseFile().map(BaseFile::getFileSize).orElse(0L) + logFileSize;
  }

  private void resetTableMetadata(HoodieTableMetadata newTableMetadata) {
    if (tableMetadata != null) {
      try {
        tableMetadata.close();
      } catch (Exception e) {
        throw new HoodieException("Failed to close HoodieTableMetadata instance", e);
      }
    }
    tableMetadata = newTableMetadata;
  }

  protected HoodieTableMetadata createMetadataTable(HoodieEngineContext engineContext) {
    return metaClient.getTableFormat().getMetadataFactory()
        .create(engineContext, metaClient.getStorage(), metadataConfig, basePath.toString(), true);
  }

  /**
   * Estimates the heap held by one entry of {@code cachedAllInputFileSlices}, for the purposes of
   * {@link org.apache.hudi.common.config.HoodieCommonConfig#HOODIE_FILE_INDEX_MAX_MEMORY}.
   *
   * <p>Bounding by bytes rather than by a partition count matters because a partition's file-slice list scales with
   * the number of file groups (and, for MOR, log files) it holds, which varies by orders of magnitude between tables:
   * a fixed partition count that is generous for a narrow table is far past any reasonable heap budget for a wide one.
   *
   * <p>This counts structure rather than walking the object graph, because Caffeine calls a weigher on every single
   * insertion, lazy listing is the default and this budget defaults to 500 MB -- so an exact estimator here runs on
   * every Spark read. Walking the graph with {@link org.apache.hudi.common.util.ObjectSizeCalculator} measured, on
   * one developer machine and one JDK, about 90us per partition at 20 file groups once warm and about 380us on the
   * first call, and 0.5-0.9ms at 140 file slices; counting structure instead takes 0.4us and 0.8us on the same two
   * shapes. Those absolute figures are machine- and load-dependent -- an earlier run of the same measurement under a
   * concurrent build reported 2.6ms for the 140-slice case -- but the ratio between the two is three orders of
   * magnitude either way, which is the point: an exact estimator is tens of seconds of driver CPU for one cold plan
   * of a table with many partitions. (The spillable-map flavour of this same cache can afford that estimator only
   * because {@code ExternalSpillableMap} samples it -- on the first insertion, then every hundredth.)
   *
   * <p>What is counted instead is what the exact size is nearly linear in: the number of files described, and the
   * length of the path each of them holds. Every file slice carries a {@code HoodieBaseFile}, every log file a
   * {@code HoodieLogFile} and every bootstrapped base file an external source {@code BaseFile}, each holding its own
   * full path as a {@code String} and again inside a {@code StoragePathInfo}'s {@code StoragePath}, whose {@code URI}
   * keeps the parsed components and whose {@code toString} and parent are cached -- measured at about 5.4 bytes of
   * heap per character of that path, charged here at 6.
   *
   * <p>The path length is <em>sampled</em>, not summed: the first file path this entry offers is taken as the length
   * of every file path in the partition. That keeps the weigher O(slices) with no walk into each slice's log files,
   * and it is a good sample because every path in a partition shares the table base path and the partition path and
   * differs only in the file name. It has to be the real file path and not {@code partition.getPath()}: the partition
   * path is relative and omits both the table base path and the file name, which for an ordinary local-filesystem
   * table is 4 characters standing in for 117. A bootstrapped table's external source files live under a different,
   * potentially much longer path, so they are counted and sampled separately rather than charged at the Hudi path's
   * length; a 1000-character external path is otherwise invisible to the weigher.
   *
   * <p>Calibrated against {@code ObjectSizeCalculator} over absolute-path shapes: table base paths of 44, 59 and 120
   * characters, partition paths of 4, 33 and 202, 1-500 file slices, 0-10 log files each, file names of 73, 128 and
   * 255 characters, and bootstrap source files under external paths of 17 and 1000 characters. Over that grid the
   * proxy lands between 0.94x and 1.78x of the exact figure, and on entries actually produced by listing a table
   * through {@code HoodieTestTable} at 1, 6, 26 and 126 file groups it lands at 1.13x-1.15x.
   *
   * <p>A proxy necessarily mis-charges, and it is worth being explicit about which way, because the two directions
   * are not symmetric: under-charging lets the cache exceed the configured budget, over-charging only makes the bound
   * tighter than asked. On every shape measured the under-charge bottoms out at 0.94x, i.e. the resident set runs at
   * most about 6% over {@code hoodie.file.index.cache.max.mem}. The rest of the spread is over-charge, and it is
   * concentrated in three shapes: a file slice with log files and no base file at all (a MOR file group before its
   * first compaction) is charged for a base file that does not exist, which is 1.78x at one log file, 1.26x at three
   * and 1.07x at ten; a partition whose base files have names far longer than its log files' -- 255-character names
   * against ordinary ones -- reaches 1.50x, because one sampled path is applied to both; and a bootstrapped table
   * under a 1000-character external path sits at 1.14x. Ordinary copy-on-write partitions, local or object-store,
   * stay within 0.94x-1.16x. So "pathological" here means specifically a partition whose file paths differ widely in
   * length from the first one sampled, and the consequence of hitting it is a cache that holds less than the budget
   * allows, not more. That trade is deliberate: mis-charging a cache trades heap against re-listing and cannot lose
   * data either way, whereas an exact weigher costs the CPU above on every read.
   *
   * <p>Note that this makes {@code hoodie.file.index.cache.max.mem} and
   * {@code hoodie.file.index.cache.spillable.mem} approximations of different shapes rather than one quantity: the
   * budget here is a sum of per-entry estimates, whereas the spillable map's is its entry count times a single
   * sampled average payload size, which on a skewed table is a different number again.
   */
  static int estimateFileSliceCacheWeight(PartitionPath partition, List<FileSlice> fileSlices) {
    long files = 0;
    long bootstrapFiles = 0;
    // NOTE: Sampled, not summed: one file path per partition, applied to every file in it. Within a partition every
    //       path shares the table base path and the partition path and differs only in the file name, so one sample
    //       is representative; summing would cost a pass over every log file, which is what this weigher exists to
    //       avoid. -1 means "nothing sampled yet".
    int filePathLength = -1;
    int bootstrapPathLength = -1;
    for (FileSlice fileSlice : fileSlices) {
      // The slice itself, its log files, and -- for a bootstrapped table -- the base file's external source file
      files += 1 + fileSlice.getLogFileCnt();
      Option<HoodieBaseFile> baseFile = fileSlice.getBaseFile();
      if (baseFile.isPresent()) {
        if (filePathLength < 0) {
          filePathLength = baseFile.get().getPath().length();
        }
        Option<BaseFile> bootstrapBaseFile = baseFile.get().getBootstrapBaseFile();
        if (bootstrapBaseFile.isPresent()) {
          bootstrapFiles++;
          if (bootstrapPathLength < 0) {
            bootstrapPathLength = bootstrapBaseFile.get().getPath().length();
          }
        }
      } else if (filePathLength < 0 && fileSlice.hasLogFiles()) {
        // A slice with no base file at all (MOR before its first compaction): sample a log file's path instead
        filePathLength = fileSlice.getLatestLogFile().get().getPath().toString().length();
      }
    }
    if (filePathLength < 0) {
      // Nothing to sample from -- an empty partition, or one holding only empty slices. The file terms are zero
      // anyway; fall back to the partition path so the figure stays defined.
      filePathLength = partition.getPath().length();
    }
    if (bootstrapPathLength < 0) {
      bootstrapPathLength = filePathLength;
    }
    long weight = FILE_SLICE_CACHE_ENTRY_OVERHEAD_BYTES
        + (long) FILE_SLICE_CACHE_KEY_BYTES_PER_PATH_CHAR * partition.getPath().length()
        + files * (FILE_SLICE_CACHE_BYTES_PER_FILE
            + (long) FILE_SLICE_CACHE_BYTES_PER_PATH_CHAR * filePathLength)
        + bootstrapFiles * (FILE_SLICE_CACHE_BYTES_PER_FILE
            + (long) FILE_SLICE_CACHE_BYTES_PER_PATH_CHAR * bootstrapPathLength);
    // NOTE: Caffeine weights are ints. A single partition heavier than Integer.MAX_VALUE bytes cannot be expressed,
    //       and clamping it simply means such an entry is treated as "as heavy as an entry can be".
    return (int) Math.max(1L, Math.min(Integer.MAX_VALUE, weight));
  }

  /**
   * Partition path information containing the relative partition path
   * and values of partition columns.
   */
  @Value
  public static class PartitionPath implements Serializable {

    String path;
    Object[] values;
  }

  /**
   * APIs for caching {@link StoragePathInfo}.
   */
  protected interface FileStatusCache {
    Option<List<StoragePathInfo>> get(StoragePath path);

    void put(StoragePath path, List<StoragePathInfo> leafFiles);

    void invalidate();
  }

  public static class NoopCache implements FileStatusCache {
    @Override
    public Option<List<StoragePathInfo>> get(StoragePath path) {
      return Option.empty();
    }

    @Override
    public void put(StoragePath path, List<StoragePathInfo> leafFiles) {
      // no-op
    }

    @Override
    public void invalidate() {
      // no-op
    }
  }
}
