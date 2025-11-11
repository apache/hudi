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

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
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
public abstract class BaseHoodieTableFileIndex implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieTableFileIndex.class);

  private final String[] partitionColumns;

  protected final HoodieMetadataConfig metadataConfig;

  private final HoodieTableQueryType queryType;
  private final Option<String> specifiedQueryInstant;
  private final Option<String> beginInstantTime;
  private final Option<String> endInstantTime;
  private final List<StoragePath> queryPaths;

  private final boolean shouldIncludePendingCommits;
  private final boolean shouldValidateInstant;

  // The `shouldListLazily` variable controls how we initialize/refresh the TableFileIndex:
  //  - non-lazy/eager listing (shouldListLazily=false):  all partitions and file slices will be loaded eagerly during initialization.
  //  - lazy listing (shouldListLazily=true): partitions listing will be done lazily with the knowledge from query predicate on partition
  //        columns. And file slices fetching only happens for partitions satisfying the given filter.
  //
  // In SparkSQL, `shouldListLazily` is controlled by option `REFRESH_PARTITION_AND_FILES_IN_INITIALIZATION`.
  // In lazy listing case, if no predicate on partition is provided, all partitions will still be loaded.
  private final boolean shouldListLazily;

  private final StoragePath basePath;

  private final HoodieTableMetaClient metaClient;
  private final HoodieEngineContext engineContext;

  private final transient FileStatusCache fileStatusCache;

  // NOTE: Individual partitions are always cached in full: meaning that if partition is cached
  //       it will hold all the file-slices residing w/in the partition
  private transient volatile Map<PartitionPath, List<FileSlice>> cachedAllInputFileSlices = new HashMap<>();

  // NOTE: It always contains either all partition paths, or null if it is not initialized yet
  private transient volatile List<PartitionPath> cachedAllPartitionPaths = null;

  private transient HoodieTableMetadata tableMetadata = null;

  /**
   * @param engineContext Hudi engine-specific context
   * @param metaClient Hudi table's meta-client
   * @param configProperties unifying configuration (in the form of generic properties)
   * @param queryType target query type
   * @param queryPaths target DFS paths being queried
   * @param specifiedQueryInstant instant as of which table is being queried
   * @param shouldIncludePendingCommits flags whether file-index should exclude any pending operations
   * @param shouldValidateInstant flags to validate whether query instant is present in the timeline
   * @param fileStatusCache transient cache of fetched [[FileStatus]]es
   * @param beginInstantTime begin instant time for incremental query (optional)
   * @param endInstantTime end instant time for incremental query (optional)
   */
  public BaseHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  HoodieTableQueryType queryType,
                                  List<StoragePath> queryPaths,
                                  Option<String> specifiedQueryInstant,
                                  boolean shouldIncludePendingCommits,
                                  boolean shouldValidateInstant,
                                  FileStatusCache fileStatusCache,
                                  boolean shouldListLazily,
                                  Option<String> beginInstantTime,
                                  Option<String> endInstantTime) {
    this.partitionColumns = metaClient.getTableConfig().getPartitionFields()
        .orElseGet(() -> new String[0]);

    this.metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(configProperties)
        .enable(configProperties.getBoolean(ENABLE.key(), DEFAULT_METADATA_ENABLE_FOR_READERS)
            && HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
        .build();

    this.queryType = queryType;
    this.queryPaths = queryPaths;
    this.specifiedQueryInstant = specifiedQueryInstant;
    this.shouldIncludePendingCommits = shouldIncludePendingCommits;
    this.shouldValidateInstant = shouldValidateInstant;
    this.shouldListLazily = shouldListLazily;
    this.beginInstantTime = beginInstantTime;
    this.endInstantTime = endInstantTime;

    this.basePath = metaClient.getBasePath();

    this.metaClient = metaClient;
    this.engineContext = engineContext;
    this.fileStatusCache = fileStatusCache;

    doRefresh();
  }

  protected abstract Object[] doParsePartitionColumnValues(String[] partitionColumns, String partitionPath);

  /**
   * Returns latest completed instant as seen by this instance of the file-index
   */
  public Option<HoodieInstant> getLatestCompletedInstant() {
    return getActiveTimeline().filterCompletedInstants().lastInstant();
  }

  /**
   * Returns table's base-path
   */
  public StoragePath getBasePath() {
    return basePath;
  }

  public int getFileSlicesCount() {
    return getAllInputFileSlices().values().stream()
        .mapToInt(List::size).sum();
  }

  @Override
  public void close() throws Exception {
    resetTableMetadata(null);
  }

  protected String[] getPartitionColumns() {
    return partitionColumns;
  }

  protected List<StoragePath> getQueryPaths() {
    return queryPaths;
  }

  /**
   * Returns all partition paths matching the ones explicitly provided by the query (if any)
   */
  protected List<PartitionPath> getAllQueryPartitionPaths() {
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
    if (!areAllFileSlicesCached()) {
      ensurePreloadedPartitions(getAllQueryPartitionPaths());
    }

    return cachedAllInputFileSlices;
  }

  /**
   * Get input file slice for the given partition. Will use cache directly if it is computed before.
   */
  protected Map<PartitionPath, List<FileSlice>> getInputFileSlices(PartitionPath... partitions) {
    ensurePreloadedPartitions(Arrays.asList(partitions));
    return Arrays.stream(partitions).collect(
        Collectors.toMap(Function.identity(), partition -> cachedAllInputFileSlices.get(partition))
    );
  }

  private void ensurePreloadedPartitions(List<PartitionPath> partitionPaths) {
    // Fetching file slices for partitions that have not been cached yet
    List<PartitionPath> missingPartitions = partitionPaths.stream()
        .filter(p -> !cachedAllInputFileSlices.containsKey(p))
        .collect(Collectors.toList());

    // NOTE: Individual partitions are always cached in full, therefore if partition is cached
    //       it will hold all the file-slices residing w/in the partition
    cachedAllInputFileSlices.putAll(loadFileSlicesForPartitions(missingPartitions));
  }

  private Map<PartitionPath, List<FileSlice>> loadFileSlicesForPartitions(List<PartitionPath> partitions) {
    if (partitions.isEmpty()) {
      return Collections.emptyMap();
    }

    if (specifiedQueryInstant.isPresent() && !shouldIncludePendingCommits) {
      validateTimestampAsOf(metaClient, specifiedQueryInstant.get());
    }

    List<StoragePathInfo> allFiles = listPartitionPathFiles(partitions);
    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<HoodieInstant> latestInstant = activeTimeline.lastInstant();

    HoodieTableFileSystemView fileSystemView =
        new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles);

    Option<String> queryInstant = specifiedQueryInstant.or(() -> latestInstant.map(HoodieInstant::getTimestamp));

    validate(activeTimeline, queryInstant);

    // NOTE: For MOR table, when the compaction is inflight, we need to not only fetch the
    // latest slices, but also include the base and log files of the second-last version of
    // the file slice in the same file group as the latest file slice that is under compaction.
    // This logic is realized by `AbstractTableFileSystemView::getLatestMergedFileSlicesBeforeOrOn`
    // API.  Note that for COW table, the merging logic of two slices does not happen as there
    // is no compaction, thus there is no performance impact.
    return partitions.stream().collect(
        Collectors.toMap(
            Function.identity(),
            partitionPath ->
                queryInstant.map(instant ->
                        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath.path, queryInstant.get())
                    )
                    .orElseGet(() -> fileSystemView.getLatestFileSlices(partitionPath.path))
                    .collect(Collectors.toList())
        ));
  }

  protected List<PartitionPath> listPartitionPaths(List<String> relativePartitionPaths,
                                                   Types.RecordType partitionFields,
                                                   Expression partitionColumnPredicates) {
    List<String> matchedPartitionPaths;
    try {
      matchedPartitionPaths = tableMetadata.getPartitionPathWithPathPrefixUsingFilterExpression(relativePartitionPaths,
          partitionFields, partitionColumnPredicates);
    } catch (IOException e) {
      throw new HoodieIOException("Error fetching partition paths", e);
    }

    // Convert partition's path into partition descriptor
    return matchedPartitionPaths.stream()
        .map(partitionPath -> {
          Object[] partitionColumnValues = parsePartitionColumnValues(partitionColumns, partitionPath);
          return new PartitionPath(partitionPath, partitionColumnValues);
        })
        .collect(Collectors.toList());
  }

  protected List<PartitionPath> listPartitionPaths(List<String> relativePartitionPaths) {
    List<String> matchedPartitionPaths;
    try {
      if (isPartitionedTable()) {
        if (queryType == HoodieTableQueryType.INCREMENTAL && beginInstantTime.isPresent()) {
          HoodieTimeline timelineAfterBeginInstant = TimelineUtils.getCommitsTimelineAfter(metaClient, beginInstantTime.get(), Option.empty());
          HoodieTimeline timelineToQuery = endInstantTime.map(timelineAfterBeginInstant::findInstantsBeforeOrEquals).orElse(timelineAfterBeginInstant);
          matchedPartitionPaths = TimelineUtils.getWrittenPartitions(timelineToQuery);
        } else {
          matchedPartitionPaths = tableMetadata.getPartitionPathWithPathPrefixes(relativePartitionPaths);
        }
      } else {
        matchedPartitionPaths = Collections.singletonList(StringUtils.EMPTY_STRING);
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error fetching partition paths", e);
    }

    // Convert partition's path into partition descriptor
    return matchedPartitionPaths.stream()
        .map(partitionPath -> {
          Object[] partitionColumnValues = parsePartitionColumnValues(partitionColumns, partitionPath);
          return new PartitionPath(partitionPath, partitionColumnValues);
        })
        .collect(Collectors.toList());
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

  private Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
    Object[] partitionColumnValues = doParsePartitionColumnValues(partitionColumns, partitionPath);
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
  private List<StoragePathInfo> listPartitionPathFiles(List<PartitionPath> partitions) {
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

    // NOTE: We're constructing a mapping of absolute form of the partition-path into
    //       its relative one, such that we don't need to reconstruct these again later on
    Map<String, StoragePath> missingPartitionPathsMap = missingPartitionPaths.stream()
        .collect(Collectors.toMap(
            relativePartitionPath -> new StoragePath(basePath, relativePartitionPath.toString()).toString(),
            Function.identity()
        ));

    try {
      Map<String, List<StoragePathInfo>> fetchedPartitionsMap =
          tableMetadata.getAllFilesInPartitions(missingPartitionPathsMap.keySet());

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
      throw new HoodieIOException("Failed to list partition paths", e);
    }
  }

  private void doRefresh() {
    HoodieTimer timer = HoodieTimer.start();

    resetTableMetadata(createMetadataTable(engineContext, metaClient.getStorage(), metadataConfig, basePath));

    // Make sure we reload active timeline
    metaClient.reloadActiveTimeline();

    // Reset it to null to trigger re-loading of all partition path
    this.cachedAllPartitionPaths = null;
    // Reset to force reload file slices inside partitions
    this.cachedAllInputFileSlices = new HashMap<>();
    if (!shouldListLazily) {
      ensurePreloadedPartitions(getAllQueryPartitionPaths());
    }

    LOG.info(String.format("Refresh table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), timer.endTimer()));
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
    return cachedAllInputFileSlices.values().stream()
        .flatMap(Collection::stream)
        .mapToLong(BaseHoodieTableFileIndex::fileSliceSize)
        .sum();
  }

  protected boolean areAllFileSlicesCached() {
    // Loop over partition paths to check if all partitions are initialized.
    return areAllPartitionPathsCached()
        && cachedAllPartitionPaths.stream().allMatch(p -> cachedAllInputFileSlices.containsKey(p));
  }

  protected boolean areAllPartitionPathsCached() {
    // If the partition paths is not fully initialized yet, then the file slices are also not fully initialized.
    return cachedAllPartitionPaths != null;
  }

  protected boolean shouldReadAsPartitionedTable() {
    return (partitionColumns.length > 0 && canParsePartitionValues()) || HoodieTableMetadata.isMetadataTable(basePath);
  }

  protected PartitionPath convertToPartitionPath(String partitionPath) {
    Object[] partitionColumnValues = parsePartitionColumnValues(partitionColumns, partitionPath);
    return new PartitionPath(partitionPath, partitionColumnValues);
  }

  private static long fileSliceSize(FileSlice fileSlice) {
    long logFileSize = fileSlice.getLogFiles().map(HoodieLogFile::getFileSize)
        .filter(s -> s > 0)
        .reduce(0L, Long::sum);

    return fileSlice.getBaseFile().map(BaseFile::getFileLen).orElse(0L) + logFileSize;
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

  private static HoodieTableMetadata createMetadataTable(
      HoodieEngineContext engineContext,
      HoodieStorage storage,
      HoodieMetadataConfig metadataConfig,
      StoragePath basePath
  ) {
    HoodieTableMetadata newTableMetadata = HoodieTableMetadata.create(
        engineContext, storage, metadataConfig, basePath.toString(), true);
    return newTableMetadata;
  }

  /**
   * Partition path information containing the relative partition path
   * and values of partition columns.
   */
  public static final class PartitionPath {

    final String path;
    final Object[] values;

    public PartitionPath(String path, Object[] values) {
      this.path = path;
      this.values = values;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof PartitionPath
          && Objects.equals(path, ((PartitionPath) other).path)
          && Arrays.equals(values, ((PartitionPath) other).values);
    }

    @Override
    public int hashCode() {
      return path.hashCode() * 1103 + Arrays.hashCode(values);
    }
  }

  /**
   * APIs for caching {@link StoragePathInfo}.
   */
  protected interface FileStatusCache {
    Option<List<StoragePathInfo>> get(StoragePath path);

    void put(StoragePath path, List<StoragePathInfo> leafFiles);

    void invalidate();
  }
}
