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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.CachingPath.createPathUnsafe;

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
  private static final Logger LOG = LogManager.getLogger(BaseHoodieTableFileIndex.class);

  private final String[] partitionColumns;

  protected final HoodieMetadataConfig metadataConfig;

  private final HoodieTableQueryType queryType;
  private final Option<String> specifiedQueryInstant;
  protected final List<Path> queryPaths;

  private final boolean shouldIncludePendingCommits;
  private final boolean shouldValidateInstant;

  private final HoodieTableType tableType;
  protected final Path basePath;

  private final HoodieTableMetaClient metaClient;
  private final HoodieEngineContext engineContext;

  private final transient FileStatusCache fileStatusCache;

  protected volatile boolean queryAsNonePartitionedTable = false;

  protected transient volatile long cachedFileSize = 0L;
  private transient volatile Map<PartitionPath, List<FileSlice>> cachedAllInputFileSlices = new HashMap<>();
  /**
   * It always contains all partition paths, or null if it is not initialized yet.
   */
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
   */
  public BaseHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  HoodieTableQueryType queryType,
                                  List<Path> queryPaths,
                                  Option<String> specifiedQueryInstant,
                                  boolean shouldIncludePendingCommits,
                                  boolean shouldValidateInstant,
                                  FileStatusCache fileStatusCache,
                                  boolean shouldListLazily) {
    this.partitionColumns = metaClient.getTableConfig().getPartitionFields()
        .orElse(new String[0]);

    this.metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(configProperties)
        .build();

    this.queryType = queryType;
    this.queryPaths = queryPaths;
    this.specifiedQueryInstant = specifiedQueryInstant;
    this.shouldIncludePendingCommits = shouldIncludePendingCommits;
    this.shouldValidateInstant = shouldValidateInstant;

    this.tableType = metaClient.getTableType();
    this.basePath = metaClient.getBasePathV2();

    this.metaClient = metaClient;
    this.engineContext = engineContext;
    this.fileStatusCache = fileStatusCache;

    /**
     * The `shouldListLazily` variable controls how we initialize the TableFileIndex:
     *  - non-lazy/eager listing (shouldListLazily=false):  all partitions and file slices will be loaded eagerly during initialization.
     *  - lazy listing (shouldListLazily=true): partitions listing will be done lazily with the knowledge from query predicate on partition
     *        columns. And file slices fetching only happens for partitions satisfying the given filter.
     *
     * In SparkSQL, `shouldListLazily` is controlled by option `REFRESH_PARTITION_AND_FILES_IN_INITIALIZATION`.
     * In lazy listing case, if no predicate on partition is provided, all partitions will still be loaded.
     */
    if (shouldListLazily) {
      initMetadataTable();
    } else {
      doRefresh();
    }
  }

  protected abstract Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath);

  /**
   * Returns latest completed instant as seen by this instance of the file-index
   */
  public Option<HoodieInstant> getLatestCompletedInstant() {
    return getActiveTimeline().filterCompletedInstants().lastInstant();
  }

  /**
   * Returns table's base-path
   */
  public String getBasePath() {
    return basePath.toString();
  }

  /**
   * Fetch list of latest base files and log files per partition.
   *
   * @return mapping from string partition paths to its base/log files
   */
  public Map<String, List<FileSlice>> listFileSlices() {
    return cachedAllInputFileSlices.entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey().path, Map.Entry::getValue));
  }

  public int getFileSlicesCount() {
    return cachedAllInputFileSlices.values().stream()
        .mapToInt(List::size).sum();
  }

  @Override
  public void close() throws Exception {
    resetTableMetadata(null);
  }

  protected List<PartitionPath> getAllQueryPartitionPaths() {
    if (cachedAllPartitionPaths == null) {
      List<String> queryRelativePartitionPaths = queryPaths.stream()
          .map(path -> FSUtils.getRelativePartitionPath(basePath, path))
          .collect(Collectors.toList());

      this.cachedAllPartitionPaths = listPartitionPaths(queryRelativePartitionPaths);
      // If the partition value contains InternalRow.empty, we query it as a non-partitioned table.
      // This normally happens for default partition case or non-encoded partition value.
      this.queryAsNonePartitionedTable = cachedAllPartitionPaths.stream().anyMatch(p -> p.values.length == 0);
    }

    return cachedAllPartitionPaths;
  }

  protected Map<PartitionPath, List<FileSlice>> getAllInputFileSlices() {
    if (!isAllInputFileSlicesCached()) {
      // Fetching file slices for partitions that have not been cached
      List<PartitionPath> partitions = getAllQueryPartitionPaths().stream()
          .filter(p -> !cachedAllInputFileSlices.containsKey(p)).collect(Collectors.toList());
      cachedAllInputFileSlices.putAll(loadFileSlicesForPartitions(partitions));
    }
    return cachedAllInputFileSlices;
  }

  /**
   * Get input file slice for the given partition. Will use cache directly if it is computed before.
   */
  protected List<FileSlice> getCachedInputFileSlices(PartitionPath partition) {
    return cachedAllInputFileSlices.computeIfAbsent(partition,
        p -> loadFileSlicesForPartitions(Collections.singletonList(p)).get(p));
  }

  private Map<PartitionPath, List<FileSlice>> loadFileSlicesForPartitions(List<PartitionPath> partitions) {
    Map<PartitionPath, FileStatus[]> partitionFiles = partitions.stream()
        .collect(Collectors.toMap(p -> p, this::loadPartitionPathFiles));
    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<HoodieInstant> latestInstant = activeTimeline.lastInstant();

    FileStatus[] allFiles = partitionFiles.values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);
    HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles);

    Option<String> queryInstant = specifiedQueryInstant.or(() -> latestInstant.map(HoodieInstant::getTimestamp));

    validate(activeTimeline, queryInstant);

    // NOTE: For MOR table, when the compaction is inflight, we need to not only fetch the
    // latest slices, but also include the base and log files of the second-last version of
    // the file slice in the same file group as the latest file slice that is under compaction.
    // This logic is realized by `AbstractTableFileSystemView::getLatestMergedFileSlicesBeforeOrOn`
    // API.  Note that for COW table, the merging logic of two slices does not happen as there
    // is no compaction, thus there is no performance impact.
    Map<PartitionPath, List<FileSlice>> listedPartitions = partitionFiles.keySet().stream()
        .collect(Collectors.toMap(
                Function.identity(),
                partitionPath ->
                    queryInstant.map(instant ->
                            fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath.path, queryInstant.get())
                        )
                        .orElse(fileSystemView.getLatestFileSlices(partitionPath.path))
                        .collect(Collectors.toList())
            )
        );

    this.cachedFileSize += listedPartitions.values().stream()
        .flatMap(Collection::stream)
        .mapToLong(BaseHoodieTableFileIndex::fileSliceSize)
        .sum();

    return listedPartitions;
  }

  /**
   * Get partition path with the given partition value
   * @param partitionColumnValuePairs list of pair of partition column name to the predicate value
   * @return partitions that match the given partition values
   */
  protected List<PartitionPath> getPartitionPaths(List<Pair<String, String>> partitionColumnValuePairs) {
    if (cachedAllPartitionPaths != null) {
      LOG.debug("All partition paths have already loaded, use it directly");
      return cachedAllPartitionPaths;
    }

    Pair<String, Boolean> relativeQueryPartitionPathPair = composeRelativePartitionPaths(partitionColumnValuePairs);
    // Fallback to eager list if there is no predicate on partition columns (i.e., the input partitionColumnValuePairs list is empty)
    if (relativeQueryPartitionPathPair.getLeft().isEmpty()) {
      return getAllQueryPartitionPaths();
    }

    // If the composed partition path is complete, we return it directly, to save extra DFS listing operations.
    if (relativeQueryPartitionPathPair.getRight()) {
      return Collections.singletonList(new PartitionPath(relativeQueryPartitionPathPair.getLeft(),
          parsePartitionColumnValues(partitionColumns, relativeQueryPartitionPathPair.getLeft())));
    }
    // The input partition values (from query predicate) forms a prefix of partition path, do listing to the path only.
    return listPartitionPaths(Collections.singletonList(relativeQueryPartitionPathPair.getLeft()));
  }

  /**
   * Construct relative partition path (i.e., partition prefix) from the given partition values
   * @return relative partition path and a flag to indicate if the path is complete (i.e., not a prefix)
   */
  private Pair<String, Boolean> composeRelativePartitionPaths(List<Pair<String, String>> partitionColumnValuePairs) {
    if (partitionColumnValuePairs.size() == 0) {
      LOG.info("The input partition names or value is empty");
      return Pair.of("", false);
    }

    boolean hiveStylePartitioning = Boolean.parseBoolean(metaClient.getTableConfig().getHiveStylePartitioningEnable());
    boolean urlEncodePartitioning = Boolean.parseBoolean(metaClient.getTableConfig().getUrlEncodePartitioning());
    Map<String, Integer> partitionNameToIdx = IntStream.range(0, partitionColumnValuePairs.size())
        .mapToObj(i -> Pair.of(i, partitionColumnValuePairs.get(i).getKey()))
        .collect(Collectors.toMap(Pair::getValue, Pair::getKey));
    StringBuilder queryPartitionPath = new StringBuilder();
    boolean isPartial = false;
    for (int idx = 0; idx < partitionColumns.length; ++idx) {
      String columnName = partitionColumns[idx];
      if (partitionNameToIdx.containsKey(columnName)) {
        int k = partitionNameToIdx.get(columnName);
        String columnValue = partitionColumnValuePairs.get(k).getValue();
        String encodedValue = urlEncodePartitioning ? PartitionPathEncodeUtils.escapePathName(columnValue) : columnValue;
        queryPartitionPath.append(hiveStylePartitioning ? columnName + "=" : "").append(encodedValue).append("/");
      } else {
        isPartial = true;
        break;
      }
    }
    queryPartitionPath.deleteCharAt(queryPartitionPath.length() - 1);
    return Pair.of(queryPartitionPath.toString(), !isPartial && partitionColumnValuePairs.size() == partitionColumns.length);
  }

  private List<PartitionPath> listPartitionPaths(List<String> relativePartitionPaths) {
    List<String> matchedPartitionPaths;
    try {
      matchedPartitionPaths = tableMetadata.getPartitionPathsWithPrefixes(relativePartitionPaths);
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

  /**
   * Load partition paths and it's files under the query table path.
   */
  private FileStatus[] loadPartitionPathFiles(PartitionPath partition) {
    // Try fetch from the FileStatusCache first
    Option<FileStatus[]> files = fileStatusCache.get(partition.fullPartitionPath(basePath));
    if (files.isPresent()) {
      return files.get();
    }

    try {
      Path path = partition.fullPartitionPath(basePath);
      FileStatus[] fetchedFiles = tableMetadata.getAllFilesInPartition(path);

      // Update the fileStatusCache
      fileStatusCache.put(partition.fullPartitionPath(basePath), fetchedFiles);
      return fetchedFiles;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to list partition path (" + partition + ") for a table", e);
    }
  }

  private void initMetadataTable() {
    HoodieTableMetadata newTableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePath.toString(),
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());
    resetTableMetadata(newTableMetadata);
  }

  private void doRefresh() {
    HoodieTimer timer = HoodieTimer.start();

    initMetadataTable();

    // Reset it to null to trigger re-loading of all partition path
    this.cachedAllPartitionPaths = null;
    this.cachedFileSize = 0;
    metaClient.reloadActiveTimeline();

    // Refresh the partitions & file slices
    this.cachedAllInputFileSlices = loadFileSlicesForPartitions(getAllQueryPartitionPaths());

    LOG.info(String.format("Refresh table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), timer.endTimer()));
  }

  private void validate(HoodieTimeline activeTimeline, Option<String> queryInstant) {
    if (shouldValidateInstant) {
      if (queryInstant.isPresent() && !activeTimeline.containsInstant(queryInstant.get())) {
        throw new HoodieIOException(String.format("Query instant (%s) not found in the timeline", queryInstant.get()));
      }
    }
  }

  protected boolean isAllInputFileSlicesCached() {
    // If the partition paths is not fully initialized yet, then the file slices are also not fully initialized.
    if (cachedAllPartitionPaths == null) {
      return false;
    }
    // Loop over partition paths to check if all partitions are initialized.
    return cachedAllPartitionPaths.stream().allMatch(p -> cachedAllInputFileSlices.containsKey(p));
  }

  protected boolean isPartitionedTable() {
    return !queryAsNonePartitionedTable && (partitionColumns.length > 0 || HoodieTableMetadata.isMetadataTable(basePath.toString()));
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

    Path fullPartitionPath(Path basePath) {
      if (!path.isEmpty()) {
        // NOTE: Since we now that the path is a proper relative path that doesn't require
        //       normalization we create Hadoop's Path using more performant unsafe variant
        return new CachingPath(basePath, createPathUnsafe(path));
      }

      return basePath;
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

  protected interface FileStatusCache {
    Option<FileStatus[]> get(Path path);

    void put(Path path, FileStatus[] leafFiles);

    void invalidate();
  }
}
