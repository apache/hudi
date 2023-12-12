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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieSerializableFileStatus;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.expression.BindVisitor;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.PartialBindVisitor;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Types;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Implementation of {@link HoodieTableMetadata} based file-system-backed table metadata.
 */
public class FileSystemBackedTableMetadata extends AbstractHoodieTableMetadata {

  private static final int DEFAULT_LISTING_PARALLELISM = 1500;

  private final boolean hiveStylePartitioningEnabled;
  private final boolean urlEncodePartitioningEnabled;

  public FileSystemBackedTableMetadata(HoodieEngineContext engineContext, HoodieTableConfig tableConfig,
                                       SerializableConfiguration conf, String datasetBasePath) {
    super(engineContext, conf, datasetBasePath);

    this.hiveStylePartitioningEnabled = Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable());
    this.urlEncodePartitioningEnabled = Boolean.parseBoolean(tableConfig.getUrlEncodePartitioning());
  }

  public FileSystemBackedTableMetadata(HoodieEngineContext engineContext,
                                       SerializableConfiguration conf,
                                       String datasetBasePath) {
    super(engineContext, conf, datasetBasePath);

    FileSystem fs = FSUtils.getFs(dataBasePath.get(), conf.get());
    Path metaPath = new Path(dataBasePath.get(), HoodieTableMetaClient.METAFOLDER_NAME);
    TableNotFoundException.checkTableValidity(fs, this.dataBasePath.get(), metaPath);
    HoodieTableConfig tableConfig = new HoodieTableConfig(fs, metaPath.toString(), null, null);
    this.hiveStylePartitioningEnabled = Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable());
    this.urlEncodePartitioningEnabled = Boolean.parseBoolean(tableConfig.getUrlEncodePartitioning());
  }

  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException {
    FileSystem fs = partitionPath.getFileSystem(hadoopConf.get());
    return FSUtils.getAllDataFilesInPartition(fs, partitionPath);
  }

  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    return getPartitionPathWithPathPrefixes(Collections.singletonList(""));
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixUsingFilterExpression(List<String> relativePathPrefixes,
                                                                          Types.RecordType partitionFields,
                                                                          Expression expression) throws IOException {
    return relativePathPrefixes.stream().flatMap(relativePathPrefix -> {
      try {
        return getPartitionPathWithPathPrefixUsingFilterExpression(relativePathPrefix,
            partitionFields, expression).stream();
      } catch (IOException e) {
        throw new HoodieIOException("Error fetching partition paths with relative path: " + relativePathPrefix, e);
      }
    }).collect(Collectors.toList());
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) {
    return relativePathPrefixes.stream().flatMap(relativePathPrefix -> {
      try {
        return getPartitionPathWithPathPrefix(relativePathPrefix).stream();
      } catch (IOException e) {
        throw new HoodieIOException("Error fetching partition paths with relative path: " + relativePathPrefix, e);
      }
    }).collect(Collectors.toList());
  }

  private List<String> getPartitionPathWithPathPrefix(String relativePathPrefix) throws IOException {
    return getPartitionPathWithPathPrefixUsingFilterExpression(relativePathPrefix, null, null);
  }

  private List<String> getPartitionPathWithPathPrefixUsingFilterExpression(String relativePathPrefix,
                                                                           Types.RecordType partitionFields,
                                                                           Expression pushedExpr) throws IOException {
    List<Path> pathsToList = new CopyOnWriteArrayList<>();
    pathsToList.add(StringUtils.isNullOrEmpty(relativePathPrefix)
        ? dataBasePath.get() : new Path(dataBasePath.get(), relativePathPrefix));
    List<String> partitionPaths = new CopyOnWriteArrayList<>();

    int currentPartitionLevel = -1;
    boolean needPushDownExpressions;
    Expression fullBoundExpr;
    // Not like `HoodieBackedTableMetadata`, since we don't know the exact partition levels here,
    // given it's possible that partition values contains `/`, which could affect
    // the result to get right `partitionValue` when listing paths, here we have
    // to make it more strict that `urlEncodePartitioningEnabled` must be enabled.
    // TODO better enable urlEncodePartitioningEnabled if hiveStylePartitioningEnabled is enabled?
    if (hiveStylePartitioningEnabled && urlEncodePartitioningEnabled
        && pushedExpr != null && partitionFields != null) {
      currentPartitionLevel = getPathPartitionLevel(partitionFields, relativePathPrefix);
      needPushDownExpressions = true;
      fullBoundExpr = pushedExpr.accept(new BindVisitor(partitionFields, caseSensitive));
    } else {
      fullBoundExpr = Predicates.alwaysTrue();
      needPushDownExpressions = false;
    }

    while (!pathsToList.isEmpty()) {
      // TODO: Get the parallelism from HoodieWriteConfig
      int listingParallelism = Math.min(DEFAULT_LISTING_PARALLELISM, pathsToList.size());

      // List all directories in parallel
      engineContext.setJobStatus(this.getClass().getSimpleName(), "Listing all partitions with prefix " + relativePathPrefix);
      // Need to use serializable file status here, see HUDI-5936
      List<HoodieSerializableFileStatus> dirToFileListing = engineContext.flatMap(pathsToList, path -> {
        FileSystem fileSystem = path.getFileSystem(hadoopConf.get());
        return Arrays.stream(HoodieSerializableFileStatus.fromFileStatuses(fileSystem.listStatus(path)));
      }, listingParallelism);
      pathsToList.clear();

      // if current dictionary contains PartitionMetadata, add it to result
      // if current dictionary does not contain PartitionMetadata, add it to queue to be processed.
      int fileListingParallelism = Math.min(DEFAULT_LISTING_PARALLELISM, dirToFileListing.size());
      if (!dirToFileListing.isEmpty()) {
        // result below holds a list of pair. first entry in the pair optionally holds the deduced list of partitions.
        // and second entry holds optionally a directory path to be processed further.
        engineContext.setJobStatus(this.getClass().getSimpleName(), "Processing listed partitions");
        List<Pair<Option<String>, Option<Path>>> result = engineContext.map(dirToFileListing, fileStatus -> {
          Path path = fileStatus.getPath();
          FileSystem fileSystem = path.getFileSystem(hadoopConf.get());
          if (fileStatus.isDirectory()) {
            if (HoodiePartitionMetadata.hasPartitionMetadata(fileSystem, path)) {
              return Pair.of(Option.of(FSUtils.getRelativePartitionPath(dataBasePath.get(), path)), Option.empty());
            } else if (!path.getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
              return Pair.of(Option.empty(), Option.of(path));
            }
          } else if (path.getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
            String partitionName = FSUtils.getRelativePartitionPath(dataBasePath.get(), path.getParent());
            return Pair.of(Option.of(partitionName), Option.empty());
          }
          return Pair.of(Option.empty(), Option.empty());
        }, fileListingParallelism);

        partitionPaths.addAll(result.stream().filter(entry -> entry.getKey().isPresent())
            .map(entry -> entry.getKey().get())
            .filter(relativePartitionPath -> fullBoundExpr instanceof Predicates.TrueExpression
                || (Boolean) fullBoundExpr.eval(
                extractPartitionValues(partitionFields, relativePartitionPath, urlEncodePartitioningEnabled)))
            .collect(Collectors.toList()));

        Expression partialBoundExpr;
        // If partitionPaths is nonEmpty, we're already at the last path level, and all paths
        // are filtered already.
        if (needPushDownExpressions && partitionPaths.isEmpty()) {
          // Here we assume the path level matches the number of partition columns, so we'll rebuild
          // new schema based on current path level.
          // e.g. partition columns are <region, date, hh>, if we're listing the second level, then
          // currentSchema would be <region, date>
          // `PartialBindVisitor` will bind reference if it can be found from `currentSchema`, otherwise
          // will change the expression to `alwaysTrue`. Can see `PartialBindVisitor` for details.
          Types.RecordType currentSchema = Types.RecordType.get(partitionFields.fields().subList(0, ++currentPartitionLevel));
          PartialBindVisitor partialBindVisitor = new PartialBindVisitor(currentSchema, caseSensitive);
          partialBoundExpr = pushedExpr.accept(partialBindVisitor);
        } else {
          partialBoundExpr = Predicates.alwaysTrue();
        }

        pathsToList.addAll(result.stream().filter(entry -> entry.getValue().isPresent()).map(entry -> entry.getValue().get())
            .filter(path -> partialBoundExpr instanceof Predicates.TrueExpression
                || (Boolean) partialBoundExpr.eval(
                    extractPartitionValues(partitionFields, FSUtils.getRelativePartitionPath(dataBasePath.get(), path), urlEncodePartitioningEnabled)))
            .collect(Collectors.toList()));
      }
    }
    return partitionPaths;
  }

  @Override
  public Map<String, FileStatus[]> getAllFilesInPartitions(Collection<String> partitionPaths)
      throws IOException {
    if (partitionPaths == null || partitionPaths.isEmpty()) {
      return Collections.emptyMap();
    }

    int parallelism = Math.min(DEFAULT_LISTING_PARALLELISM, partitionPaths.size());

    engineContext.setJobStatus(this.getClass().getSimpleName(), "Listing all files in " + partitionPaths.size() + " partitions");
    // Need to use serializable file status here, see HUDI-5936
    List<Pair<String, HoodieSerializableFileStatus[]>> partitionToFiles = engineContext.map(new ArrayList<>(partitionPaths), partitionPathStr -> {
      Path partitionPath = new Path(partitionPathStr);
      FileSystem fs = partitionPath.getFileSystem(hadoopConf.get());
      return Pair.of(partitionPathStr, HoodieSerializableFileStatus.fromFileStatuses(FSUtils.getAllDataFilesInPartition(fs, partitionPath)));
    }, parallelism);

    return partitionToFiles.stream().collect(Collectors.toMap(Pair::getLeft, pair -> HoodieSerializableFileStatus.toFileStatuses(pair.getRight())));
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  @Override
  public void reset() {
    // no-op
  }

  public Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName)
      throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilter for " + fileName);
  }

  @Override
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList)
      throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilters!");
  }

  @Override
  public Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(final List<Pair<String, String>> partitionNameFileNameList, final String columnName)
      throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getColumnsStats!");
  }

  @Override
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> keyPrefixes, String partitionName, boolean shouldLoadInMemory) {
    throw new HoodieMetadataException("Unsupported operation: getRecordsByKeyPrefixes!");
  }

  @Override
  public Map<String, HoodieRecordGlobalLocation> readRecordIndex(List<String> recordKeys) {
    throw new HoodieMetadataException("Unsupported operation: readRecordIndex!");
  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    throw new HoodieMetadataException("Unsupported operation: getNumFileGroupsForPartition");
  }
}
