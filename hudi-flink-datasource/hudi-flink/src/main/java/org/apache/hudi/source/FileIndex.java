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

package org.apache.hudi.source;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.stats.ColumnStatsIndices;
import org.apache.hudi.source.stats.ExpressionEvaluator;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.ExpressionUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A file index which supports listing files efficiently through metadata table.
 *
 * <p>It caches the partition paths to avoid redundant look up.
 */
public class FileIndex {
  private static final Logger LOG = LoggerFactory.getLogger(FileIndex.class);

  private final Path path;
  private final RowType rowType;
  private final HoodieMetadataConfig metadataConfig;
  private final boolean dataSkippingEnabled;
  private List<String> partitionPaths;      // cache of partition paths
  private List<ResolvedExpression> filters; // push down filters
  private final boolean tableExists;

  private FileIndex(Path path, Configuration conf, RowType rowType) {
    this.path = path;
    this.rowType = rowType;
    this.metadataConfig = metadataConfig(conf);
    this.dataSkippingEnabled = conf.getBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED);
    this.tableExists = StreamerUtil.tableExists(path.toString(), HadoopConfigurations.getHadoopConf(conf));
  }

  public static FileIndex instance(Path path, Configuration conf, RowType rowType) {
    return new FileIndex(path, conf, rowType);
  }

  /**
   * Returns the partition path key and values as a list of map, each map item in the list
   * is a mapping of the partition key name to its actual partition value. For example, say
   * there is a file path with partition keys [key1, key2, key3]:
   *
   * <p><pre>
   *   -- file:/// ... key1=val1/key2=val2/key3=val3
   *   -- file:/// ... key1=val4/key2=val5/key3=val6
   * </pre>
   *
   * <p>The return list should be [{key1:val1, key2:val2, key3:val3}, {key1:val4, key2:val5, key3:val6}].
   *
   * @param partitionKeys  The partition key list
   * @param defaultParName The default partition name for nulls
   * @param hivePartition  Whether the partition path is in Hive style
   */
  public List<Map<String, String>> getPartitions(
      List<String> partitionKeys,
      String defaultParName,
      boolean hivePartition) {
    if (partitionKeys.size() == 0) {
      // non partitioned table
      return Collections.emptyList();
    }
    List<String> partitionPaths = getOrBuildPartitionPaths();
    if (partitionPaths.size() == 1 && partitionPaths.get(0).isEmpty()) {
      return Collections.emptyList();
    }
    List<Map<String, String>> partitions = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      String[] paths = partitionPath.split(Path.SEPARATOR);
      Map<String, String> partitionMapping = new LinkedHashMap<>();
      if (hivePartition) {
        Arrays.stream(paths).forEach(p -> {
          String[] kv = p.split("=");
          if (kv.length == 2) {
            partitionMapping.put(kv[0], defaultParName.equals(kv[1]) ? null : kv[1]);
          }
        });
      } else {
        for (int i = 0; i < partitionKeys.size(); i++) {
          partitionMapping.put(partitionKeys.get(i), defaultParName.equals(paths[i]) ? null : paths[i]);
        }
      }
      partitions.add(partitionMapping);
    }
    return partitions;
  }

  /**
   * Returns all the file statuses under the table base path.
   */
  public FileStatus[] getFilesInPartitions() {
    if (!tableExists) {
      return new FileStatus[0];
    }
    String[] partitions = getOrBuildPartitionPaths().stream().map(p -> fullPartitionPath(path, p)).toArray(String[]::new);
    FileStatus[] allFileStatus = FSUtils.getFilesInPartitions(HoodieFlinkEngineContext.DEFAULT, metadataConfig, path.toString(),
            partitions, "/tmp/")
        .values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);
    Set<String> candidateFiles = candidateFilesInMetadataTable(allFileStatus);
    if (candidateFiles == null) {
      // no need to filter by col stats or error occurs.
      return allFileStatus;
    }
    return Arrays.stream(allFileStatus).parallel()
        .filter(fileStatus -> candidateFiles.contains(fileStatus.getPath().getName()))
        .toArray(FileStatus[]::new);
  }

  /**
   * Returns the full partition path.
   *
   * @param basePath      The base path.
   * @param partitionPath The relative partition path, may be empty if the table is non-partitioned.
   * @return The full partition path string
   */
  private static String fullPartitionPath(Path basePath, String partitionPath) {
    if (partitionPath.isEmpty()) {
      return basePath.toString();
    }
    return new Path(basePath, partitionPath).toString();
  }

  /**
   * Reset the state of the file index.
   */
  @VisibleForTesting
  public void reset() {
    this.partitionPaths = null;
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  /**
   * Sets up explicit partition paths for pruning.
   */
  public void setPartitionPaths(@Nullable Set<String> partitionPaths) {
    if (partitionPaths != null) {
      this.partitionPaths = new ArrayList<>(partitionPaths);
    }
  }

  /**
   * Sets up pushed down filters.
   */
  public void setFilters(List<ResolvedExpression> filters) {
    if (filters.size() > 0) {
      this.filters = new ArrayList<>(filters);
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Computes pruned list of candidate base-files' names based on provided list of data filters.
   * conditions, by leveraging Metadata Table's Column Statistics index (hereon referred as ColStats for brevity)
   * bearing "min", "max", "num_nulls" statistics for all columns.
   *
   * <p>NOTE: This method has to return complete set of candidate files, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every base-file's name, that is NOT referenced in its index.
   *
   * <p>The {@code filters} must all be simple.
   *
   * @return list of pruned (data-skipped) candidate base-files' names
   */
  @Nullable
  private Set<String> candidateFilesInMetadataTable(FileStatus[] allFileStatus) {
    // NOTE: Data Skipping is only effective when it references columns that are indexed w/in
    //       the Column Stats Index (CSI). Following cases could not be effectively handled by Data Skipping:
    //          - Expressions on top-level column's fields (ie, for ex filters like "struct.field > 0", since
    //          CSI only contains stats for top-level columns, in this case for "struct")
    //          - Any expression not directly referencing top-level column (for ex, sub-queries, since there's
    //          nothing CSI in particular could be applied for)
    if (!metadataConfig.enabled() || !dataSkippingEnabled) {
      validateConfig();
      return null;
    }
    if (this.filters == null || this.filters.size() == 0) {
      return null;
    }
    String[] referencedCols = ExpressionUtils.referencedColumns(filters);
    if (referencedCols.length == 0) {
      return null;
    }
    try {
      final List<RowData> colStats = ColumnStatsIndices.readColumnStatsIndex(path.toString(), metadataConfig, referencedCols);
      final Pair<List<RowData>, String[]> colStatsTable = ColumnStatsIndices.transposeColumnStatsIndex(colStats, referencedCols, rowType);
      List<RowData> transposedColStats = colStatsTable.getLeft();
      String[] queryCols = colStatsTable.getRight();
      if (queryCols.length == 0) {
        // the indexed columns have no intersection with the referenced columns, returns early
        return null;
      }
      RowType.RowField[] queryFields = DataTypeUtils.projectRowFields(rowType, queryCols);

      Set<String> allIndexedFileNames = transposedColStats.stream().parallel()
          .map(row -> row.getString(0).toString())
          .collect(Collectors.toSet());
      Set<String> candidateFileNames = transposedColStats.stream().parallel()
          .filter(row -> ExpressionEvaluator.filterExprs(filters, row, queryFields))
          .map(row -> row.getString(0).toString())
          .collect(Collectors.toSet());

      // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
      //       base-file: since it's bound to clustering, which could occur asynchronously
      //       at arbitrary point in time, and is not likely to be touching all the base files.
      //
      //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
      //       files and all outstanding base-files, and make sure that all base files not
      //       represented w/in the index are included in the output of this method
      Set<String> nonIndexedFileNames = Arrays.stream(allFileStatus)
          .map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toSet());
      nonIndexedFileNames.removeAll(allIndexedFileNames);

      candidateFileNames.addAll(nonIndexedFileNames);
      return candidateFileNames;
    } catch (Throwable throwable) {
      LOG.warn("Read column stats for data skipping error", throwable);
      return null;
    }
  }

  private void validateConfig() {
    if (dataSkippingEnabled && !metadataConfig.enabled()) {
      LOG.warn("Data skipping requires Metadata Table to be enabled! "
          + "isMetadataTableEnabled = {}", metadataConfig.enabled());
    }
  }

  /**
   * Returns all the relative partition paths.
   *
   * <p>The partition paths are cached once invoked.
   */
  public List<String> getOrBuildPartitionPaths() {
    if (this.partitionPaths != null) {
      return this.partitionPaths;
    }
    this.partitionPaths = this.tableExists
        ? FSUtils.getAllPartitionPaths(HoodieFlinkEngineContext.DEFAULT, metadataConfig, path.toString())
        : Collections.emptyList();
    return this.partitionPaths;
  }

  private static HoodieMetadataConfig metadataConfig(org.apache.flink.configuration.Configuration conf) {
    Properties properties = new Properties();

    // set up metadata.enabled=true in table DDL to enable metadata listing
    properties.put(HoodieMetadataConfig.ENABLE.key(), conf.getBoolean(FlinkOptions.METADATA_ENABLED));

    return HoodieMetadataConfig.newBuilder().fromProperties(properties).build();
  }
}
