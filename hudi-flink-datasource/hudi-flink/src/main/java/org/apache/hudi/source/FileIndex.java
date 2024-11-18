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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.prune.PrimaryKeyPruners;
import org.apache.hudi.source.stats.FileStatsIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A file index which supports listing files efficiently through metadata table.
 *
 * <p>It caches the partition paths to avoid redundant look up.
 */
public class FileIndex implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(FileIndex.class);

  private final StoragePath path;
  private final boolean tableExists;
  private final HoodieMetadataConfig metadataConfig;
  private final org.apache.hadoop.conf.Configuration hadoopConf;
  private final PartitionPruners.PartitionPruner partitionPruner; // for partition pruning
  private final ColumnStatsProbe colStatsProbe;                   // for probing column stats
  private final int dataBucket;                                   // for bucket pruning
  private List<String> partitionPaths;                            // cache of partition paths
  private final FileStatsIndex fileStatsIndex;                    // for data skipping

  private FileIndex(
      StoragePath path,
      Configuration conf,
      RowType rowType,
      ColumnStatsProbe colStatsProbe,
      PartitionPruners.PartitionPruner partitionPruner,
      int dataBucket) {
    this.path = path;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    this.tableExists = StreamerUtil.tableExists(path.toString(), hadoopConf);
    this.metadataConfig = StreamerUtil.metadataConfig(conf);
    this.colStatsProbe = isDataSkippingFeasible(conf.get(FlinkOptions.READ_DATA_SKIPPING_ENABLED)) ? colStatsProbe : null;
    this.partitionPruner = partitionPruner;
    this.dataBucket = dataBucket;
    this.fileStatsIndex = new FileStatsIndex(path.toString(), rowType, metadataConfig);
  }

  /**
   * Returns the builder.
   */
  public static Builder builder() {
    return new Builder();
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
    if (partitionKeys.isEmpty()) {
      // non partitioned table
      return Collections.emptyList();
    }
    List<String> partitionPaths = getOrBuildPartitionPaths();
    if (partitionPaths.size() == 1 && partitionPaths.get(0).isEmpty()) {
      return Collections.emptyList();
    }
    List<Map<String, String>> partitions = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      String[] paths = partitionPath.split(StoragePath.SEPARATOR);
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
  public List<StoragePathInfo> getFilesInPartitions() {
    if (!tableExists) {
      return Collections.emptyList();
    }
    String[] partitions =
        getOrBuildPartitionPaths().stream().map(p -> fullPartitionPath(path, p)).toArray(String[]::new);
    if (partitions.length < 1) {
      return Collections.emptyList();
    }
    List<StoragePathInfo> allFiles = FSUtils.getFilesInPartitions(
            new HoodieFlinkEngineContext(hadoopConf),
            new HoodieHadoopStorage(path, HadoopFSUtils.getStorageConf(hadoopConf)), metadataConfig, path.toString(), partitions)
        .values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (allFiles.isEmpty()) {
      // returns early for empty table.
      return allFiles;
    }

    // bucket pruning
    if (this.dataBucket >= 0) {
      String bucketIdStr = BucketIdentifier.bucketIdStr(this.dataBucket);
      List<StoragePathInfo> filesAfterBucketPruning = allFiles.stream()
          .filter(fileInfo -> fileInfo.getPath().getName().contains(bucketIdStr))
          .collect(Collectors.toList());
      logPruningMsg(allFiles.size(), filesAfterBucketPruning.size(), "bucket pruning");
      allFiles = filesAfterBucketPruning;
    }

    // data skipping
    Set<String> candidateFiles = fileStatsIndex.computeCandidateFiles(
        colStatsProbe, allFiles.stream().map(f -> f.getPath().getName()).collect(Collectors.toList()));
    if (candidateFiles == null) {
      // no need to filter by col stats or error occurs.
      return allFiles;
    }
    List<StoragePathInfo> results = allFiles.stream().parallel()
        .filter(fileStatus -> candidateFiles.contains(fileStatus.getPath().getName()))
        .collect(Collectors.toList());
    logPruningMsg(allFiles.size(), results.size(), "data skipping");
    return results;
  }

  /**
   * Returns the full partition path.
   *
   * @param basePath      The base path.
   * @param partitionPath The relative partition path, may be empty if the table is non-partitioned.
   * @return The full partition path string
   */
  private static String fullPartitionPath(StoragePath basePath, String partitionPath) {
    if (partitionPath.isEmpty()) {
      return basePath.toString();
    }
    return new StoragePath(basePath, partitionPath).toString();
  }

  /**
   * Reset the state of the file index.
   */
  @VisibleForTesting
  public void reset() {
    this.partitionPaths = null;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Returns all the relative partition paths.
   *
   * <p>The partition paths are cached once invoked.
   */
  public List<String> getOrBuildPartitionPaths() {
    if (this.partitionPaths != null) {
      return this.partitionPaths;
    }
    List<String> allPartitionPaths = this.tableExists ? FSUtils.getAllPartitionPaths(
        new HoodieFlinkEngineContext(hadoopConf),
        new HoodieHadoopStorage(path, HadoopFSUtils.getStorageConf(hadoopConf)), metadataConfig, path.toString())
        : Collections.emptyList();
    if (this.partitionPruner == null) {
      this.partitionPaths = allPartitionPaths;
    } else {
      Set<String> prunedPartitionPaths = this.partitionPruner.filter(allPartitionPaths);
      this.partitionPaths = new ArrayList<>(prunedPartitionPaths);
    }
    return this.partitionPaths;
  }

  private boolean isDataSkippingFeasible(boolean dataSkippingEnabled) {
    // NOTE: Data Skipping is only effective when it references columns that are indexed w/in
    //       the Column Stats Index (CSI). Following cases could not be effectively handled by Data Skipping:
    //          - Expressions on top-level column's fields (ie, for ex filters like "struct.field > 0", since
    //          CSI only contains stats for top-level columns, in this case for "struct")
    //          - Any expression not directly referencing top-level column (for ex, sub-queries, since there's
    //          nothing CSI in particular could be applied for)
    if (dataSkippingEnabled) {
      if (metadataConfig.isEnabled()) {
        return true;
      } else {
        LOG.warn("Data skipping requires Metadata Table to be enabled! Disable the data skipping");
      }
    }
    return false;
  }

  private void logPruningMsg(int numTotalFiles, int numLeftFiles, String action) {
    LOG.info("\n"
        + "------------------------------------------------------------\n"
        + "---------- action:        {}\n"
        + "---------- total files:   {}\n"
        + "---------- left files:    {}\n"
        + "---------- skipping rate: {}\n"
        + "------------------------------------------------------------",
        action, numTotalFiles, numLeftFiles, percentage(numTotalFiles, numLeftFiles));
  }

  private static double percentage(double total, double left) {
    return (total - left) / total;
  }

  // -------------------------------------------------------------------------
  //  Inner class
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link FileIndex}.
   */
  public static class Builder {
    private StoragePath path;
    private Configuration conf;
    private RowType rowType;
    private ColumnStatsProbe columnStatsProbe;
    private PartitionPruners.PartitionPruner partitionPruner;
    private int dataBucket = PrimaryKeyPruners.BUCKET_ID_NO_PRUNING;

    private Builder() {
    }

    public Builder path(StoragePath path) {
      this.path = path;
      return this;
    }

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder rowType(RowType rowType) {
      this.rowType = rowType;
      return this;
    }

    public Builder columnStatsProbe(ColumnStatsProbe columnStatsProbe) {
      this.columnStatsProbe = columnStatsProbe;
      return this;
    }

    public Builder partitionPruner(PartitionPruners.PartitionPruner partitionPruner) {
      this.partitionPruner = partitionPruner;
      return this;
    }

    public Builder dataBucket(int dataBucket) {
      this.dataBucket = dataBucket;
      return this;
    }

    public FileIndex build() {
      return new FileIndex(Objects.requireNonNull(path), Objects.requireNonNull(conf), Objects.requireNonNull(rowType),
          columnStatsProbe, partitionPruner, dataBucket);
    }
  }
}
