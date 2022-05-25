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
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A file index which supports listing files efficiently through metadata table.
 *
 * <p>It caches the partition paths to avoid redundant look up.
 */
public class FileIndex {
  private final Path path;
  private final HoodieMetadataConfig metadataConfig;
  private List<String> partitionPaths; // cache of partition paths
  private final boolean tableExists;

  private FileIndex(Path path, Configuration conf) {
    this.path = path;
    this.metadataConfig = metadataConfig(conf);
    this.tableExists = StreamerUtil.tableExists(path.toString(), HadoopConfigurations.getHadoopConf(conf));
  }

  public static FileIndex instance(Path path, Configuration conf) {
    return new FileIndex(path, conf);
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
    return FSUtils.getFilesInPartitions(HoodieFlinkEngineContext.DEFAULT, metadataConfig, path.toString(),
            partitions, "/tmp/")
        .values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);
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
