/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;

/**
 * Utils for executing multi-table services.
 */
public class MultiTableServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTableServiceUtils.class);

  public static class Constants {
    public static final String TABLES_TO_BE_SERVED_PROP = "hoodie.tableservice.tablesToServe";

    public static final String TABLES_SKIP_WRONG_PATH = "hoodie.tableservice.skipNonHudiTable";

    public static final String COMMA_SEPARATOR = ",";

    private static final int DEFAULT_LISTING_PARALLELISM = 1500;
  }

  public static List<String> getTablesToBeServedFromProps(JavaSparkContext jsc, TypedProperties properties) {
    StorageConfiguration<Configuration> conf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    String combinedTablesString = properties.getString(Constants.TABLES_TO_BE_SERVED_PROP);
    boolean skipWrongPath = properties.getBoolean(Constants.TABLES_SKIP_WRONG_PATH, false);
    if (combinedTablesString == null) {
      return new ArrayList<>();
    }
    String[] tablesArray = combinedTablesString.split(Constants.COMMA_SEPARATOR);

    List<String> tablePaths;
    if (skipWrongPath) {
      tablePaths = Arrays.stream(tablesArray)
          .filter(tablePath -> {
            if (isHoodieTable(new Path(tablePath), conf.unwrap())) {
              return true;
            } else {
              // Log the wrong path in console.
              LOG.info("Hoodie table not found in path {}, skip", tablePath);
              return false;
            }
          }).collect(Collectors.toList());
    } else {
      tablePaths = Arrays.asList(tablesArray);
      tablePaths.stream()
          .filter(tablePath -> !isHoodieTable(new Path(tablePath), conf.unwrap()))
          .findFirst()
          .ifPresent(tablePath -> {
            throw new TableNotFoundException("Table not found: " + tablePath);
          });
    }
    return tablePaths;
  }

  /**
   * Type of directories when searching hoodie tables under path.
   */
  enum DirType {
    HOODIE_TABLE,
    META_FOLDER,
    NORMAL_DIR
  }

  public static List<String> findHoodieTablesUnderPath(JavaSparkContext jsc, String pathStr) {
    Path rootPath = new Path(pathStr);
    StorageConfiguration<Configuration> conf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    if (isHoodieTable(rootPath, conf.unwrap())) {
      return Collections.singletonList(pathStr);
    }

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> hoodieTablePaths = new CopyOnWriteArrayList<>();
    List<Path> pathsToList = new CopyOnWriteArrayList<>();
    pathsToList.add(rootPath);
    int listingParallelism = Math.min(Constants.DEFAULT_LISTING_PARALLELISM, pathsToList.size());

    while (!pathsToList.isEmpty()) {
      // List all directories in parallel
      List<FileStatus[]> dirToFileListing = engineContext.map(pathsToList, path -> {
        FileSystem fileSystem = path.getFileSystem(conf.unwrap());
        return fileSystem.listStatus(path);
      }, listingParallelism);
      pathsToList.clear();

      // if current dictionary contains meta folder(.hoodie), add it to result. Otherwise, add it to queue
      List<FileStatus> dirs = dirToFileListing.stream().flatMap(Arrays::stream)
          .filter(FileStatus::isDirectory)
          .collect(Collectors.toList());

      if (!dirs.isEmpty()) {
        List<Pair<FileStatus, DirType>> dirResults = engineContext.map(dirs, fileStatus -> {
          if (isHoodieTable(fileStatus.getPath(), conf.unwrap())) {
            return Pair.of(fileStatus, DirType.HOODIE_TABLE);
          } else if (fileStatus.getPath().getName().equals(METAFOLDER_NAME)) {
            return Pair.of(fileStatus, DirType.META_FOLDER);
          } else {
            return Pair.of(fileStatus, DirType.NORMAL_DIR);
          }
        }, Math.min(Constants.DEFAULT_LISTING_PARALLELISM, dirs.size()));

        dirResults.stream().parallel().forEach(dirResult -> {
          FileStatus fileStatus = dirResult.getLeft();
          if (dirResult.getRight() == DirType.HOODIE_TABLE) {
            hoodieTablePaths.add(fileStatus.getPath().toString());
          } else if (dirResult.getRight() == DirType.NORMAL_DIR) {
            pathsToList.add(fileStatus.getPath());
          }
        });
      }
    }

    return hoodieTablePaths;
  }

  private static boolean isHoodieTable(Path path, Configuration conf) {
    try {
      FileSystem fs = path.getFileSystem(conf);
      return fs.exists(path) && fs.exists(new Path(path, METAFOLDER_NAME));
    } catch (Exception e) {
      throw new HoodieException("Error checking presence of partition meta file for " + path, e);
    }
  }

  public static TableServicePipeline buildTableServicePipeline(JavaSparkContext jsc,
                                                               String basePath,
                                                               HoodieMultiTableServicesMain.Config cfg,
                                                               TypedProperties props) {
    TableServicePipeline pipeline = new TableServicePipeline();
    HoodieTableMetaClient metaClient = UtilHelpers.createMetaClient(jsc, basePath, true);
    TypedProperties propsWithTableConfig = TypedProperties.copy(metaClient.getTableConfig().getProps());
    propsWithTableConfig.putAll(props);

    if (cfg.enableCompaction) {
      pipeline.add(CompactionTask.newBuilder()
          .withJsc(jsc)
          .withBasePath(basePath)
          .withParallelism(cfg.parallelism)
          .withCompactionRunningMode(cfg.compactionRunningMode)
          .withCompactionStrategyName(cfg.compactionStrategyClassName)
          .withProps(propsWithTableConfig)
          .withRetry(cfg.retry)
          .withMetaclient(metaClient)
          .build());
    }
    if (cfg.enableClustering) {
      pipeline.add(ClusteringTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withParallelism(cfg.parallelism)
          .withClusteringRunningMode(cfg.clusteringRunningMode)
          .withProps(propsWithTableConfig)
          .withRetry(cfg.retry)
          .withMetaclient(metaClient)
          .build());
    }
    if (cfg.enableClean) {
      pipeline.add(CleanTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withRetry(cfg.retry)
          .withProps(propsWithTableConfig)
          .build());
    }
    if (cfg.enableArchive) {
      pipeline.add(ArchiveTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withProps(propsWithTableConfig)
          .withRetry(cfg.retry)
          .build());
    }
    return pipeline;
  }
}
