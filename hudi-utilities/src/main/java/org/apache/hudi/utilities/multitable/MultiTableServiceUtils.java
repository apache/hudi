/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;

public class MultiTableServiceUtils {

  public static class Constants {
    public static final String TABLES_TO_BE_INGESTED_PROP = "hoodie.tableservice.tablesToServe";

    public static final String COMMA_SEPARATOR = ",";

    public static final String LOCAL_SPARK_MASTER = "local[2]";

    private static final int DEFAULT_LISTING_PARALLELISM = 1500;
  }

  public static List<String> getTablesToBeIngestedFromProps(TypedProperties properties) {
    String combinedTablesString = properties.getString(Constants.TABLES_TO_BE_INGESTED_PROP);
    if (combinedTablesString == null) {
      return new ArrayList<>();
    }
    String[] tablesArray = combinedTablesString.split(Constants.COMMA_SEPARATOR);
    return Arrays.asList(tablesArray);
  }

  public static List<String> findHoodieTablesUnderPath(JavaSparkContext jsc, String pathStr) {
    Path rootPath = new Path(pathStr);
    SerializableConfiguration conf = new SerializableConfiguration(jsc.hadoopConfiguration());
    if (isHoodieTable(rootPath, conf.get())) {
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
        FileSystem fileSystem = path.getFileSystem(conf.get());
        return fileSystem.listStatus(path);
      }, listingParallelism);
      pathsToList.clear();

      // if current dictionary contains meta folder(.hoodie), add it to result. Otherwise, add it to queue
      List<FileStatus> dirs = dirToFileListing.stream().flatMap(Arrays::stream)
          .filter(FileStatus::isDirectory)
          .collect(Collectors.toList());

      if (!dirs.isEmpty()) {
        List<Pair<FileStatus, Integer>> dirResults = engineContext.map(dirs, fileStatus -> {
          if (isHoodieTable(fileStatus.getPath(), conf.get())) {
            return Pair.of(fileStatus, 0);
          } else if (!fileStatus.getPath().getName().equals(METAFOLDER_NAME)) {
            return Pair.of(fileStatus, 1);
          }
          return Pair.of(fileStatus, 2);
        }, Math.min(Constants.DEFAULT_LISTING_PARALLELISM, dirs.size()));

        dirResults.stream().parallel().forEach(dirResult -> {
          FileStatus fileStatus = dirResult.getLeft();
          if (dirResult.getRight() == 0) {
            hoodieTablePaths.add(fileStatus.getPath().toString());
          } else if (dirResult.getRight() == 1) {
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
    if (cfg.enableCompaction) {
      pipeline.add(CompactionTask.newBuilder()
          .withJsc(jsc)
          .withBasePath(basePath)
          .withParallelism(cfg.parallelism)
          .withCompactionRunningMode(cfg.compactionRunningMode)
          .withProps(props)
          .withRetry(cfg.retry)
          .build());
    }
    if (cfg.enableClustering) {
      pipeline.add(ClusteringTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withParallelism(cfg.parallelism)
          .withClusteringRunningMode(cfg.clusteringRunningMode)
          .withProps(props)
          .withRetry(cfg.retry)
          .build());
    }
    if (cfg.enableClean) {
      pipeline.add(CleanTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withRetry(cfg.retry)
          .withProps(props)
          .build());
    }
    if (cfg.enableArchive) {
      pipeline.add(ArchiveTask.newBuilder()
          .withBasePath(basePath)
          .withJsc(jsc)
          .withProps(props)
          .withRetry(cfg.retry)
          .build());
    }
    return pipeline;
  }
}
