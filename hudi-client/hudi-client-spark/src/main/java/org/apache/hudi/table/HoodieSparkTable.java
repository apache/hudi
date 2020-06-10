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

package org.apache.hudi.table;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieSparkIndexFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class HoodieSparkTable<T extends HoodieRecordPayload>
    extends BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkTable.class);

  protected HoodieSparkTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient, new SparkTaskContextSupplier(), HoodieSparkIndexFactory.createIndex(config));
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(
        hadoopConf,
        config.getBasePath(),
        true,
        config.getConsistencyGuardConfig(),
        Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))
    );
    return HoodieSparkTable.create(metaClient, config, hadoopConf);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieTableMetaClient metaClient,
                                                                      HoodieWriteConfig config,
                                                                      Configuration hadoopConf) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieSparkCopyOnWriteTable<T>(config, hadoopConf, metaClient);
      case MERGE_ON_READ:
        return new HoodieSparkMergeOnReadTable<>(config, hadoopConf, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  @Override
  public void cleanFailedWrites(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats, boolean consistencyCheckEnabled) throws HoodieIOException {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));

      if (!fs.exists(markerDir)) {
        // Happens when all writes are appends
        return;
      }
      final String baseFileExtension = getBaseFileFormat().getFileExtension();
      List<String> invalidDataPaths = FSUtils.getAllDataFilesForMarkers(fs, basePath, instantTs, markerDir.toString(),
          baseFileExtension);
      List<String> validDataPaths = stats.stream().map(w -> String.format("%s/%s", basePath, w.getPath()))
          .filter(p -> p.endsWith(".parquet")).collect(Collectors.toList());
      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);
      if (!invalidDataPaths.isEmpty()) {
        LOG.info(
            "Removing duplicate data files created due to spark retries before committing. Paths=" + invalidDataPaths);
      }

      Map<String, List<Pair<String, String>>> groupByPartition = invalidDataPaths.stream()
          .map(dp -> Pair.of(new Path(dp).getParent().toString(), dp)).collect(Collectors.groupingBy(Pair::getKey));

      if (!groupByPartition.isEmpty()) {
        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(context, groupByPartition, ConsistencyGuard.FileVisibility.APPEAR);
        }

        // Now delete partially written files
        jsc.parallelize(new ArrayList<>(groupByPartition.values()), config.getFinalizeWriteParallelism())
            .map(partitionWithFileList -> {
              final FileSystem fileSystem = metaClient.getFs();
              LOG.info("Deleting invalid data files=" + partitionWithFileList);
              if (partitionWithFileList.isEmpty()) {
                return true;
              }
              // Delete
              partitionWithFileList.stream().map(Pair::getValue).forEach(file -> {
                try {
                  fileSystem.delete(new Path(file), false);
                } catch (IOException e) {
                  throw new HoodieIOException(e.getMessage(), e);
                }
              });

              return true;
            }).collect();

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(context, groupByPartition, ConsistencyGuard.FileVisibility.DISAPPEAR);
        }
      }
      // Now delete the marker directory
      deleteMarkerDir(instantTs);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public void waitForAllFiles(HoodieEngineContext context, Map<String, List<Pair<String, String>>> groupByPartition, ConsistencyGuard.FileVisibility visibility) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    // This will either ensure all files to be deleted are present.
    boolean checkPassed =
        jsc.parallelize(new ArrayList<>(groupByPartition.entrySet()), config.getFinalizeWriteParallelism())
            .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
                partitionWithFileList.getValue().stream(), visibility))
            .collect().stream().allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

}
