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

package org.apache.hudi.utilities;

import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.ReadOptimizedView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import scala.Tuple2;

/**
 * Hoodie snapshot copy job which copies latest files from all partitions to another place, for snapshot backup.
 */
public class HoodieSnapshotCopier implements Serializable {

  private static Logger logger = LogManager.getLogger(HoodieSnapshotCopier.class);

  static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-bp"}, description = "Hoodie table base path", required = true)
    String basePath = null;

    @Parameter(names = {"--output-path", "-op"}, description = "The snapshot output path", required = true)
    String outputPath = null;

    @Parameter(names = {"--date-partitioned", "-dp"}, description = "Can we assume date partitioning?")
    boolean shouldAssumeDatePartitioning = false;
  }

  public void snapshot(JavaSparkContext jsc, String baseDir, final String outputDir,
      final boolean shouldAssumeDatePartitioning) throws IOException {
    FileSystem fs = FSUtils.getFs(baseDir, jsc.hadoopConfiguration());
    final SerializableConfiguration serConf = new SerializableConfiguration(jsc.hadoopConfiguration());
    final HoodieTableMetaClient tableMetadata = new HoodieTableMetaClient(fs.getConf(), baseDir);
    final ReadOptimizedView fsView = new HoodieTableFileSystemView(tableMetadata,
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    // Get the latest commit
    Option<HoodieInstant> latestCommit =
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
    if (!latestCommit.isPresent()) {
      logger.warn("No commits present. Nothing to snapshot");
      return;
    }
    final String latestCommitTimestamp = latestCommit.get().getTimestamp();
    logger.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.",
        latestCommitTimestamp));

    List<String> partitions = FSUtils.getAllPartitionPaths(fs, baseDir, shouldAssumeDatePartitioning);
    if (partitions.size() > 0) {
      logger.info(String.format("The job needs to copy %d partitions.", partitions.size()));

      // Make sure the output directory is empty
      Path outputPath = new Path(outputDir);
      if (fs.exists(outputPath)) {
        logger.warn(String.format("The output path %s targetBasePath already exists, deleting", outputPath));
        fs.delete(new Path(outputDir), true);
      }

      jsc.parallelize(partitions, partitions.size()).flatMap(partition -> {
        // Only take latest version files <= latestCommit.
        FileSystem fs1 = FSUtils.getFs(baseDir, serConf.newCopy());
        List<Tuple2<String, String>> filePaths = new ArrayList<>();
        Stream<HoodieDataFile> dataFiles = fsView.getLatestDataFilesBeforeOrOn(partition, latestCommitTimestamp);
        dataFiles.forEach(hoodieDataFile -> filePaths.add(new Tuple2<>(partition, hoodieDataFile.getPath())));

        // also need to copy over partition metadata
        Path partitionMetaFile =
            new Path(new Path(baseDir, partition), HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE);
        if (fs1.exists(partitionMetaFile)) {
          filePaths.add(new Tuple2<>(partition, partitionMetaFile.toString()));
        }

        return filePaths.iterator();
      }).foreach(tuple -> {
        String partition = tuple._1();
        Path sourceFilePath = new Path(tuple._2());
        Path toPartitionPath = new Path(outputDir, partition);
        FileSystem ifs = FSUtils.getFs(baseDir, serConf.newCopy());

        if (!ifs.exists(toPartitionPath)) {
          ifs.mkdirs(toPartitionPath);
        }
        FileUtil.copy(ifs, sourceFilePath, ifs, new Path(toPartitionPath, sourceFilePath.getName()), false,
            ifs.getConf());
      });

      // Also copy the .commit files
      logger.info(String.format("Copying .commit files which are no-late-than %s.", latestCommitTimestamp));
      FileStatus[] commitFilesToCopy =
          fs.listStatus(new Path(baseDir + "/" + HoodieTableMetaClient.METAFOLDER_NAME), (commitFilePath) -> {
            if (commitFilePath.getName().equals(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
              return true;
            } else {
              String commitTime = FSUtils.getCommitFromCommitFile(commitFilePath.getName());
              return HoodieTimeline.compareTimestamps(commitTime, latestCommitTimestamp,
                  HoodieTimeline.LESSER_OR_EQUAL);
            }
          });
      for (FileStatus commitStatus : commitFilesToCopy) {
        Path targetFilePath =
            new Path(outputDir + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitStatus.getPath().getName());
        if (!fs.exists(targetFilePath.getParent())) {
          fs.mkdirs(targetFilePath.getParent());
        }
        if (fs.exists(targetFilePath)) {
          logger.error(
              String.format("The target output commit file (%s targetBasePath) already exists.", targetFilePath));
        }
        FileUtil.copy(fs, commitStatus.getPath(), fs, targetFilePath, false, fs.getConf());
      }
    } else {
      logger.info("The job has 0 partition to copy.");
    }

    // Create the _SUCCESS tag
    Path successTagPath = new Path(outputDir + "/_SUCCESS");
    if (!fs.exists(successTagPath)) {
      logger.info(String.format("Creating _SUCCESS under targetBasePath: $s", outputDir));
      fs.createNewFile(successTagPath);
    }
  }

  public static void main(String[] args) throws IOException {
    // Take input configs
    final Config cfg = new Config();
    new JCommander(cfg, args);
    logger.info(String.format("Snapshot hoodie table from %s targetBasePath to %stargetBasePath", cfg.basePath,
        cfg.outputPath));

    // Create a spark job to do the snapshot copy
    SparkConf sparkConf = new SparkConf().setAppName("Hoodie-snapshot-copier");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    logger.info("Initializing spark job.");

    // Copy
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc, cfg.basePath, cfg.outputPath, cfg.shouldAssumeDatePartitioning);

    // Stop the job
    jsc.stop();
  }
}
