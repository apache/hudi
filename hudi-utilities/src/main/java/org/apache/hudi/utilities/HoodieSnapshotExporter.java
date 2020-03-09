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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.DataSource;

import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Export the latest records of Hudi dataset to a set of external files (e.g., plain parquet files).
 *
 * @experimental This export is an experimental tool. If you want to export hudi to hudi, please use HoodieSnapshotCopier.
 */

public class HoodieSnapshotExporter {
  private static final Logger LOG = LogManager.getLogger(HoodieSnapshotExporter.class);

  public static class Config implements Serializable {
    @Parameter(names = {"--source-base-path"}, description = "Base path for the source Hudi dataset to be snapshotted", required = true)
    String sourceBasePath = null;

    @Parameter(names = {"--target-base-path"}, description = "Base path for the target output files (snapshots)", required = true)
    String targetOutputPath = null;

    @Parameter(names = {"--output-format"}, description = "e.g. Hudi or Parquet", required = true)
    String outputFormat;

    @Parameter(names = {"--output-partition-field"}, description = "A field to be used by Spark repartitioning")
    String outputPartitionField;
  }

  public int export(SparkSession spark, Config cfg) throws IOException {
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    FileSystem fs = FSUtils.getFs(cfg.sourceBasePath, jsc.hadoopConfiguration());

    final SerializableConfiguration serConf = new SerializableConfiguration(jsc.hadoopConfiguration());
    final HoodieTableMetaClient tableMetadata = new HoodieTableMetaClient(fs.getConf(), cfg.sourceBasePath);
    final TableFileSystemView.BaseFileOnlyView fsView = new HoodieTableFileSystemView(tableMetadata,
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    // Get the latest commit
    Option<HoodieInstant> latestCommit =
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
    if (!latestCommit.isPresent()) {
      LOG.error("No commits present. Nothing to snapshot");
      return -1;
    }
    final String latestCommitTimestamp = latestCommit.get().getTimestamp();
    LOG.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.",
        latestCommitTimestamp));

    List<String> partitions = FSUtils.getAllPartitionPaths(fs, cfg.sourceBasePath, false);
    if (partitions.size() > 0) {
      List<String> dataFiles = new ArrayList<>();

      for (String partition : partitions) {
        dataFiles.addAll(fsView.getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp).map(f -> f.getPath()).collect(Collectors.toList()));
      }

      try {
        DataSource.lookupDataSource(cfg.outputFormat, spark.sessionState().conf());
      } catch (Exception e) {
        LOG.error(String.format("The %s output format is not supported! ", cfg.outputFormat));
        return -1;
      }
      if (!cfg.outputFormat.equalsIgnoreCase("hudi")) {
        // Do transformation
        // A field to do simple Spark repartitioning
        DataFrameWriter<Row> write = null;
        Dataset<Row> original = spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq());
        List<Column> needColumns = Arrays.asList(original.columns()).stream().filter(col -> !col.startsWith("_hoodie_")).map(col -> new Column(col)).collect(Collectors.toList());
        Dataset<Row> reader = original.select(JavaConversions.asScalaIterator(needColumns.iterator()).toSeq());
        if (!StringUtils.isNullOrEmpty(cfg.outputPartitionField)) {
          write = reader.repartition(new Column(cfg.outputPartitionField))
              .write().partitionBy(cfg.outputPartitionField);
        } else {
          write = reader.write();
        }
        write.format(cfg.outputFormat)
            .mode(SaveMode.Overwrite)
            .save(cfg.targetOutputPath);
      } else {
        // No transformation is needed for output format "HUDI", just copy the original files.
        copySnapshot(jsc, fs, cfg, partitions, dataFiles, latestCommitTimestamp, serConf);
      }
    } else {
      LOG.info("The job has 0 partition to copy.");
    }
    return 0;
  }

  private void copySnapshot(JavaSparkContext jsc,
                            FileSystem fs,
                            Config cfg,
                            List<String> partitions,
                            List<String> dataFiles,
                            String latestCommitTimestamp,
                            SerializableConfiguration serConf) throws IOException {
    // Make sure the output directory is empty
    Path outputPath = new Path(cfg.targetOutputPath);
    if (fs.exists(outputPath)) {
      LOG.warn(String.format("The output path %s targetBasePath already exists, deleting", outputPath));
      fs.delete(new Path(cfg.targetOutputPath), true);
    }

    jsc.parallelize(partitions, partitions.size()).flatMap(partition -> {
      // Only take latest version files <= latestCommit.
      FileSystem fs1 = FSUtils.getFs(cfg.sourceBasePath, serConf.newCopy());
      List<Tuple2<String, String>> filePaths = new ArrayList<>();
      dataFiles.forEach(hoodieDataFile -> filePaths.add(new Tuple2<>(partition, hoodieDataFile)));

      // also need to copy over partition metadata
      Path partitionMetaFile =
          new Path(new Path(cfg.sourceBasePath, partition), HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE);
      if (fs1.exists(partitionMetaFile)) {
        filePaths.add(new Tuple2<>(partition, partitionMetaFile.toString()));
      }

      return filePaths.iterator();
    }).foreach(tuple -> {
      String partition = tuple._1();
      Path sourceFilePath = new Path(tuple._2());
      Path toPartitionPath = new Path(cfg.targetOutputPath, partition);
      FileSystem ifs = FSUtils.getFs(cfg.targetOutputPath, serConf.newCopy());

      if (!ifs.exists(toPartitionPath)) {
        ifs.mkdirs(toPartitionPath);
      }
      FileUtil.copy(ifs, sourceFilePath, ifs, new Path(toPartitionPath, sourceFilePath.getName()), false,
          ifs.getConf());
    });

    // Also copy the .commit files
    LOG.info(String.format("Copying .commit files which are no-late-than %s.", latestCommitTimestamp));
    FileStatus[] commitFilesToCopy =
        fs.listStatus(new Path(cfg.sourceBasePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME), (commitFilePath) -> {
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
          new Path(cfg.targetOutputPath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitStatus.getPath().getName());
      if (!fs.exists(targetFilePath.getParent())) {
        fs.mkdirs(targetFilePath.getParent());
      }
      if (fs.exists(targetFilePath)) {
        LOG.error(
            String.format("The target output commit file (%s targetBasePath) already exists.", targetFilePath));
      }
      FileUtil.copy(fs, commitStatus.getPath(), fs, targetFilePath, false, fs.getConf());
    }
  }

  public static void main(String[] args) throws IOException {
    // Take input configs
    final Config cfg = new Config();
    new JCommander(cfg, null, args);

    // Create a spark job to do the snapshot export
    SparkSession spark = SparkSession.builder().appName("Hoodie-snapshot-exporter")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();
    LOG.info("Initializing spark job.");

    HoodieSnapshotExporter hoodieSnapshotExporter = new HoodieSnapshotExporter();
    hoodieSnapshotExporter.export(spark, cfg);

    // Stop the job
    spark.stop();
  }
}
