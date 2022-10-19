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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.exception.HoodieSnapshotExporterException;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * Export the latest records of Hudi dataset to a set of external files (e.g., plain parquet files).
 */
public class HoodieSnapshotExporter {

  @FunctionalInterface
  public interface Partitioner {

    DataFrameWriter<Row> partition(Dataset<Row> source);

  }

  private static final Logger LOG = LogManager.getLogger(HoodieSnapshotExporter.class);

  public static class OutputFormatValidator implements IValueValidator<String> {

    public static final String HUDI = "hudi";
    public static final List<String> FORMATS = CollectionUtils.createImmutableList("json", "parquet", "orc", HUDI);

    @Override
    public void validate(String name, String value) {
      if (value == null || !FORMATS.contains(value)) {
        throw new ParameterException(
            String.format("Invalid output format: value:%s: supported formats:%s", value, FORMATS));
      }
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--source-base-path"}, description = "Base path for the source Hudi dataset to be snapshotted", required = true)
    public String sourceBasePath;

    @Parameter(names = {"--target-output-path"}, description = "Base path for the target output files (snapshots)", required = true)
    public String targetOutputPath;

    @Parameter(names = {"--output-format"}, description = "Output format for the exported dataset; accept these values: json|parquet|orc|hudi", required = true,
        validateValueWith = OutputFormatValidator.class)
    public String outputFormat;

    @Parameter(names = {"--output-partition-field"}, description = "A field to be used by Spark repartitioning")
    public String outputPartitionField = null;

    @Parameter(names = {"--output-partitioner"}, description = "A class to facilitate custom repartitioning")
    public String outputPartitioner = null;
  }

  public void export(JavaSparkContext jsc, Config cfg) throws IOException {
    FileSystem outputFs = FSUtils.getFs(cfg.targetOutputPath, jsc.hadoopConfiguration());
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    if (outputPathExists(outputFs, cfg)) {
      throw new HoodieSnapshotExporterException("The target output path already exists.");
    }

    FileSystem sourceFs = FSUtils.getFs(cfg.sourceBasePath, jsc.hadoopConfiguration());
    final String latestCommitTimestamp = getLatestCommitTimestamp(sourceFs, cfg)
        .<HoodieSnapshotExporterException>orElseThrow(() -> {
          throw new HoodieSnapshotExporterException("No commits present. Nothing to snapshot.");
        });
    LOG.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.",
        latestCommitTimestamp));

    final List<String> partitions = getPartitions(engineContext, cfg);
    if (partitions.isEmpty()) {
      throw new HoodieSnapshotExporterException("The source dataset has 0 partition to snapshot.");
    }
    LOG.info(String.format("The job needs to export %d partitions.", partitions.size()));

    if (cfg.outputFormat.equals(OutputFormatValidator.HUDI)) {
      exportAsHudi(jsc, sourceFs, cfg, partitions, latestCommitTimestamp);
    } else {
      exportAsNonHudi(jsc, sourceFs, cfg, partitions, latestCommitTimestamp);
    }
    createSuccessTag(outputFs, cfg);
  }

  private boolean outputPathExists(FileSystem fs, Config cfg) throws IOException {
    return fs.exists(new Path(cfg.targetOutputPath));
  }

  private Option<String> getLatestCommitTimestamp(FileSystem fs, Config cfg) {
    final HoodieTableMetaClient tableMetadata = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(cfg.sourceBasePath).build();
    Option<HoodieInstant> latestCommit = tableMetadata.getActiveTimeline().getWriteTimeline()
        .filterCompletedInstants().lastInstant();
    return latestCommit.isPresent() ? Option.of(latestCommit.get().getTimestamp()) : Option.empty();
  }

  private List<String> getPartitions(HoodieEngineContext engineContext, Config cfg) {
    return FSUtils.getAllPartitionPaths(engineContext, cfg.sourceBasePath, true, false);
  }

  private void createSuccessTag(FileSystem fs, Config cfg) throws IOException {
    Path successTagPath = new Path(cfg.targetOutputPath + "/_SUCCESS");
    if (!fs.exists(successTagPath)) {
      LOG.info(String.format("Creating _SUCCESS under target output path: %s", cfg.targetOutputPath));
      fs.createNewFile(successTagPath);
    }
  }

  private void exportAsNonHudi(JavaSparkContext jsc, FileSystem sourceFs,
                               Config cfg, List<String> partitions, String latestCommitTimestamp) {
    Partitioner defaultPartitioner = dataset -> {
      Dataset<Row> hoodieDroppedDataset = dataset.drop(JavaConversions.asScalaIterator(HoodieRecord.HOODIE_META_COLUMNS.iterator()).toSeq());
      return StringUtils.isNullOrEmpty(cfg.outputPartitionField)
          ? hoodieDroppedDataset.write()
          : hoodieDroppedDataset.repartition(new Column(cfg.outputPartitionField)).write().partitionBy(cfg.outputPartitionField);
    };

    Partitioner partitioner = StringUtils.isNullOrEmpty(cfg.outputPartitioner)
        ? defaultPartitioner
        : ReflectionUtils.loadClass(cfg.outputPartitioner);

    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    context.setJobStatus(this.getClass().getSimpleName(), "Exporting as non-HUDI dataset: " + cfg.targetOutputPath);
    final BaseFileOnlyView fsView = getBaseFileOnlyView(sourceFs, cfg);
    Iterator<String> exportingFilePaths = jsc
        .parallelize(partitions, partitions.size())
        .flatMap(partition -> fsView
            .getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp)
            .map(HoodieBaseFile::getPath).iterator())
        .toLocalIterator();

    Dataset<Row> sourceDataset = new SQLContext(jsc).read().parquet(JavaConversions.asScalaIterator(exportingFilePaths).toSeq());
    partitioner.partition(sourceDataset)
        .format(cfg.outputFormat)
        .mode(SaveMode.Overwrite)
        .save(cfg.targetOutputPath);
  }

  private void exportAsHudi(JavaSparkContext jsc, FileSystem sourceFs,
                            Config cfg, List<String> partitions, String latestCommitTimestamp) throws IOException {
    final BaseFileOnlyView fsView = getBaseFileOnlyView(sourceFs, cfg);

    final HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    final SerializableConfiguration serConf = context.getHadoopConf();
    context.setJobStatus(this.getClass().getSimpleName(), "Exporting as HUDI dataset");

    List<Tuple2<String, String>> files = context.flatMap(partitions, partition -> {
      // Only take latest version files <= latestCommit.
      List<Tuple2<String, String>> filePaths = new ArrayList<>();
      Stream<HoodieBaseFile> dataFiles = fsView.getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp);
      dataFiles.forEach(hoodieDataFile -> filePaths.add(new Tuple2<>(partition, hoodieDataFile.getPath())));
      // also need to copy over partition metadata
      FileSystem fs = FSUtils.getFs(cfg.sourceBasePath, serConf.newCopy());
      Path partitionMetaFile = HoodiePartitionMetadata.getPartitionMetafilePath(fs,
          FSUtils.getPartitionPath(cfg.sourceBasePath, partition)).get();
      if (fs.exists(partitionMetaFile)) {
        filePaths.add(new Tuple2<>(partition, partitionMetaFile.toString()));
      }
      return filePaths.stream();
    }, partitions.size());

    context.foreach(files, tuple -> {
      String partition = tuple._1();
      Path sourceFilePath = new Path(tuple._2());
      Path toPartitionPath = FSUtils.getPartitionPath(cfg.targetOutputPath, partition);
      FileSystem executorSourceFs = FSUtils.getFs(cfg.sourceBasePath, serConf.newCopy());
      FileSystem executorOutputFs = FSUtils.getFs(cfg.targetOutputPath, serConf.newCopy());

      if (!executorOutputFs.exists(toPartitionPath)) {
        executorOutputFs.mkdirs(toPartitionPath);
      }
      FileUtil.copy(
          executorSourceFs,
          sourceFilePath,
          executorOutputFs,
          new Path(toPartitionPath, sourceFilePath.getName()),
          false,
          executorOutputFs.getConf());
    }, files.size());

    // Also copy the .commit files
    LOG.info(String.format("Copying .commit files which are no-late-than %s.", latestCommitTimestamp));
    FileSystem outputFs = FSUtils.getFs(cfg.targetOutputPath, jsc.hadoopConfiguration());
    FileStatus[] commitFilesToCopy =
        sourceFs.listStatus(new Path(cfg.sourceBasePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME), (commitFilePath) -> {
          if (commitFilePath.getName().equals(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
            return true;
          } else {
            String instantTime = FSUtils.getCommitFromCommitFile(commitFilePath.getName());
            return HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, latestCommitTimestamp
            );
          }
        });
    for (FileStatus commitStatus : commitFilesToCopy) {
      Path targetFilePath =
          new Path(cfg.targetOutputPath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitStatus.getPath().getName());
      if (!outputFs.exists(targetFilePath.getParent())) {
        outputFs.mkdirs(targetFilePath.getParent());
      }
      if (outputFs.exists(targetFilePath)) {
        LOG.error(
            String.format("The target output commit file (%s targetBasePath) already exists.", targetFilePath));
      }
      FileUtil.copy(sourceFs, commitStatus.getPath(), outputFs, targetFilePath, false, outputFs.getConf());
    }
  }

  private BaseFileOnlyView getBaseFileOnlyView(FileSystem sourceFs, Config cfg) {
    HoodieTableMetaClient tableMetadata = HoodieTableMetaClient.builder()
        .setConf(sourceFs.getConf())
        .setBasePath(cfg.sourceBasePath)
        .build();
    return new HoodieTableFileSystemView(tableMetadata, tableMetadata
        .getActiveTimeline().getWriteTimeline().filterCompletedInstants());
  }

  public static void main(String[] args) throws IOException {
    final Config cfg = new Config();
    new JCommander(cfg, null, args);

    SparkConf sparkConf = new SparkConf().setAppName("Hoodie-snapshot-exporter");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    LOG.info("Initializing spark job.");

    try {
      new HoodieSnapshotExporter().export(jsc, cfg);
    } finally {
      jsc.stop();
    }
  }
}
