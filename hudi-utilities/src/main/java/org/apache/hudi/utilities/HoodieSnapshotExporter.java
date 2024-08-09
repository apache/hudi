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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.JavaScalaConverters;
import org.apache.hudi.utilities.exception.HoodieSnapshotExporterException;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.UtilHelpers.buildSparkConf;

/**
 * Export the latest records of Hudi dataset to a set of external files (e.g., plain parquet files).
 */
public class HoodieSnapshotExporter {

  @FunctionalInterface
  public interface Partitioner {

    DataFrameWriter<Row> partition(Dataset<Row> source);

  }

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSnapshotExporter.class);

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

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for file listing")
    public int parallelism = 0;
  }

  public void export(JavaSparkContext jsc, Config cfg) throws IOException {
    FileSystem outputFs = HadoopFSUtils.getFs(cfg.targetOutputPath, jsc.hadoopConfiguration());
    if (outputFs.exists(new Path(cfg.targetOutputPath))) {
      throw new HoodieSnapshotExporterException("The target output path already exists.");
    }

    FileSystem sourceFs = HadoopFSUtils.getFs(cfg.sourceBasePath, jsc.hadoopConfiguration());
    final String latestCommitTimestamp = getLatestCommitTimestamp(sourceFs, cfg)
        .<HoodieSnapshotExporterException>orElseThrow(() -> {
          throw new HoodieSnapshotExporterException("No commits present. Nothing to snapshot.");
        });
    LOG.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.",
        latestCommitTimestamp));

    final HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    final List<String> partitions = getPartitions(engineContext, cfg, new HoodieHadoopStorage(sourceFs));
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

  private Option<String> getLatestCommitTimestamp(FileSystem fs, Config cfg) {
    final HoodieTableMetaClient tableMetadata = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf()))
        .setBasePath(cfg.sourceBasePath).build();
    Option<HoodieInstant> latestCommit = tableMetadata.getActiveTimeline().getWriteTimeline()
        .filterCompletedInstants().lastInstant();
    return latestCommit.isPresent() ? Option.of(latestCommit.get().getTimestamp()) : Option.empty();
  }

  private List<String> getPartitions(HoodieEngineContext engineContext, Config cfg, HoodieStorage storage) {
    return FSUtils.getAllPartitionPaths(engineContext, storage, cfg.sourceBasePath, true, false);
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
      Dataset<Row> hoodieDroppedDataset = dataset.drop(JavaScalaConverters.convertJavaIteratorToScalaIterator(HoodieRecord.HOODIE_META_COLUMNS.iterator()).toSeq());
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

    Dataset<Row> sourceDataset = new SQLContext(jsc).read().parquet(JavaScalaConverters.convertJavaIteratorToScalaIterator(exportingFilePaths).toSeq());
    partitioner.partition(sourceDataset)
        .format(cfg.outputFormat)
        .mode(SaveMode.ErrorIfExists)
        .save(cfg.targetOutputPath);
  }

  private void exportAsHudi(JavaSparkContext jsc, FileSystem sourceFs,
                            Config cfg, List<String> partitions, String latestCommitTimestamp) throws IOException {
    final int parallelism = cfg.parallelism == 0 ? jsc.defaultParallelism() : cfg.parallelism;
    final BaseFileOnlyView fsView = getBaseFileOnlyView(sourceFs, cfg);
    final HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    final StorageConfiguration<?> storageConf = context.getStorageConf();
    context.setJobStatus(this.getClass().getSimpleName(), "Exporting as HUDI dataset");
    List<Pair<String, String>> partitionAndFileList = context.flatMap(partitions, partition -> {
      // Only take latest version files <= latestCommit.
      List<Pair<String, String>> filePaths = fsView
          .getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp)
          .map(f -> Pair.of(partition, f.getPath()))
          .collect(Collectors.toList());
      // also need to copy over partition metadata
      HoodieStorage storage = HoodieStorageUtils.getStorage(cfg.sourceBasePath, storageConf);
      StoragePath partitionMetaFile = HoodiePartitionMetadata.getPartitionMetafilePath(storage,
          FSUtils.constructAbsolutePath(cfg.sourceBasePath, partition)).get();
      if (storage.exists(partitionMetaFile)) {
        filePaths.add(Pair.of(partition, partitionMetaFile.toString()));
      }
      return filePaths.stream();
    }, parallelism);

    context.foreach(partitionAndFileList, partitionAndFile -> {
      String partition = partitionAndFile.getLeft();
      Path sourceFilePath = new Path(partitionAndFile.getRight());
      Path toPartitionPath = HadoopFSUtils.constructAbsolutePathInHadoopPath(cfg.targetOutputPath, partition);
      FileSystem executorSourceFs = HadoopFSUtils.getFs(cfg.sourceBasePath, storageConf.newInstance());
      FileSystem executorOutputFs = HadoopFSUtils.getFs(cfg.targetOutputPath, storageConf.newInstance());

      if (!executorOutputFs.exists(toPartitionPath)) {
        executorOutputFs.mkdirs(toPartitionPath);
      }
      FileUtil.copy(
          executorSourceFs,
          sourceFilePath,
          executorOutputFs,
          new Path(toPartitionPath, sourceFilePath.getName()),
          false,
          true,
          executorOutputFs.getConf());
    }, parallelism);

    // Also copy the .commit files
    LOG.info(String.format("Copying .commit files which are no-late-than %s.", latestCommitTimestamp));
    FileStatus[] commitFilesToCopy =
        sourceFs.listStatus(new Path(cfg.sourceBasePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME), commitFilePath -> {
          if (commitFilePath.getName().equals(HoodieTableConfig.HOODIE_PROPERTIES_FILE)) {
            return true;
          } else {
            String instantTime = FSUtils.getCommitFromCommitFile(commitFilePath.getName());
            return HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, latestCommitTimestamp
            );
          }
        });
    context.foreach(Arrays.asList(commitFilesToCopy), commitFile -> {
      Path targetFilePath =
          new Path(cfg.targetOutputPath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitFile.getPath().getName());
      FileSystem executorSourceFs = HadoopFSUtils.getFs(cfg.sourceBasePath, storageConf.unwrapCopyAs(Configuration.class));
      FileSystem executorOutputFs = HadoopFSUtils.getFs(cfg.targetOutputPath, storageConf.unwrapCopyAs(Configuration.class));

      if (!executorOutputFs.exists(targetFilePath.getParent())) {
        executorOutputFs.mkdirs(targetFilePath.getParent());
      }
      FileUtil.copy(
          executorSourceFs,
          commitFile.getPath(),
          executorOutputFs,
          targetFilePath,
          false,
          true,
          executorOutputFs.getConf());
    }, parallelism);
  }

  private BaseFileOnlyView getBaseFileOnlyView(FileSystem sourceFs, Config cfg) {
    HoodieTableMetaClient tableMetadata = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(sourceFs.getConf()))
        .setBasePath(cfg.sourceBasePath)
        .build();
    return new HoodieTableFileSystemView(tableMetadata, tableMetadata
        .getActiveTimeline().getWriteTimeline().filterCompletedInstants());
  }

  public static void main(String[] args) throws IOException {
    final Config cfg = new Config();
    new JCommander(cfg, null, args);

    SparkConf sparkConf = buildSparkConf("Hoodie-snapshot-exporter", "local[*]");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    LOG.info("Initializing spark job.");

    try {
      new HoodieSnapshotExporter().export(jsc, cfg);
    } finally {
      jsc.stop();
    }
  }
}
