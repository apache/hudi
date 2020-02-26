package org.apache.hudi.utilities;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieSnapshotExporter {
  private static final Logger LOG = LogManager.getLogger(HoodieSnapshotExporter.class);

  public static class Config implements Serializable {
    @Parameter(names = {"--source-base-path", "-sbp"}, description = "Base path for the source Hudi dataset to be snapshotted", required = true)
    String sourceBasePath = null;

    @Parameter(names = {"--target-base-path", "-tbp"}, description = "Base path for the target output files (snapshots)", required = true)
    String targetBasePath = null;

    @Parameter(names = {"--snapshot-prefix", "-sp"}, description = "Snapshot prefix or directory under the target base path in order to segregate different snapshots")
    String snapshotPrefix;

    @Parameter(names = {"--output-format", "-of"}, description = "e.g. Hudi or Parquet", required = true)
    String outputFormat;

    @Parameter(names = {"--output-partition-field", "-opf"}, description = "A field to be used by Spark repartitioning")
    String outputPartitionField;

    @Parameter(names = {"--output-partitioner", "-op"}, description = "A class to facilitate custom repartitioning")
    String outputPartitioner;
  }

  public void export(SparkSession spark, Config cfg) throws IOException {
    String sourceBasePath = cfg.sourceBasePath;
    String targetBasePath = cfg.targetBasePath;
    String snapshotPrefix = cfg.snapshotPrefix;
    String outputFormat = cfg.outputFormat;
    String outputPartitionField = cfg.outputPartitionField;
    String outputPartitioner = cfg.outputPartitioner;
    SparkContext jc = spark.sparkContext();
    FileSystem fs = FSUtils.getFs(sourceBasePath, jc.hadoopConfiguration());
//    final HoodieTableMetaClient tableMetadata = new HoodieTableMetaClient(fs.getConf(), sourceBasePath);
//    HoodieTimeline commitTimeline = tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
//    HoodieInstant hoodieInstant = commitTimeline.lastInstant().get();
//    Option<byte[]> instant = commitTimeline.getInstantDetails(hoodieInstant);
//    HoodieCommitMetadata latestMeta = HoodieCommitMetadata
//        .fromBytes(instant.get(), HoodieCommitMetadata.class);
//    HashMap<String, String> fileIdAndFullPaths = latestMeta.getFileIdAndFullPaths(sourceBasePath);
//
//    if (fileIdAndFullPaths.size() <= 0) {
//      spark.stop();
//      System.exit(-1);
//    }
//
//    List<String> files = fileIdAndFullPaths.values().stream().collect(Collectors.toList());
//    if (snapshotPrefix != null && !snapshotPrefix.isEmpty()) {
//      files = fileIdAndFullPaths.values().stream().filter(f -> f.contains(snapshotPrefix)).collect(Collectors.toList());
//    }
//
//    spark.read().parquet(JavaConversions.asScalaIterator(files.iterator()).toSeq()).show();

//    spark.read().parquet(JavaConversions.asScalaIterator(files.iterator()).toSeq()).write().format(outputFormat).mode(SaveMode.Overwrite).save(targetBasePath);

    final HoodieTableMetaClient tableMetadata = new HoodieTableMetaClient(fs.getConf(), sourceBasePath);
    final TableFileSystemView.BaseFileOnlyView fsView = new HoodieTableFileSystemView(tableMetadata,
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    // Get the latest commit
    Option<HoodieInstant> latestCommit =
        tableMetadata.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
    if (!latestCommit.isPresent()) {
      LOG.warn("No commits present. Nothing to snapshot");
      return;
    }
    final String latestCommitTimestamp = latestCommit.get().getTimestamp();
    LOG.info(String.format("Starting to snapshot latest version files which are also no-late-than %s.",
        latestCommitTimestamp));

    List<String> partitions = FSUtils.getAllPartitionPaths(fs, sourceBasePath, false);
    List<String> dataFiles = new ArrayList<>();
    if (StringUtils.isNotBlank(snapshotPrefix)) {
      for (String partition : partitions) {
        if (partition.contains(snapshotPrefix)) {
          dataFiles.addAll(fsView.getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp).map(f -> f.getPath()).collect(Collectors.toList()));
        }
      }
    } else {
      for (String partition : partitions) {
        dataFiles.addAll(fsView.getLatestBaseFilesBeforeOrOn(partition, latestCommitTimestamp).map(f -> f.getPath()).collect(Collectors.toList()));
      }
    }


    if (StringUtils.isNotBlank(outputPartitionField)) {
//      spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq()).repartition(new Column(outputPartitionField)).show(100);
      spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq()).repartition(new Column(outputPartitionField)).write().format(outputFormat).mode(SaveMode.Overwrite).save(targetBasePath);
    } else if (StringUtils.isNotBlank(outputPartitioner)) {
      spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq());
    } else {
//      spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq()).show(100);
      spark.read().parquet(JavaConversions.asScalaIterator(dataFiles.iterator()).toSeq()).write().format(outputFormat).mode(SaveMode.Overwrite).save(targetBasePath);
    }

  }

  public static void main(String[] args) throws IOException {
    // Take input configs
    final Config cfg = new Config();
    new JCommander(cfg, null, args);

    // Create a spark job to do the snapshot copy
    SparkSession spark = SparkSession.builder().appName("Hoodie-snapshot-exporter").master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();
    LOG.info("Initializing spark job.");

    HoodieSnapshotExporter hoodieSnapshotExporter = new HoodieSnapshotExporter();
    hoodieSnapshotExporter.export(spark, cfg);

    // Stop the job
    spark.stop();
  }
}
