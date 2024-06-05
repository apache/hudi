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

package org.apache.hudi.utilities;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class provides file size updates for the latest files that hudi is consuming. These stats are at table level by default, but
 * specifying --enable-partition-stats will also show stats at the partition level. If a start date (--start-date parameter) and/or
 * end date (--end-date parameter) are specified, stats are based on files that were modified in the half-open interval
 * [start date (--start-date parameter), end date (--end-date parameter)). --num-days parameter can be used to select data files over
 * last --num-days. If --start-date is specified, --num-days will be ignored. If none of the date parameters are set, stats will be
 * computed over all data files of all partitions in the table. Note that date filtering is carried out only if the partition name
 * has the format '[column name=]yyyy-M-d', '[column name=]yyyy/M/d'.
 * <br><br>
 * The following stats are produced by this class:
 * Number of files.
 * Total table size.
 * Minimum file size
 * Maximum file size
 * Average file size
 * Median file size
 * p50 file size
 * p90 file size
 * p95 file size
 * p99 file size
 * <br><br>
 * Sample spark-submit command:
 * ./bin/spark-submit \
 * --class org.apache.hudi.utilities.TableSizeStats \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.14.0-SNAPSHOT.jar \
 * --base-path <base-path> \
 * --num-days <number-of-days>
 */
public class TableSizeStats implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(TableSizeStats.class);

  // Date formatter for parsing partition dates (example: 2023/5/5/ or 2023-5-5).
  private static final DateTimeFormatter DATE_FORMATTER =
      (new DateTimeFormatterBuilder()).appendOptional(DateTimeFormatter.ofPattern("yyyy/M/d")).appendOptional(DateTimeFormatter.ofPattern("yyyy-M-d")).toFormatter();

  // File size stats will be displayed in the units specified below.
  private static final String[] FILE_SIZE_UNITS = {"B", "KB", "MB", "GB", "TB"};

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public TableSizeStats(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--num-days", "-nd"}, description = "Consider files modified within this many days.", required = false)
    public long numDays = 0;

    @Parameter(names = {"--start-date", "-sd"}, description = "Consider files modified on or after this date.", required = false)
    public String startDate = null;

    @Parameter(names = {"--end-date", "-ed"}, description = "Consider files modified before this date.", required = false)
    public String endDate = null;

    @Parameter(names = {"--enable-table-stats", "-fs"}, description = "Show file-level stats.", required = false)
    public boolean tableStats = false;

    @Parameter(names = {"--enable-partition-stats", "-ps"}, description = "Show partition-level stats.", required = false)
    public boolean partitionStats = false;

    @Parameter(names = {"--props-path", "-pp"}, description = "Properties file containing base paths one per line", required = false)
    public String propsFilePath = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "TableSizeStats {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --num-days " + numDays + ", \n"
          + "   --start-date " + startDate + ", \n"
          + "   --end-date " + endDate + ", \n"
          + "   --enable-table-stats " + tableStats + ", \n"
          + "   --enable-partition-stats " + partitionStats + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(numDays, config.numDays)
          && Objects.equals(startDate, config.startDate)
          && Objects.equals(endDate, config.endDate)
          && Objects.equals(tableStats, config.tableStats)
          && Objects.equals(partitionStats, config.partitionStats)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, numDays, startDate, endDate, tableStats, partitionStats, parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Table-Size-Stats", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      TableSizeStats tableSizeStats = new TableSizeStats(jsc, cfg);
      tableSizeStats.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Fetching table size stats ******");

      // Determine starting and ending date intervals for filtering data files.
      LocalDate[] dateInterval = getUserSpecifiedDateInterval(cfg);

      if (cfg.propsFilePath != null) {
        List<String> filePaths = getFilePaths(cfg.propsFilePath, jsc.hadoopConfiguration());
        for (String filePath : filePaths) {
          logTableStats(filePath, dateInterval);
        }
      } else {
        if (cfg.basePath == null) {
          throw new HoodieIOException("Base path needs to be set.");
        }
        logTableStats(cfg.basePath, dateInterval);
      }

    } catch (Exception e) {
      throw new HoodieException("Unable to do fetch table size stats." + cfg.basePath, e);
    }
  }

  private void logTableStats(String basePath, LocalDate[] dateInterval) throws IOException {

    LOG.warn("Processing table " + basePath);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(isMetadataEnabled(basePath, jsc))
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
        engineContext, new HoodieHadoopStorage(basePath, storageConf), metadataConfig, basePath);

    List<String> allPartitions = tableMetadata.getAllPartitionPaths();

    // As a sanity check, throw exception and exit early if date interval is specified, but the first partition does not have
    // date.
    if (dateInterval != null && getPartitionDate(allPartitions.get(0)) == null) {
      throw new HoodieException(
          "Cannot apply --start-date, --end-date, or --num-days when partition does not contain date. Interval: " + Arrays.toString(dateInterval) + ", Partition Name: " + allPartitions.get(0));
    }

    final Histogram tableHistogram = new Histogram(new UniformReservoir(1_000_000));
    allPartitions.forEach(partition -> {
      LocalDate partitionDate = null;
      LocalDate startDate = null;
      LocalDate endDate = null;
      if (dateInterval != null) {
        // Date interval is specified, so try to parse date out of partition name.
        partitionDate = getPartitionDate(partition);
        startDate = dateInterval[0];
        endDate = dateInterval[1];
      }

      // Compute file size stats for all files in this partition if:
      // 1. partition date is null (i.e partition name does not contain a date)
      // 2. both start date and end date are null (not specified).
      // 3. endDate is null (not specified) and partition date is equal to or after startDate.
      // 4. startDate is null (not specified) and partition date is before endDate.
      // 5. startDate and endDate are both specified and partition date lies in the range [startDate, endDate)
      if (partitionDate == null
          || (startDate == null && endDate == null)
          || (endDate == null && (partitionDate.isEqual(startDate) || partitionDate.isAfter(startDate)))
          || (startDate == null && partitionDate.isBefore(endDate))
          || (startDate != null && endDate != null && ((partitionDate.isEqual(startDate) || partitionDate.isAfter(startDate)) && partitionDate.isBefore(endDate)))) {
        HoodieTableMetaClient metaClientLocal = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(storageConf.newInstance()).build();
        HoodieMetadataConfig metadataConfig1 = HoodieMetadataConfig.newBuilder()
            .enable(false)
            .build();
        HoodieTableFileSystemView fileSystemView = FileSystemViewManager
            .createInMemoryFileSystemView(new HoodieLocalEngineContext(storageConf),
                metaClientLocal, metadataConfig1);
        List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList());

        // No need to collect partition level stats if user hasn't requested partition-level stats or if there are no partitions in this table.
        final Histogram partitionHistogram = cfg.partitionStats && partition.trim().length() > 0 ? new Histogram(new UniformReservoir(1_000_000)) : null;
        baseFiles.forEach(baseFile -> {
          // Add file size to histogram since the file was modified within the specified date range.
          if (partitionHistogram != null) {
            partitionHistogram.update(baseFile.getFileSize());
          }

          tableHistogram.update(baseFile.getFileSize());
        });

        // Display file size distribution stats for partition
        if (partitionHistogram != null) {
          logStats("Partition stats [name: " + partition + (partitionDate != null ? ", has date: yes" : "") + "]", partitionHistogram);
        }
      }
    });

    if (cfg.tableStats) {
      // Display file size distribution stats for entire table.
      logStats("Table stats [path: " + basePath + "]", tableHistogram);
    } else {
      // Display only total talbe size
      LOG.info("Total size: {}", getFileSizeUnit(Arrays.stream(tableHistogram.getSnapshot().getValues()).sum()));
    }
  }

  private static boolean isMetadataEnabled(String basePath, JavaSparkContext jsc) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).build();

    Set<String> partitions = metaClient.getTableConfig().getMetadataPartitions();
    return !partitions.isEmpty() && partitions.contains("files");
  }

  private static List<String> getFilePaths(String propsPath, Configuration hadoopConf) {
    List<String> filePaths = new ArrayList<>();
    FileSystem fs = HadoopFSUtils.getFs(
        propsPath,
        Option.ofNullable(hadoopConf).orElseGet(Configuration::new)
    );

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(propsPath)), StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      while (line != null) {
        filePaths.add(line);
        line = reader.readLine();
      }
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs from file." + propsPath);
      throw new HoodieIOException("Cannot read properties from dfs from file " + propsPath, ioe);
    }
    return filePaths;
  }

  private static LocalDate[] getUserSpecifiedDateInterval(Config cfg) {
    // Set endDate to null by default.
    LocalDate endDate = null;
    if (cfg.endDate != null) {
      try {
        endDate = LocalDate.parse(cfg.endDate, DATE_FORMATTER);
        LOG.info("Setting ending date to {}. ", endDate);
      } catch (DateTimeParseException dtpe) {
        throw new HoodieException("Unable to parse --end-date. ", dtpe);
      }
    } else {
      LOG.info("End date is not specified: {}.", endDate);
    }

    // Set startDate to null by default.
    LocalDate startDate = null;

    // Set startDate to cfg.startDate if specified. cfg.startDate takes priority over cfg.numDays if both are specified.
    if (cfg.startDate != null) {
      startDate = LocalDate.parse(cfg.startDate, DATE_FORMATTER);
      LOG.info("Setting starting date to {}.", startDate);
    } else {
      if (cfg.numDays == 0) {
        LOG.info("Start date not specified: {}.", startDate);
      } else if (cfg.numDays > 0) {
        endDate = LocalDate.now();
        startDate = endDate.minusDays(cfg.numDays);
        LOG.info("Setting starting date to {} ({} - {} days). ", startDate, endDate, cfg.numDays);
      } else {
        throw new HoodieException("--num-days must specify a positive value.");
      }
    }

    // Check if starting date is before ending date.
    if (startDate != null && endDate != null && !startDate.isBefore(endDate)) {
      throw new HoodieException("Starting date must be before ending date. Start Date: " + startDate + ", End Date: " + endDate);
    }

    return startDate == null && endDate == null ? null : new LocalDate[]{startDate, endDate};
  }

  @Nullable
  private static LocalDate getPartitionDate(String partition) {
    // Partition name should conform to date format if startDate and/or endDate are specified. Otherwise, we don't
    // need to parse partition name as date.
    String dateString = partition;
    if (partition.contains("=")) {
      // Assume partition date format of "<column>=<date>" and try parsing out date.
      String[] parts = partition.split("=");
      if (parts != null && parts.length == 2) {
        dateString = parts[1].trim();
      }
    }

    LocalDate partitionDate = null;
    try {
      return LocalDate.parse(dateString, DATE_FORMATTER);
    } catch (DateTimeParseException dtpe) {
      LOG.error("Partition name {} must conform to date format if --start-date, --end-date, or --num-days are specified. ", partition, dtpe);
    }
    return partitionDate;
  }

  private static String getFileSizeUnit(double size) {
    int counter = 0;
    while (size > 1024 && counter < FILE_SIZE_UNITS.length) {
      size /= 1024;
      counter++;
    }

    return String.format("%.2f %s", size, FILE_SIZE_UNITS[counter]);
  }

  private static void logStats(String header, Histogram histogram) {
    LOG.info(header);
    Snapshot snapshot = histogram.getSnapshot();
    LOG.info("Number of files: {}", snapshot.size());
    LOG.info("Total size: {}", getFileSizeUnit(Arrays.stream(snapshot.getValues()).sum()));
    LOG.info("Minimum file size: {}", getFileSizeUnit(snapshot.getMin()));
    LOG.info("Maximum file size: {}", getFileSizeUnit(snapshot.getMax()));
    LOG.info("Average file size: {}", getFileSizeUnit(snapshot.getMean()));
    LOG.info("Median file size: {}", getFileSizeUnit(snapshot.getMedian()));
    LOG.info("P50 file size: {}", getFileSizeUnit(snapshot.getValue(0.5)));
    LOG.info("P90 file size: {}", getFileSizeUnit(snapshot.getValue(0.9)));
    LOG.info("P95 file size: {}", getFileSizeUnit(snapshot.getValue(0.95)));
    LOG.info("P99 file size: {}", getFileSizeUnit(snapshot.getValue(0.99)));
  }
}
