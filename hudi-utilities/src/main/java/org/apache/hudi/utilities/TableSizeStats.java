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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.smallfile.SmallFileDetector;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
 * --base-path <base-path> \
 * --num-days <number-of-days>
 */
@Slf4j
public class TableSizeStats implements Serializable {

  private static final long serialVersionUID = 1L;

  // Date formatter for parsing partition dates (example: 2023/5/5/ or 2023-5-5).
  private static final DateTimeFormatter DATE_FORMATTER =
      (new DateTimeFormatterBuilder()).appendOptional(DateTimeFormatter.ofPattern("yyyy/M/d")).appendOptional(DateTimeFormatter.ofPattern("yyyy-M-d")).toFormatter();

  // File size stats will be displayed in the units specified below.
  private static final String[] FILE_SIZE_UNITS = {"B", "KB", "MB", "GB", "TB"};

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private final TypedProperties props;

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

    @Parameter(names = {"--output", "-o"}, description = "Output format: TABLE (default) or JSON.", required = false)
    public String output = "TABLE";

    @Parameter(names = {"--top-n", "-tn"},
        description = "When --enable-partition-stats is set, only show the top N largest partitions (sorted by totalBytes desc). Default 0 = show all.",
        required = false)
    public int topN = 0;

    @Parameter(names = {"--include-row-counts", "-irc"},
        description = "Opt-in: also collect per-partition row counts via Parquet footer reads. Adds numRecords + avgRowSize columns.",
        required = false)
    public boolean includeRowCounts = false;

    @Parameter(names = {"--analyze-table-characteristics", "-atc"},
        description = "Opt-in: run micro-partition / small-file / hot-partition detectors and emit a verdict section.",
        required = false)
    public boolean analyzeTableCharacteristics = false;

    @Parameter(names = {"--print-top-k"},
        description = "When >0 and --analyze-table-characteristics is set, additionally print the top K "
            + "partitions with the smallest avg file size under the micro and small-file detectors — "
            + "an evidence-oriented complement to the table-level verdicts. Default 0 = off.",
        required = false)
    public int printTopK = 0;

    @Parameter(names = {"--micro-partition-count-threshold"},
        description = "Table is flagged micro-partitioned when numPartitions exceeds this. Default 10000.",
        required = false)
    public int microPartitionCountThreshold = 10000;

    @Parameter(names = {"--micro-partition-min-files"},
        description = "Per-partition file count gate for the size-based micro/small-file rule. Default 25.",
        required = false)
    public int microPartitionMinFiles = 25;

    @Parameter(names = {"--micro-partition-max-avg-bytes"},
        description = "Per-partition avg-file-size threshold (bytes) for the size-based rule. Default 52428800 (50 MB).",
        required = false)
    public long microPartitionMaxAvgBytes = 50L * 1024 * 1024;

    @Parameter(names = {"--micro-partition-min-age-days"},
        description = "Minimum table age before applying the size-based micro rule. Default 30.",
        required = false)
    public int microPartitionMinAgeDays = 30;

    @Parameter(names = {"--small-files-min-files-per-partition"},
        description = "Per-partition file count gate for small-files detection — partitions with fewer files are excluded from the prevalence ratio. Default 5.",
        required = false)
    public int smallFilesMinFilesPerPartition = 5;

    @Parameter(names = {"--small-files-threshold-bytes"},
        description = "Avg-file-size threshold (bytes) for flagging a qualifying partition. Default 52428800 (50 MB).",
        required = false)
    public long smallFilesThresholdBytes = 50L * 1024 * 1024;

    @Parameter(names = {"--small-files-moderate-pct"},
        description = "Fraction of qualifying partitions flagged to trigger MODERATE verdict. Default 0.10.",
        required = false)
    public double smallFilesModeratePct = 0.10;

    @Parameter(names = {"--small-files-severe-pct"},
        description = "Fraction of qualifying partitions flagged to trigger SEVERE verdict. Default 0.30.",
        required = false)
    public double smallFilesSeverePct = 0.30;

    @Parameter(names = {"--small-files-min-table-commits"},
        description = "Minimum total ingest commits (active+archived) before emitting the small-file verdict. Default 10.",
        required = false)
    public int smallFilesMinTableCommits = 10;

    @Parameter(names = {"--hot-window-commits"},
        description = "Number of recent ingest commits to scan for hot-partition detection. Default 50.",
        required = false)
    public int hotWindowCommits = 50;

    @Parameter(names = {"--hot-partition-commit-share"},
        description = "Minimum share of the hot window a partition must appear in to be flagged hot. Default 0.5.",
        required = false)
    public double hotPartitionCommitShare = 0.5;

    @Parameter(names = {"--props-path", "-pp"}, description = "Properties file containing base paths one per line", required = false)
    public String propsFilePath = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--enable-hive-support", "-ehs"}, description = "Enables hive support during spark context initialization.", required = false)
    public Boolean enableHiveSupport = false;

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
          + "   --output " + output + ", \n"
          + "   --top-n " + topN + ", \n"
          + "   --include-row-counts " + includeRowCounts + ", \n"
          + "   --analyze-table-characteristics " + analyzeTableCharacteristics + ", \n"
          + "   --micro-partition-count-threshold " + microPartitionCountThreshold + ", \n"
          + "   --micro-partition-min-files " + microPartitionMinFiles + ", \n"
          + "   --micro-partition-max-avg-bytes " + microPartitionMaxAvgBytes + ", \n"
          + "   --micro-partition-min-age-days " + microPartitionMinAgeDays + ", \n"
          + "   --small-files-min-files-per-partition " + smallFilesMinFilesPerPartition + ", \n"
          + "   --small-files-threshold-bytes " + smallFilesThresholdBytes + ", \n"
          + "   --small-files-moderate-pct " + smallFilesModeratePct + ", \n"
          + "   --small-files-severe-pct " + smallFilesSeverePct + ", \n"
          + "   --small-files-min-table-commits " + smallFilesMinTableCommits + ", \n"
          + "   --hot-window-commits " + hotWindowCommits + ", \n"
          + "   --hot-partition-commit-share " + hotPartitionCommitShare + ", \n"
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
          && Objects.equals(output, config.output)
          && Objects.equals(topN, config.topN)
          && Objects.equals(includeRowCounts, config.includeRowCounts)
          && Objects.equals(analyzeTableCharacteristics, config.analyzeTableCharacteristics)
          && Objects.equals(microPartitionCountThreshold, config.microPartitionCountThreshold)
          && Objects.equals(microPartitionMinFiles, config.microPartitionMinFiles)
          && Objects.equals(microPartitionMaxAvgBytes, config.microPartitionMaxAvgBytes)
          && Objects.equals(microPartitionMinAgeDays, config.microPartitionMinAgeDays)
          && Objects.equals(smallFilesMinFilesPerPartition, config.smallFilesMinFilesPerPartition)
          && Objects.equals(smallFilesThresholdBytes, config.smallFilesThresholdBytes)
          && Objects.equals(smallFilesModeratePct, config.smallFilesModeratePct)
          && Objects.equals(smallFilesSeverePct, config.smallFilesSeverePct)
          && Objects.equals(smallFilesMinTableCommits, config.smallFilesMinTableCommits)
          && Objects.equals(hotWindowCommits, config.hotWindowCommits)
          && Objects.equals(hotPartitionCommitShare, config.hotPartitionCommitShare)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, numDays, startDate, endDate, tableStats, partitionStats, output, topN, includeRowCounts,
          analyzeTableCharacteristics, microPartitionCountThreshold, microPartitionMinFiles, microPartitionMaxAvgBytes,
          microPartitionMinAgeDays, smallFilesMinFilesPerPartition, smallFilesThresholdBytes, smallFilesModeratePct,
          smallFilesSeverePct, smallFilesMinTableCommits, hotWindowCommits, hotPartitionCommitShare,
          parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    Map<String, String> sparkConfigMap = new HashMap<>();
    sparkConfigMap.put("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("Table-Size-Stats", cfg.sparkMaster, cfg.enableHiveSupport, sparkConfigMap);

    try {
      TableSizeStats tableSizeStats = new TableSizeStats(jsc, cfg);
      tableSizeStats.run();
    } catch (TableNotFoundException e) {
      log.warn("The Hudi data table is not found: [{}].", cfg.basePath, e);
    } catch (Throwable throwable) {
      log.error("Failed to get table size stats for {}", cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      log.info(cfg.toString());
      log.info(" ****** Fetching table size stats ******");

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

    log.info("Processing table {}", basePath);
    boolean mdtEnabled = isMetadataEnabled(basePath, jsc);
    boolean colStatsAvailable = isColumnStatsMetadataAvailable(basePath, jsc);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(mdtEnabled)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());

    HoodieTableMetaClient metaClientLocal = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storageConf.newInstance()).build();

    // Both tableMetadata and fileSystemView are AutoCloseable — under --props-path batch
    // mode (multiple tables per process) leaving them un-closed leaks MDT readers and
    // file-listing handles per table, which can hit FD / OOM limits over long batches.
    try (HoodieTableMetadata tableMetadata =
             metaClientLocal.getTableFormat().getMetadataFactory().create(
                 engineContext, HoodieStorageUtils.getStorage(basePath, storageConf), metadataConfig, basePath);
         HoodieTableFileSystemView fileSystemView = FileSystemViewManager
             .createInMemoryFileSystemView(new HoodieLocalEngineContext(storageConf),
                 metaClientLocal, metadataConfig)) {

      List<String> allPartitions = tableMetadata.getAllPartitionPaths();

      // As a sanity check, throw exception and exit early if date interval is specified, but the first partition does not have
      // date.
      if (dateInterval != null && !allPartitions.isEmpty() && getPartitionDate(allPartitions.get(0)) == null) {
        throw new HoodieException(
            "Cannot apply --start-date, --end-date, or --num-days when partition does not contain date. Interval: " + Arrays.toString(dateInterval) + ", Partition Name: " + allPartitions.get(0));
      }

      // Build the per-table FileSystemView once and reuse across partitions. The view honors
      // the table's MDT setting so that on large tables we avoid per-partition fs listings.
      // The previous implementation hard-coded enable(false) here, which forced an FS listing
      // for every partition even when MDT was available -- 10-100x slowdown on object stores.
      logTableStatsBody(basePath, dateInterval, metaClientLocal, tableMetadata,
          fileSystemView, mdtEnabled, colStatsAvailable, storageConf,
          allPartitions);
    } catch (HoodieException he) {
      throw he;
    } catch (Exception e) {
      throw new HoodieException("Failure during logTableStats for " + basePath, e);
    }
  }

  /**
   * Per-table stat collection extracted into its own method so the outer
   * {@link #logTableStats} stays focused on resource lifecycle. {@code tableMetadata}
   * and {@code fileSystemView} are owned by the caller's try-with-resources.
   */
  private void logTableStatsBody(String basePath, LocalDate[] dateInterval,
                                 HoodieTableMetaClient metaClientLocal,
                                 HoodieTableMetadata tableMetadata,
                                 HoodieTableFileSystemView fileSystemView,
                                 boolean mdtEnabled, boolean colStatsAvailable,
                                 StorageConfiguration<?> storageConf,
                                 List<String> allPartitions) throws IOException {
    // Collect per-partition rows that pass the date filter so we can sort/aggregate later.
    List<PartitionRow> rows = new ArrayList<>();
    // Table-level reservoirs are one per run, so 1M slots (~8 MB each) is fine and gives
    // accurate p95/p99 on tables with hundreds of thousands of files.
    final Histogram tableSizeHistogram = new Histogram(new UniformReservoir(1_000_000));
    final Histogram tableFileCountHistogram = new Histogram(new UniformReservoir(1_000_000));

    for (String partition : allPartitions) {
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
      boolean inRange = partitionDate == null
          || (startDate == null && endDate == null)
          || (endDate == null && (partitionDate.isEqual(startDate) || partitionDate.isAfter(startDate)))
          || (startDate == null && partitionDate.isBefore(endDate))
          || (startDate != null && endDate != null && ((partitionDate.isEqual(startDate) || partitionDate.isAfter(startDate)) && partitionDate.isBefore(endDate)));
      if (!inRange) {
        continue;
      }
      List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList());

      // The per-partition histogram is only rendered under --enable-partition-stats. Skip the
      // allocation when it isn't needed: UniformReservoir eagerly allocates an AtomicLongArray
      // of its capacity (8 bytes per slot) on construction, so a 1M-slot reservoir is ~8 MB
      // per partition and would explode driver heap on tables with many partitions. When we
      // do build it, use a 4096-slot reservoir — plenty of headroom for the per-partition
      // file count in practice, while keeping the per-partition cost at ~32 KB.
      Histogram partitionSizeHist = cfg.partitionStats ? new Histogram(new UniformReservoir(4096)) : null;
      long partitionTotalBytes = 0L;
      for (HoodieBaseFile baseFile : baseFiles) {
        long size = baseFile.getFileSize();
        if (partitionSizeHist != null) {
          partitionSizeHist.update(size);
        }
        tableSizeHistogram.update(size);
        partitionTotalBytes += size;
      }
      tableFileCountHistogram.update(baseFiles.size());

      Long partitionRowCount = null;
      if (cfg.includeRowCounts && !baseFiles.isEmpty()) {
        partitionRowCount = computeRowCount(partition, baseFiles, tableMetadata,
            colStatsAvailable, storageConf);
      }

      rows.add(new PartitionRow(partition, baseFiles.size(), partitionTotalBytes,
          partitionSizeHist, partitionRowCount));
    }

    // Sort partitions by total bytes descending so output (and --top-n) shows the largest first.
    rows.sort(Comparator.comparingLong((PartitionRow r) -> r.totalBytes).reversed());

    TableCharacteristics characteristics = cfg.analyzeTableCharacteristics
        ? analyzeCharacteristics(basePath, metaClientLocal, rows)
        : null;

    OutputFormat output = parseOutputFormat(cfg.output);
    if (output == OutputFormat.JSON) {
      emitJson(basePath, rows, tableSizeHistogram, tableFileCountHistogram, mdtEnabled, characteristics);
    } else {
      emitTable(basePath, rows, tableSizeHistogram, tableFileCountHistogram, mdtEnabled, characteristics);
    }
  }

  // ---- table characteristics (micro / small / hot detectors) ----------------

  /**
   * Runs the three table-characteristic detectors over the per-partition row set. Each
   * detector's verdict is independent — the umbrella flag {@code --analyze-table-characteristics}
   * gates whether any of them run; thresholds are individually configurable.
   */
  private TableCharacteristics analyzeCharacteristics(String basePath,
                                                      HoodieTableMetaClient metaClient,
                                                      List<PartitionRow> rows) {
    TableCharacteristics tc = new TableCharacteristics();
    tc.tableAgeDays = computeTableAgeDays(metaClient);
    tc.numPartitions = rows.size();

    runMicroPartitionDetector(rows, tc);
    runSmallFileDetector(metaClient, rows, tc);

    try {
      computeHotPartitions(metaClient, tc);
    } catch (Exception e) {
      log.warn("Hot-partition detection failed: " + e.getMessage());
    }
    return tc;
  }

  /**
   * Micro-partition verdict: table is "micro" when partition count exceeds the count threshold,
   * OR (on mature tables) any partition has many small files. The size rule is gated by
   * table age — new tables legitimately have small partitions.
   */
  private void runMicroPartitionDetector(List<PartitionRow> rows, TableCharacteristics tc) {
    boolean countTrigger = tc.numPartitions > cfg.microPartitionCountThreshold;
    boolean sizeTriggerEligible = tc.tableAgeDays >= cfg.microPartitionMinAgeDays;
    int sizeMatches = 0;
    if (sizeTriggerEligible) {
      for (PartitionRow r : rows) {
        long avg = r.fileCount == 0 ? 0 : r.totalBytes / r.fileCount;
        if (r.fileCount >= cfg.microPartitionMinFiles && avg < cfg.microPartitionMaxAvgBytes) {
          sizeMatches++;
        }
      }
    }
    tc.microCountTrigger = countTrigger;
    tc.microSizeTrigger = sizeMatches > 0;
    tc.microSizeMatchCount = sizeMatches;
    tc.microPartitioned = countTrigger || tc.microSizeTrigger;
    // Top-K evidence: partitions with the smallest avg file size among those satisfying the
    // micro file-count gate. Independent of the verdict — useful even when the verdict is "no".
    if (cfg.printTopK > 0) {
      collectTopKSmallest(rows, cfg.microPartitionMinFiles, cfg.printTopK, tc.microTopKSmallest);
    }
  }

  /**
   * Small-file pile-up verdict (CLEAN / MODERATE / SEVERE / SKIPPED) based on per-partition
   * prevalence. Count partitions with >= minFiles as "qualifying"; among those, count partitions
   * with avgFileSize < threshold as "flagged". Verdict thresholds are flagged/qualifying ratio
   * against the moderate/severe pct gates. Skipped when the table has too few commits — the
   * signal is unreliable on freshly-ingested tables.
   */
  private void runSmallFileDetector(HoodieTableMetaClient metaClient,
                                    List<PartitionRow> rows, TableCharacteristics tc) {
    tc.smallFilesThresholdBytes = cfg.smallFilesThresholdBytes;
    tc.smallFileMinFilesPerPartition = cfg.smallFilesMinFilesPerPartition;

    // Delegate the detection math + timeline commit counting to the shared library. Local
    // PartitionRows are lightweight enough that we adapt in place rather than restructure.
    SmallFileDetector.Config sfCfg = SmallFileDetector.Config.builder()
        .minFilesPerPartition(cfg.smallFilesMinFilesPerPartition)
        .thresholdBytes(cfg.smallFilesThresholdBytes)
        .moderatePct(cfg.smallFilesModeratePct)
        .severePct(cfg.smallFilesSeverePct)
        .minTableCommits(cfg.smallFilesMinTableCommits)
        .build();
    List<SmallFileDetector.PartitionStats> stats = new ArrayList<>(rows.size());
    for (PartitionRow r : rows) {
      stats.add(new SmallFileDetector.PartitionStats(r.partition, r.fileCount, r.totalBytes));
    }
    SmallFileDetector.Result result = SmallFileDetector.run(metaClient, stats, sfCfg);

    tc.smallFileTableIngestCommitCount = result.getTableIngestCommitCount();
    tc.smallFileVerdict = result.getVerdict();
    if (tc.smallFileVerdict == SmallFileDetector.Verdict.SKIPPED) {
      // Preserve the original short-circuit: on SKIPPED, downstream output shouldn't advertise
      // qualifying/flagged counts since the young-table gate makes the signal unreliable.
      tc.smallFileQualifyingPartitions = 0;
      tc.smallFileFlaggedPartitions = 0;
      tc.smallFileFlaggedPct = 0.0;
    } else {
      tc.smallFileQualifyingPartitions = (int) result.getQualifyingPartitions();
      tc.smallFileFlaggedPartitions = (int) result.getFlaggedPartitions();
      tc.smallFileFlaggedPct = result.getFlaggedPct();
    }
    // Top-K evidence: partitions with the smallest avg file size among the qualifying set.
    // Kept here (rather than in SmallFileDetector) because it's presentation state — the top-K
    // is only surfaced by the TableSizeStats output emitters, not by external consumers of the
    // detector. Only collect when the table wasn't SKIPPED (matches prior behavior).
    if (cfg.printTopK > 0 && tc.smallFileVerdict != SmallFileDetector.Verdict.SKIPPED) {
      collectTopKSmallest(rows, cfg.smallFilesMinFilesPerPartition, cfg.printTopK, tc.smallFileTopKSmallest);
    }
  }

  /**
   * Collects the top-K partitions with the smallest avg file size among those satisfying the
   * given file-count gate. Populates {@code out} in ascending avg-file-size order.
   */
  private static void collectTopKSmallest(List<PartitionRow> rows, int minFiles, int k,
                                          List<SmallestPartition> out) {
    List<SmallestPartition> qualifying = new ArrayList<>();
    for (PartitionRow r : rows) {
      if (r.fileCount < minFiles) {
        continue;
      }
      long avg = r.fileCount == 0 ? 0 : r.totalBytes / r.fileCount;
      qualifying.add(new SmallestPartition(r.partition, r.fileCount, r.totalBytes, avg));
    }
    qualifying.sort(Comparator.comparingLong((SmallestPartition p) -> p.avgFileSize)
        .thenComparingLong(p -> p.totalBytes));
    for (int i = 0; i < Math.min(k, qualifying.size()); i++) {
      out.add(qualifying.get(i));
    }
  }

  private long computeTableAgeDays(HoodieTableMetaClient metaClient) {
    // hoodie.properties is written once at table creation and never rewritten under normal
    // operation, so its mtime is a stable proxy for table-creation time — and unlike the
    // active timeline's first-instant, it isn't affected by archival. This is important for
    // mature tables whose earliest commits have been archived (activeTimeline.firstInstant()
    // would report a much younger age and silently skip the size-based micro rule).
    try {
      StoragePath propsPath = new StoragePath(metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
      long mtime = metaClient.getStorage().getPathInfo(propsPath).getModificationTime();
      if (mtime > 0) {
        LocalDate created = Instant.ofEpochMilli(mtime).atZone(ZoneId.systemDefault()).toLocalDate();
        return ChronoUnit.DAYS.between(created, LocalDate.now());
      }
    } catch (Exception e) {
      log.warn("Failed to read hoodie.properties mtime for table age, falling back to active timeline: "
          + e.getMessage());
    }
    // Fallback: active timeline's first instant. May underestimate on tables with archived
    // early commits, but preserves prior behavior when the FS stat is unavailable.
    try {
      HoodieActiveTimeline activeTimeline =
          metaClient.getActiveTimeline();
      Option<HoodieInstant> firstOpt =
          activeTimeline.firstInstant();
      if (!firstOpt.isPresent()) {
        return 0;
      }
      String ts = firstOpt.get().requestedTime();
      // Hudi instant times are yyyyMMddHHmmssSSS (17 digits). Parse the date portion.
      if (ts.length() < 8) {
        return 0;
      }
      LocalDate first = LocalDate.parse(ts.substring(0, 8),
          DateTimeFormatter.ofPattern("yyyyMMdd"));
      return ChronoUnit.DAYS.between(first, LocalDate.now());
    } catch (Exception e) {
      log.warn("Failed to compute table age: " + e.getMessage());
      return 0;
    }
  }

  /**
   * Scans the last N completed ingest commits (commit + deltacommit + replacecommit, excluding
   * compaction and clustering operations) and accumulates per-partition commit-frequency / bytes /
   * records, then flags partitions whose participation share exceeds the threshold.
   * Replacecommit is kept because INSERT_OVERWRITE/INSERT_OVERWRITE_TABLE are real ingests;
   * clustering also writes replacecommit but is filtered out by the inner op-type check.
   *
   * <p>Known limitation: for MoR tables this only considers base-file commits — log-file (deltacommit)
   * data shows up here, but per-record log-file write stats are noisier than parquet write stats, so
   * the resulting "hot" scoring favors tables where partitions get rewritten frequently rather than
   * appended-to. For pure log-heavy MoR workloads, the hot-partition signal may under-report; revisit
   * if/when we need to score MoR log-file hotness separately.
   */
  private void computeHotPartitions(HoodieTableMetaClient metaClient, TableCharacteristics tc)
      throws IOException {
    HoodieActiveTimeline timeline =
        metaClient.reloadActiveTimeline();
    List<HoodieInstant> instants =
        candidateIngestInstantsNewestFirst(timeline);

    // agg[partition] = [commitCount, bytesWritten, recordsWritten]
    Map<String, long[]> agg = new LinkedHashMap<>();
    int kept = aggregateHotPartitionCounters(timeline, instants, agg);
    tc.hotWindowEffectiveCommits = kept;
    flagHotPartitions(agg, kept, tc);
  }

  /**
   * Most-recent first. We walk until we've accumulated `hotWindowCommits` post-filter
   * ingest commits (true ingests, not COMPACT/CLUSTER). The window can grow to fill
   * arbitrarily many raw instants on tables where compaction/clustering dominate recent
   * activity — bounded only by the active timeline size.
   *
   * Action filter mirrors {@link SmallFileDetector#countIngestCommits} (commit/deltacommit/replacecommit) so
   * INSERT_OVERWRITE / INSERT_OVERWRITE_TABLE — which write replacecommits and are real
   * ingests — are picked up. Clustering also writes replacecommits, so the caller still
   * needs the WriteOperationType.CLUSTER check to skip those.
   */
  private static List<HoodieInstant>
      candidateIngestInstantsNewestFirst(
          HoodieActiveTimeline timeline) {
    return timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .filter(i -> {
          String a = i.getAction();
          return a.equals(HoodieTimeline.COMMIT_ACTION)
              || a.equals(HoodieTimeline.DELTA_COMMIT_ACTION)
              || a.equals(HoodieTimeline.REPLACE_COMMIT_ACTION);
        })
        .sorted(Comparator.comparing(
            (HoodieInstant i) -> i.requestedTime()).reversed())
        .collect(Collectors.toList());
  }

  /**
   * Walks the candidate instants and accumulates per-partition counters until we have
   * {@code hotWindowCommits} true ingest commits. Returns the actual number kept (the
   * effective window size). Mutates {@code agg} in place.
   */
  private int aggregateHotPartitionCounters(
      HoodieActiveTimeline timeline,
      List<HoodieInstant> instants,
      Map<String, long[]> agg) {
    int kept = 0;
    for (HoodieInstant inst : instants) {
      if (kept >= cfg.hotWindowCommits) {
        break;
      }
      try {
        // readInstantContent reads the instant from storage internally. An earlier
        // `getInstantDetails` call here was a redundant second read on every scanned
        // instant. Empty/corrupt bodies surface as exceptions and are caught below.
        HoodieCommitMetadata cm =
            timeline.readInstantContent(inst,
                HoodieCommitMetadata.class);
        WriteOperationType op = cm.getOperationType();
        // Exclude compaction + clustering — they touch many partitions per commit and
        // would skew hotness toward "every partition is hot".
        if (op == WriteOperationType.COMPACT
            || op == WriteOperationType.CLUSTER) {
          continue;
        }
        kept++;
        Map<String, List<HoodieWriteStat>> p2ws =
            cm.getPartitionToWriteStats();
        if (p2ws == null) {
          continue;
        }
        for (Map.Entry<String, List<HoodieWriteStat>> e :
            p2ws.entrySet()) {
          long bytes = 0;
          long records = 0;
          for (HoodieWriteStat ws : e.getValue()) {
            bytes += ws.getTotalWriteBytes();
            // numWrites is the file's total rows post-write (includes carried-over rows on
            // updates), which double-counts. Use the per-operation counters instead so a
            // commit that touches N rows reports N records, not file-row-count.
            records += ws.getNumInserts() + ws.getNumUpdateWrites() + ws.getNumDeletes();
          }
          long[] cur = agg.computeIfAbsent(e.getKey(), k -> new long[3]);
          cur[0] += 1;
          cur[1] += bytes;
          cur[2] += records;
        }
      } catch (Exception e) {
        // best-effort; an empty/corrupt instant body shouldn't fail the whole forensic run.
        // Log at WARN so the operator sees which instants were skipped and the kept count
        // discrepancy is explainable.
        log.warn("Skipping instant {} during hot-partition scan: {}", inst.requestedTime(), e.toString());
      }
    }
    return kept;
  }

  /**
   * Promotes partitions that appear in at least {@code hotPartitionCommitShare} of the
   * effective window to the {@code tc.hotPartitions} list, sorted by commitCount desc,
   * bytesWritten desc.
   */
  private void flagHotPartitions(Map<String, long[]> agg, int kept,
                                 TableCharacteristics tc) {
    // When no ingest commits fell in the window (e.g., recent activity was all COMPACT/CLUSTER),
    // ceil(share * 0) is 0 and every partition would falsely be flagged as hot. Bail early.
    if (kept == 0) {
      return;
    }
    long hotPartitionMinCommitCount = (long) Math.ceil(cfg.hotPartitionCommitShare * kept);
    for (Map.Entry<String, long[]> e : agg.entrySet()) {
      long[] v = e.getValue();
      if (v[0] >= hotPartitionMinCommitCount) {
        tc.hotPartitions.add(new HotPartition(e.getKey(), v[0], v[1], v[2]));
      }
    }
    tc.hotPartitions.sort(Comparator
        .comparingLong((HotPartition h) -> h.commitCount).reversed()
        .thenComparing(Comparator.comparingLong((HotPartition h) -> h.bytesWritten).reversed()));
  }

  /**
   * Computes the total record count for one partition. Prefers MDT column stats when
   * available (cheap O(1)), falls back to Parquet footer reads per file (expensive).
   * Returns null on any error so callers can render "-" rather than failing the run.
   */
  private Long computeRowCount(String partition, List<HoodieBaseFile> baseFiles,
                               HoodieTableMetadata tableMetadata, boolean colStatsAvailable,
                               StorageConfiguration<?> conf) {
    // Try MDT column stats first if the column_stats partition is available. Use the
    // _hoodie_record_key column — it's required, never null, exactly one per row, so
    // its valueCount equals the row count of the file.
    //
    // Partial-fallback behavior: if col-stats returns data for some files but not others
    // (common right after a write before the index catches up), keep the successful
    // lookups and fall back to footer reads only for the misses. Earlier versions threw
    // away the partial result and re-read every file, costing N footer reads when N-K
    // was enough.
    long total = 0L;
    List<HoodieBaseFile> filesNeedingFooterRead;

    if (colStatsAvailable) {
      filesNeedingFooterRead = new ArrayList<>();
      try {
        List<Pair<String, String>> partFilePairs = new ArrayList<>();
        for (HoodieBaseFile bf : baseFiles) {
          partFilePairs.add(Pair.of(partition, bf.getFileName()));
        }
        Map<Pair<String, String>,
            HoodieMetadataColumnStats> stats =
            tableMetadata.getColumnStats(partFilePairs, "_hoodie_record_key");
        for (HoodieBaseFile bf : baseFiles) {
          Pair<String, String> key =
              Pair.of(partition, bf.getFileName());
          HoodieMetadataColumnStats cs = stats.get(key);
          if (cs == null || cs.getValueCount() == null) {
            filesNeedingFooterRead.add(bf);
          } else {
            total += cs.getValueCount();
          }
        }
      } catch (Exception e) {
        log.warn("MDT col-stats lookup failed for partition " + partition + ", falling back to footer reads: " + e.getMessage());
        // The col-stats batch failed entirely; treat every file as missing.
        total = 0L;
        filesNeedingFooterRead = new ArrayList<>(baseFiles);
      }
    } else {
      filesNeedingFooterRead = baseFiles;
    }

    for (HoodieBaseFile bf : filesNeedingFooterRead) {
      try {
        Long rows = readParquetRowCount(bf.getPath(), conf.unwrapAs(Configuration.class));
        if (rows == null) {
          return null;
        }
        total += rows;
      } catch (Exception e) {
        log.warn("Failed to read row count from " + bf.getPath() + ": " + e.getMessage());
        return null;
      }
    }
    return total;
  }

  /**
   * Reads the record count from a Parquet file's footer. Uses ParquetFileReader's metadata
   * path which only reads the footer (small, end of file), not the row groups themselves.
   */
  private static Long readParquetRowCount(String pathStr, Configuration hadoopConf) throws IOException {
    Path p = new Path(pathStr);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConf))) {
      return reader.getRecordCount();
    }
  }

  // ---- output ---------------------------------------------------------------

  private void emitTable(String basePath, List<PartitionRow> rows,
                         Histogram tableSizeHist, Histogram tableFileCountHist,
                         boolean mdtEnabled, TableCharacteristics characteristics) {
    long tableTotalBytes = rows.stream().mapToLong(r -> r.totalBytes).sum();
    long tableTotalFiles = rows.stream().mapToLong(r -> r.fileCount).sum();
    Long tableTotalRows = null;
    if (cfg.includeRowCounts) {
      long sum = 0L;
      boolean allPresent = !rows.isEmpty();
      for (PartitionRow r : rows) {
        if (r.numRecords == null) {
          allPresent = false;
          break;
        }
        sum += r.numRecords;
      }
      if (allPresent) {
        tableTotalRows = sum;
      }
    }

    System.out.println("Table: " + basePath + "  (MDT=" + (mdtEnabled ? "on" : "off") + ")");
    System.out.println();

    if (cfg.partitionStats && !rows.isEmpty()) {
      List<PartitionRow> visible = cfg.topN > 0 && rows.size() > cfg.topN
          ? rows.subList(0, cfg.topN) : rows;
      String[] headers = cfg.includeRowCounts
          ? new String[]{"partition", "files", "totalBytes", "min", "max", "mean", "p50", "p95", "numRecords", "avgRowSize"}
          : new String[]{"partition", "files", "totalBytes", "min", "max", "mean", "p50", "p95"};
      List<String[]> tableRows = new ArrayList<>(visible.size());
      for (PartitionRow r : visible) {
        Snapshot s = r.sizeHist.getSnapshot();
        if (cfg.includeRowCounts) {
          tableRows.add(new String[]{
              r.partition.isEmpty() ? "(root)" : r.partition,
              Long.toString(r.fileCount),
              formatBytes(r.totalBytes),
              formatBytes((long) s.getMin()),
              formatBytes((long) s.getMax()),
              formatBytes((long) s.getMean()),
              formatBytes((long) s.getMedian()),
              formatBytes((long) s.getValue(0.95)),
              r.numRecords == null ? "-" : Long.toString(r.numRecords),
              r.numRecords == null || r.numRecords == 0 ? "-" : formatBytes(r.totalBytes / r.numRecords)
          });
        } else {
          tableRows.add(new String[]{
              r.partition.isEmpty() ? "(root)" : r.partition,
              Long.toString(r.fileCount),
              formatBytes(r.totalBytes),
              formatBytes((long) s.getMin()),
              formatBytes((long) s.getMax()),
              formatBytes((long) s.getMean()),
              formatBytes((long) s.getMedian()),
              formatBytes((long) s.getValue(0.95))
          });
        }
      }
      printRowsWithHeaders(headers, tableRows);
      if (cfg.topN > 0 && rows.size() > cfg.topN) {
        System.out.println();
        System.out.println("(showing top " + cfg.topN + " of " + rows.size()
            + " partitions by totalBytes; raise --top-n to see more)");
      }
      System.out.println();
    }

    if (cfg.tableStats || !cfg.partitionStats) {
      Snapshot ts = tableSizeHistogram(tableSizeHist);
      System.out.println("Table-level file size distribution:");
      System.out.printf("  numFiles=%d  totalBytes=%s%n", ts.size(), formatBytes(tableTotalBytes));
      System.out.printf("  min=%s  max=%s  mean=%s  median=%s%n",
          formatBytes((long) ts.getMin()),
          formatBytes((long) ts.getMax()),
          formatBytes((long) ts.getMean()),
          formatBytes((long) ts.getMedian()));
      System.out.printf("  p50=%s  p90=%s  p95=%s  p99=%s%n",
          formatBytes((long) ts.getValue(0.5)),
          formatBytes((long) ts.getValue(0.9)),
          formatBytes((long) ts.getValue(0.95)),
          formatBytes((long) ts.getValue(0.99)));
      if (cfg.includeRowCounts && tableTotalRows != null) {
        System.out.printf("  numRecords=%d  avgRowSize=%s%n",
            tableTotalRows,
            tableTotalRows == 0 ? "-" : formatBytes(tableTotalBytes / tableTotalRows));
      }
      System.out.println();

      Snapshot fcs = tableFileCountHist.getSnapshot();
      System.out.println("Per-partition file-count distribution:");
      System.out.printf("  numPartitions=%d  totalFiles=%d%n", rows.size(), tableTotalFiles);
      System.out.printf("  min=%d  max=%d  mean=%.1f  p50=%d  p95=%d  p99=%d%n",
          (long) fcs.getMin(), (long) fcs.getMax(), fcs.getMean(),
          (long) fcs.getValue(0.5), (long) fcs.getValue(0.95), (long) fcs.getValue(0.99));
      System.out.println();

      emitSkewSection(rows, tableTotalBytes);
    }

    if (characteristics != null) {
      emitCharacteristicsTable(characteristics);
    }
  }

  private void emitJson(String basePath, List<PartitionRow> rows,
                        Histogram tableSizeHist, Histogram tableFileCountHist,
                        boolean mdtEnabled, TableCharacteristics characteristics) {
    long tableTotalBytes = rows.stream().mapToLong(r -> r.totalBytes).sum();
    long tableTotalFiles = rows.stream().mapToLong(r -> r.fileCount).sum();
    Long tableTotalRows = null;
    if (cfg.includeRowCounts) {
      long sum = 0L;
      boolean allPresent = !rows.isEmpty();
      for (PartitionRow r : rows) {
        if (r.numRecords == null) {
          allPresent = false;
          break;
        }
        sum += r.numRecords;
      }
      if (allPresent) {
        tableTotalRows = sum;
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"basePath\": ").append(quote(basePath)).append(",\n");
    sb.append("  \"mdtEnabled\": ").append(mdtEnabled).append(",\n");
    sb.append("  \"numPartitions\": ").append(rows.size()).append(",\n");
    sb.append("  \"totalBytes\": ").append(tableTotalBytes).append(",\n");
    sb.append("  \"totalFiles\": ").append(tableTotalFiles);
    if (tableTotalRows != null) {
      sb.append(",\n  \"totalRecords\": ").append(tableTotalRows);
    }
    sb.append(",\n  \"tableSizeStats\": ").append(snapshotToJson(tableSizeHist.getSnapshot()));
    sb.append(",\n  \"fileCountPerPartition\": ").append(snapshotToJson(tableFileCountHist.getSnapshot()));
    sb.append(",\n  \"skew\": ").append(skewToJson(rows, tableTotalBytes));

    if (cfg.partitionStats) {
      List<PartitionRow> visible = cfg.topN > 0 && rows.size() > cfg.topN
          ? rows.subList(0, cfg.topN) : rows;
      sb.append(",\n  \"partitions\": [");
      for (int i = 0; i < visible.size(); i++) {
        PartitionRow r = visible.get(i);
        Snapshot s = r.sizeHist.getSnapshot();
        if (i > 0) {
          sb.append(",");
        }
        sb.append("\n    {\"partition\": ").append(quote(r.partition))
            .append(", \"files\": ").append(r.fileCount)
            .append(", \"totalBytes\": ").append(r.totalBytes)
            .append(", \"sizeStats\": ").append(snapshotToJson(s));
        if (cfg.includeRowCounts) {
          sb.append(", \"numRecords\": ").append(r.numRecords == null ? "null" : r.numRecords.toString());
          if (r.numRecords != null && r.numRecords > 0) {
            sb.append(", \"avgRowSize\": ").append(r.totalBytes / r.numRecords);
          }
        }
        sb.append("}");
      }
      sb.append("\n  ]");
    }
    if (characteristics != null) {
      sb.append(",\n  \"tableCharacteristics\": ").append(characteristicsToJson(characteristics));
    }
    sb.append("\n}");
    System.out.println(sb.toString());
  }

  private Snapshot tableSizeHistogram(Histogram h) {
    return h.getSnapshot();
  }

  /**
   * Emits skew metrics over per-partition total bytes: coefficient of variation, Gini,
   * top-N share, and an outlier list for partitions whose totalBytes exceeds mean+2sigma.
   */
  private void emitSkewSection(List<PartitionRow> rows, long tableTotalBytes) {
    if (rows.size() < 2 || tableTotalBytes == 0) {
      return;
    }
    double[] sizes = rows.stream().mapToDouble(r -> (double) r.totalBytes).toArray();
    double mean = Arrays.stream(sizes).average().orElse(0);
    double var = Arrays.stream(sizes).map(v -> (v - mean) * (v - mean)).average().orElse(0);
    double stdev = Math.sqrt(var);
    double cv = mean == 0 ? 0 : stdev / mean;
    double gini = giniCoefficient(sizes);
    double threshold = mean + 2 * stdev;

    System.out.println("Partition-size skew:");
    System.out.printf("  CV (stdev/mean)        = %.3f%n", cv);
    System.out.printf("  Gini coefficient       = %.3f  (0=equal, 1=one-partition-takes-all)%n", gini);
    System.out.printf("  largest partition share = %.1f%%  (%s)%n",
        100.0 * rows.get(0).totalBytes / tableTotalBytes,
        rows.get(0).partition.isEmpty() ? "(root)" : rows.get(0).partition);
    int topNForShare = Math.min(10, rows.size());
    long topNBytes = 0;
    for (int i = 0; i < topNForShare; i++) {
      topNBytes += rows.get(i).totalBytes;
    }
    System.out.printf("  top-%d partitions share  = %.1f%%%n",
        topNForShare, 100.0 * topNBytes / tableTotalBytes);

    List<PartitionRow> outliers = rows.stream()
        .filter(r -> r.totalBytes > threshold)
        .collect(Collectors.toList());
    if (!outliers.isEmpty()) {
      System.out.printf("  outliers (>mean+2σ = %s): %d partition(s)%n",
          formatBytes((long) threshold), outliers.size());
      int shown = Math.min(outliers.size(), 5);
      for (int i = 0; i < shown; i++) {
        PartitionRow r = outliers.get(i);
        System.out.printf("    %s  totalBytes=%s  files=%d%n",
            r.partition.isEmpty() ? "(root)" : r.partition,
            formatBytes(r.totalBytes), r.fileCount);
      }
      if (outliers.size() > shown) {
        System.out.printf("    ... and %d more%n", outliers.size() - shown);
      }
    }
    System.out.println();
  }

  private void emitCharacteristicsTable(TableCharacteristics tc) {
    System.out.println("Table characteristics:");

    // Micro
    System.out.printf("  Micro-partitioned:  %s%n", tc.microPartitioned ? "YES" : "no");
    if (tc.microCountTrigger) {
      System.out.printf("    count rule:  numPartitions=%d > threshold=%d%n",
          tc.numPartitions, cfg.microPartitionCountThreshold);
    } else {
      System.out.printf("    count rule:  numPartitions=%d (threshold=%d)%n",
          tc.numPartitions, cfg.microPartitionCountThreshold);
    }
    if (tc.tableAgeDays >= cfg.microPartitionMinAgeDays) {
      System.out.printf("    size rule:   %d partition(s) match (files>=%d AND avgSize<%s)  tableAge=%dd%n",
          tc.microSizeMatchCount, cfg.microPartitionMinFiles,
          formatBytes(cfg.microPartitionMaxAvgBytes), tc.tableAgeDays);
    } else {
      System.out.printf("    size rule:   skipped — table age %dd < %dd minimum%n",
          tc.tableAgeDays, cfg.microPartitionMinAgeDays);
    }
    emitTopKSmallest("    top-" + cfg.printTopK + " smallest (files>=" + cfg.microPartitionMinFiles + ")",
        tc.microTopKSmallest);

    // Small files (table-level verdict based on prevalence)
    if (tc.smallFileVerdict == SmallFileDetector.Verdict.SKIPPED) {
      System.out.printf("  Small-file pile-up: SKIPPED (table has only %d ingest commits; need >= %d)%n",
          tc.smallFileTableIngestCommitCount, cfg.smallFilesMinTableCommits);
    } else {
      System.out.printf("  Small-file pile-up: %s%n", tc.smallFileVerdict.name());
      System.out.printf("    %d of %d qualifying partitions flagged (%.1f%%; threshold=%s, min-files=%d)%n",
          tc.smallFileFlaggedPartitions, tc.smallFileQualifyingPartitions,
          100.0 * tc.smallFileFlaggedPct, formatBytes(tc.smallFilesThresholdBytes),
          tc.smallFileMinFilesPerPartition);
      System.out.printf("    moderate >= %.0f%%, severe >= %.0f%%%n",
          100.0 * cfg.smallFilesModeratePct, 100.0 * cfg.smallFilesSeverePct);
      if (tc.smallFileFlaggedPartitions > 0) {
        System.out.println("    (note: MOR log files not counted; base-file-only)");
      }
      emitTopKSmallest("    top-" + cfg.printTopK + " smallest (files>=" + cfg.smallFilesMinFilesPerPartition + ")",
          tc.smallFileTopKSmallest);
    }

    // Hot partitions
    System.out.printf("  Hot partitions (last %d ingest commits, excl. compaction/clustering):%n",
        tc.hotWindowEffectiveCommits);
    if (tc.hotPartitions.isEmpty()) {
      System.out.println("    (none flagged)");
    } else {
      int shown = Math.min(10, tc.hotPartitions.size());
      for (int i = 0; i < shown; i++) {
        HotPartition h = tc.hotPartitions.get(i);
        double pct = tc.hotWindowEffectiveCommits == 0
            ? 0 : 100.0 * h.commitCount / tc.hotWindowEffectiveCommits;
        System.out.printf("    %s  commits=%d/%d (%.0f%%)  bytes=%s  records=%d%n",
            h.partition.isEmpty() ? "(root)" : h.partition,
            h.commitCount, tc.hotWindowEffectiveCommits, pct,
            formatBytes(h.bytesWritten), h.recordsWritten);
      }
      if (tc.hotPartitions.size() > shown) {
        System.out.printf("    ... and %d more%n", tc.hotPartitions.size() - shown);
      }
    }
    System.out.println();
  }

  private static void emitTopKSmallest(String label, List<SmallestPartition> topK) {
    if (topK.isEmpty()) {
      return;
    }
    System.out.printf("%s:%n", label);
    for (SmallestPartition p : topK) {
      System.out.printf("      %s  files=%d  totalBytes=%s  avgSize=%s%n",
          p.partition.isEmpty() ? "(root)" : p.partition,
          p.fileCount, formatBytes(p.totalBytes), formatBytes(p.avgFileSize));
    }
  }

  private static void appendTopKSmallestJson(StringBuilder sb, List<SmallestPartition> topK) {
    sb.append(", \"topKSmallestPartitions\": [");
    for (int i = 0; i < topK.size(); i++) {
      SmallestPartition p = topK.get(i);
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("{\"partition\": ").append(quote(p.partition))
          .append(", \"fileCount\": ").append(p.fileCount)
          .append(", \"totalBytes\": ").append(p.totalBytes)
          .append(", \"avgFileSize\": ").append(p.avgFileSize)
          .append("}");
    }
    sb.append("]");
  }

  private String characteristicsToJson(TableCharacteristics tc) {
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"tableAgeDays\": ").append(tc.tableAgeDays);
    sb.append(", \"numPartitions\": ").append(tc.numPartitions);
    // micro
    sb.append(", \"microPartitioned\": {");
    sb.append("\"verdict\": ").append(tc.microPartitioned);
    sb.append(", \"countRuleTriggered\": ").append(tc.microCountTrigger);
    sb.append(", \"sizeRuleTriggered\": ").append(tc.microSizeTrigger);
    sb.append(", \"sizeMatchCount\": ").append(tc.microSizeMatchCount);
    sb.append(", \"countThreshold\": ").append(cfg.microPartitionCountThreshold);
    sb.append(", \"sizeRuleEligible\": ").append(tc.tableAgeDays >= cfg.microPartitionMinAgeDays);
    appendTopKSmallestJson(sb, tc.microTopKSmallest);
    sb.append("}");
    // small files
    sb.append(", \"smallFiles\": {");
    sb.append("\"verdict\": ").append(quote(tc.smallFileVerdict == null ? "" : tc.smallFileVerdict.name()));
    sb.append(", \"thresholdBytes\": ").append(tc.smallFilesThresholdBytes);
    sb.append(", \"minFilesPerPartition\": ").append(tc.smallFileMinFilesPerPartition);
    sb.append(", \"ingestCommitCount\": ").append(tc.smallFileTableIngestCommitCount);
    sb.append(", \"qualifyingPartitions\": ").append(tc.smallFileQualifyingPartitions);
    sb.append(", \"flaggedPartitions\": ").append(tc.smallFileFlaggedPartitions);
    sb.append(", \"flaggedPct\": ").append(String.format(Locale.ROOT, "%.4f", tc.smallFileFlaggedPct));
    sb.append(", \"moderatePct\": ").append(cfg.smallFilesModeratePct);
    sb.append(", \"severePct\": ").append(cfg.smallFilesSeverePct);
    appendTopKSmallestJson(sb, tc.smallFileTopKSmallest);
    sb.append("}");
    // hot partitions
    sb.append(", \"hotPartitions\": {");
    sb.append("\"windowCommits\": ").append(tc.hotWindowEffectiveCommits);
    sb.append(", \"shareThreshold\": ").append(cfg.hotPartitionCommitShare);
    sb.append(", \"partitions\": [");
    for (int i = 0; i < tc.hotPartitions.size(); i++) {
      HotPartition h = tc.hotPartitions.get(i);
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("{\"partition\": ").append(quote(h.partition))
          .append(", \"commitCount\": ").append(h.commitCount)
          .append(", \"bytesWritten\": ").append(h.bytesWritten)
          .append(", \"recordsWritten\": ").append(h.recordsWritten).append("}");
    }
    sb.append("]}");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Population Gini coefficient over a non-negative vector. Returns 0 for uniform
   * distributions and approaches 1 as the distribution concentrates on one element.
   */
  private static double giniCoefficient(double[] xs) {
    if (xs.length == 0) {
      return 0;
    }
    double[] sorted = xs.clone();
    Arrays.sort(sorted);
    double cum = 0;
    double weighted = 0;
    for (int i = 0; i < sorted.length; i++) {
      cum += sorted[i];
      weighted += sorted[i] * (i + 1);
    }
    if (cum == 0) {
      return 0;
    }
    return (2.0 * weighted) / (sorted.length * cum) - (sorted.length + 1.0) / sorted.length;
  }

  private static OutputFormat parseOutputFormat(String raw) {
    if (raw == null) {
      return OutputFormat.TABLE;
    }
    String upper = raw.trim().toUpperCase(Locale.ROOT);
    if (upper.equals("JSON")) {
      return OutputFormat.JSON;
    }
    if (upper.equals("TABLE")) {
      return OutputFormat.TABLE;
    }
    throw new HoodieException("--output must be TABLE or JSON (got " + raw + ")");
  }

  private static void printRowsWithHeaders(String[] headers, List<String[]> rows) {
    int[] widths = new int[headers.length];
    for (int i = 0; i < headers.length; i++) {
      widths[i] = headers[i].length();
    }
    for (String[] r : rows) {
      for (int i = 0; i < headers.length; i++) {
        if (r[i] != null && r[i].length() > widths[i]) {
          widths[i] = Math.min(r[i].length(), 80);
        }
      }
    }
    printRow(headers, widths);
    StringBuilder rule = new StringBuilder();
    for (int w : widths) {
      for (int i = 0; i < w; i++) {
        rule.append('-');
      }
      rule.append("  ");
    }
    System.out.println(rule);
    for (String[] r : rows) {
      printRow(r, widths);
    }
  }

  private static void printRow(String[] cols, int[] widths) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cols.length; i++) {
      String c = cols[i] == null ? "" : cols[i];
      if (c.length() > widths[i]) {
        c = c.substring(0, widths[i] - 1) + "…";
      }
      sb.append(c);
      for (int p = c.length(); p < widths[i]; p++) {
        sb.append(' ');
      }
      sb.append("  ");
    }
    System.out.println(sb);
  }

  private static String formatBytes(long bytes) {
    return getFileSizeUnit(bytes);
  }

  private static String quote(String s) {
    if (s == null) {
      return "\"\"";
    }
    StringBuilder sb = new StringBuilder(s.length() + 2);
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          sb.append("\\\\");
          break;
        case '"':
          sb.append("\\\"");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        default:
          // Escape remaining C0 controls (0x00..0x1F) via the JSON six-char escape.
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
    return sb.toString();
  }

  private static String snapshotToJson(Snapshot s) {
    return "{\"count\": " + s.size()
        + ", \"min\": " + (long) s.getMin()
        + ", \"max\": " + (long) s.getMax()
        + ", \"mean\": " + (long) s.getMean()
        + ", \"median\": " + (long) s.getMedian()
        + ", \"p50\": " + (long) s.getValue(0.5)
        + ", \"p90\": " + (long) s.getValue(0.9)
        + ", \"p95\": " + (long) s.getValue(0.95)
        + ", \"p99\": " + (long) s.getValue(0.99)
        + "}";
  }

  private static String skewToJson(List<PartitionRow> rows, long tableTotalBytes) {
    if (rows.size() < 2 || tableTotalBytes == 0) {
      return "{\"cv\": 0, \"gini\": 0, \"outliers\": []}";
    }
    double[] sizes = rows.stream().mapToDouble(r -> (double) r.totalBytes).toArray();
    double mean = Arrays.stream(sizes).average().orElse(0);
    double var = Arrays.stream(sizes).map(v -> (v - mean) * (v - mean)).average().orElse(0);
    double stdev = Math.sqrt(var);
    double cv = mean == 0 ? 0 : stdev / mean;
    double gini = giniCoefficient(sizes);
    double threshold = mean + 2 * stdev;
    int topN = Math.min(10, rows.size());
    long topNBytes = 0;
    for (int i = 0; i < topN; i++) {
      topNBytes += rows.get(i).totalBytes;
    }
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"cv\": ").append(String.format(Locale.ROOT, "%.4f", cv));
    sb.append(", \"gini\": ").append(String.format(Locale.ROOT, "%.4f", gini));
    sb.append(", \"largestPartitionShare\": ")
        .append(String.format(Locale.ROOT, "%.4f", (double) rows.get(0).totalBytes / tableTotalBytes));
    sb.append(", \"top").append(topN).append("Share\": ")
        .append(String.format(Locale.ROOT, "%.4f", (double) topNBytes / tableTotalBytes));
    sb.append(", \"outliers\": [");
    boolean first = true;
    for (PartitionRow r : rows) {
      if (r.totalBytes > threshold) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        sb.append("{\"partition\": ").append(quote(r.partition))
            .append(", \"totalBytes\": ").append(r.totalBytes)
            .append(", \"files\": ").append(r.fileCount).append("}");
      }
    }
    sb.append("]}");
    return sb.toString();
  }

  enum OutputFormat { TABLE, JSON }

  /** Aggregated per-partition stats — accumulated during the iteration, sorted for output. */
  static class PartitionRow {
    final String partition;
    final long fileCount;
    final long totalBytes;
    final Histogram sizeHist;
    final Long numRecords;

    PartitionRow(String partition, long fileCount, long totalBytes,
                 Histogram sizeHist, Long numRecords) {
      this.partition = partition;
      this.fileCount = fileCount;
      this.totalBytes = totalBytes;
      this.sizeHist = sizeHist;
      this.numRecords = numRecords;
    }
  }

  /**
   * Result of the {@code --analyze-table-characteristics} detectors. Each verdict block
   * is independent; the umbrella class only collates them for output.
   */
  static class TableCharacteristics {
    // Common context.
    long tableAgeDays;
    int numPartitions;

    // Micro-partition verdict.
    boolean microPartitioned;
    boolean microCountTrigger;
    boolean microSizeTrigger;
    int microSizeMatchCount;
    // Top-K partitions (by lowest avg file size) satisfying the micro size-rule file-count gate.
    // Populated when --print-top-k > 0. Empty otherwise.
    final List<SmallestPartition> microTopKSmallest = new ArrayList<>();

    // Small-files verdict.
    SmallFileDetector.Verdict smallFileVerdict;
    long smallFilesThresholdBytes;
    int smallFileMinFilesPerPartition;
    long smallFileTableIngestCommitCount;
    int smallFileQualifyingPartitions;
    int smallFileFlaggedPartitions;
    double smallFileFlaggedPct;
    // Top-K partitions (by lowest avg file size) among small-file "qualifying" partitions.
    // Populated when --print-top-k > 0. Empty otherwise.
    final List<SmallestPartition> smallFileTopKSmallest = new ArrayList<>();

    // Hot-partition verdict.
    int hotWindowEffectiveCommits;
    final List<HotPartition> hotPartitions = new ArrayList<>();
  }

  /** One partition entry for the top-K smallest listings under the micro / small-file detectors. */
  static class SmallestPartition {
    final String partition;
    final long fileCount;
    final long totalBytes;
    final long avgFileSize;

    SmallestPartition(String partition, long fileCount, long totalBytes, long avgFileSize) {
      this.partition = partition;
      this.fileCount = fileCount;
      this.totalBytes = totalBytes;
      this.avgFileSize = avgFileSize;
    }
  }

  /** One hot-partition row — flagged because it appears in a large share of recent ingests. */
  static class HotPartition {
    final String partition;
    final long commitCount;
    final long bytesWritten;
    final long recordsWritten;

    HotPartition(String partition, long commitCount, long bytesWritten, long recordsWritten) {
      this.partition = partition;
      this.commitCount = commitCount;
      this.bytesWritten = bytesWritten;
      this.recordsWritten = recordsWritten;
    }
  }

  private static boolean isMetadataEnabled(String basePath, JavaSparkContext jsc) {
    return getEnabledMetadataPartitions(basePath, jsc).contains("files");
  }

  /**
   * Returns true when the metadata column_stats partition is enabled, so we can use the
   * MDT-backed fast path for row counts. False when only the files partition is enabled
   * or MDT is off entirely — caller will fall back to Parquet footer reads.
   */
  private static boolean isColumnStatsMetadataAvailable(String basePath, JavaSparkContext jsc) {
    return getEnabledMetadataPartitions(basePath, jsc).contains("column_stats");
  }

  /**
   * Reads the enabled metadata-table partitions from the table config. Returns an empty set
   * when MDT is disabled or the lookup fails (e.g. the base path doesn't have a hoodie meta
   * folder yet) — both detectors above degrade to the slow path on empty.
   */
  private static Set<String> getEnabledMetadataPartitions(String basePath, JavaSparkContext jsc) {
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).build();
      return metaClient.getTableConfig().getMetadataPartitions();
    } catch (Exception ignored) {
      return Collections.emptySet();
    }
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
      log.error("Error reading in properties from dfs from file. {}", propsPath);
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
        log.info("Setting ending date to {}.", endDate);
      } catch (DateTimeParseException dtpe) {
        throw new HoodieException("Unable to parse --end-date. ", dtpe);
      }
    } else {
      log.info("End date is not specified: {}.", endDate);
    }

    // Set startDate to null by default.
    LocalDate startDate = null;

    // Set startDate to cfg.startDate if specified. cfg.startDate takes priority over cfg.numDays if both are specified.
    if (cfg.startDate != null) {
      startDate = LocalDate.parse(cfg.startDate, DATE_FORMATTER);
      log.info("Setting starting date to {}.", startDate);
    } else {
      if (cfg.numDays == 0) {
        log.info("Start date not specified: {}.", startDate);
      } else if (cfg.numDays > 0) {
        endDate = LocalDate.now();
        startDate = endDate.minusDays(cfg.numDays);
        log.info("Setting starting date to {} ({} - {} days). ", startDate, endDate, cfg.numDays);
      } else {
        throw new HoodieException("--num-days must specify a positive value.");
      }
    }

    // Check if starting date is before ending date.
    if (startDate != null && endDate != null && !startDate.isBefore(endDate)) {
      throw new HoodieException("Starting date must be before ending date. Start Date: " + startDate + ", End Date: " + endDate);
    }

    return startDate == null && endDate == null ? null : new LocalDate[] {startDate, endDate};
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
      log.error("Partition name {} must conform to date format if --start-date, --end-date, or --num-days are specified. ", partition, dtpe);
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
}
