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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Analyzes the physical layout of a Hudi table: how bytes and base files are spread across
 * partitions, how skewed that spread is, and whether the layout shows the classic symptoms
 * that make queries and table services slow.
 *
 * <p>The tool is strictly read-only. It reads the timeline and the file listing (through the
 * metadata table when the table has one) and prints a report; it never writes to the table.
 *
 * <h3>What it reports</h3>
 * <ul>
 *   <li>Table-level base-file size distribution (count, total, min / max / mean / median,
 *       p50 / p90 / p95 / p99) and the per-partition file-count distribution.</li>
 *   <li>Partition-size skew: coefficient of variation, Gini coefficient, largest / top-10
 *       partition share, and partitions above mean + 2 sigma.</li>
 *   <li>With {@code --enable-partition-stats}, one row per partition sorted by total bytes
 *       (capped with {@code --top-n}).</li>
 *   <li>With {@code --include-row-counts}, per-partition record counts, taken from the metadata
 *       table's column stats when present and from Parquet footers otherwise.</li>
 *   <li>With {@code --analyze-table-characteristics}, three detectors, each with a verdict,
 *       the thresholds it used, and actionable findings: micro-partitioning, small-file
 *       pile-up (CLEAN / MODERATE / SEVERE / SKIPPED), and hot partitions by recent write
 *       volume.</li>
 * </ul>
 *
 * <p>Date filtering ({@code --num-days}, {@code --start-date}, {@code --end-date}) narrows the
 * partitions considered and only works on tables whose partition names are of the form
 * {@code [column=]yyyy-M-d} or {@code [column=]yyyy/M/d}; the half-open interval
 * {@code [start, end)} is used. {@code --start-date} takes precedence over {@code --num-days}.
 *
 * <h3>Example</h3>
 * <pre>
 * spark-submit --master "local[2]" \
 *   --class org.apache.hudi.utilities.HoodieTableLayoutAnalyzer \
 *   $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-1.3.0-SNAPSHOT.jar \
 *   --base-path s3://bucket/path/to/table \
 *   --enable-partition-stats --analyze-table-characteristics --output JSON
 * </pre>
 */
@Slf4j
public class HoodieTableLayoutAnalyzer implements Serializable {

  private static final long serialVersionUID = 1L;

  // Date formatter for parsing partition dates (example: 2023/5/5/ or 2023-5-5).
  private static final DateTimeFormatter DATE_FORMATTER =
      (new DateTimeFormatterBuilder()).appendOptional(DateTimeFormatter.ofPattern("yyyy/M/d")).appendOptional(DateTimeFormatter.ofPattern("yyyy-M-d")).toFormatter();

  // File size stats will be displayed in the units specified below.
  private static final String[] FILE_SIZE_UNITS = {"B", "KB", "MB", "GB", "TB"};

  // The record key column is required and populated exactly once per row, so its value count
  // in the metadata table's column stats equals the row count of the file.
  private static final String ROW_COUNT_COLUMN = "_hoodie_record_key";

  static final String MICRO_PARTITION_DETECTOR = "micro-partition";
  static final String SMALL_FILES_DETECTOR = "small-files";
  static final String HOT_PARTITIONS_DETECTOR = "hot-partitions";

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private final TypedProperties props;

  public HoodieTableLayoutAnalyzer(JavaSparkContext jsc, Config cfg) {
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

    private static final long serialVersionUID = 1L;

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
        description = "Opt-in: also collect per-partition row counts. Uses metadata table column stats when present, "
            + "Parquet footer reads otherwise. Adds numRecords + avgRowSize columns.",
        required = false)
    public boolean includeRowCounts = false;

    @Parameter(names = {"--analyze-table-characteristics", "-atc"},
        description = "Opt-in: run micro-partition / small-file / hot-partition detectors and emit a verdict section.",
        required = false)
    public boolean analyzeTableCharacteristics = false;

    @Parameter(names = {"--print-top-k"},
        description = "When >0 and --analyze-table-characteristics is set, additionally print the top K "
            + "partitions with the smallest avg file size under the micro and small-file detectors. Default 0 = off.",
        required = false)
    public int printTopK = 0;

    @Parameter(names = {"--micro-partition-count-threshold"},
        description = "Table is flagged micro-partitioned when numPartitions exceeds this. Default 10000.",
        required = false)
    public int microPartitionCountThreshold = 10000;

    @Parameter(names = {"--micro-partition-min-files"},
        description = "Per-partition file count gate for the size-based micro-partition rule. Default 25.",
        required = false)
    public int microPartitionMinFiles = 25;

    @Parameter(names = {"--micro-partition-max-avg-bytes"},
        description = "Per-partition avg-file-size threshold (bytes) for the size-based rule. Default 52428800 (50 MB).",
        required = false)
    public long microPartitionMaxAvgBytes = 50L * 1024 * 1024;

    @Parameter(names = {"--micro-partition-min-age-days"},
        description = "Minimum table age before applying the size-based micro-partition rule. Default 30.",
        required = false)
    public int microPartitionMinAgeDays = 30;

    @Parameter(names = {"--small-files-min-files-per-partition"},
        description = "Per-partition file count gate for small-files detection; partitions with fewer files are excluded from the prevalence ratio. Default 5.",
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

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HoodieTableLayoutAnalyzer {\n"
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
          + "   --print-top-k " + printTopK + ", \n"
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
      return Objects.equals(basePath, config.basePath)
          && Objects.equals(numDays, config.numDays)
          && Objects.equals(startDate, config.startDate)
          && Objects.equals(endDate, config.endDate)
          && Objects.equals(tableStats, config.tableStats)
          && Objects.equals(partitionStats, config.partitionStats)
          && Objects.equals(output, config.output)
          && Objects.equals(topN, config.topN)
          && Objects.equals(includeRowCounts, config.includeRowCounts)
          && Objects.equals(analyzeTableCharacteristics, config.analyzeTableCharacteristics)
          && Objects.equals(printTopK, config.printTopK)
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
          analyzeTableCharacteristics, printTopK, microPartitionCountThreshold, microPartitionMinFiles, microPartitionMaxAvgBytes,
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
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("Table-Layout-Analyzer", cfg.sparkMaster, false, sparkConfigMap);

    try {
      HoodieTableLayoutAnalyzer analyzer = new HoodieTableLayoutAnalyzer(jsc, cfg);
      analyzer.run();
    } catch (TableNotFoundException e) {
      log.warn("The Hudi data table is not found: [{}].", cfg.basePath, e);
    } catch (Throwable throwable) {
      log.error("Failed to analyze table layout for {}", cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      log.info(cfg.toString());
      log.info(" ****** Analyzing table layout ******");

      // Determine starting and ending date intervals for filtering data files.
      LocalDate[] dateInterval = getUserSpecifiedDateInterval(cfg);

      if (cfg.propsFilePath != null) {
        List<String> filePaths = getFilePaths(cfg.propsFilePath, jsc.hadoopConfiguration());
        for (String filePath : filePaths) {
          analyzeTable(filePath, dateInterval);
        }
      } else {
        if (cfg.basePath == null) {
          throw new HoodieIOException("Base path needs to be set.");
        }
        analyzeTable(cfg.basePath, dateInterval);
      }

    } catch (Exception e) {
      throw new HoodieException("Unable to analyze table layout for " + cfg.basePath, e);
    }
  }

  private void analyzeTable(String basePath, LocalDate[] dateInterval) throws IOException {
    log.info("Processing table {}", basePath);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storageConf.newInstance()).build();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    boolean mdtEnabled = tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.FILES);
    boolean colStatsAvailable = tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(mdtEnabled)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Both tableMetadata and fileSystemView are AutoCloseable. Under --props-path batch mode
    // (multiple tables per process) leaving them open leaks metadata readers and file-listing
    // handles per table, which can hit FD / heap limits over long batches.
    //
    // The file system view is built once per table and honors the table's metadata-table
    // setting, so on large tables partitions are listed from the metadata table instead of
    // one storage listing per partition.
    try (HoodieTableMetadata tableMetadata = metaClient.getTableFormat().getMetadataFactory()
             .create(engineContext, metaClient.getStorage(), metadataConfig, basePath);
         HoodieTableFileSystemView fileSystemView = FileSystemViewManager
             .createInMemoryFileSystemView(new HoodieLocalEngineContext(storageConf), metaClient, metadataConfig)) {

      List<String> allPartitions = tableMetadata.getAllPartitionPaths();

      // As a sanity check, throw exception and exit early if date interval is specified, but the first partition does not have
      // date.
      if (dateInterval != null && !allPartitions.isEmpty() && getPartitionDate(allPartitions.get(0)) == null) {
        throw new HoodieException(
            "Cannot apply --start-date, --end-date, or --num-days when partition does not contain date. Interval: " + Arrays.toString(dateInterval) + ", Partition Name: " + allPartitions.get(0));
      }

      List<PartitionRow> rows = new ArrayList<>();
      // Table-level reservoirs are one per run, so 1M slots (~8 MB each) is fine and gives
      // accurate p95/p99 on tables with hundreds of thousands of files.
      final Histogram tableSizeHistogram = new Histogram(new UniformReservoir(1_000_000));
      final Histogram tableFileCountHistogram = new Histogram(new UniformReservoir(1_000_000));
      for (String partition : allPartitions) {
        if (!isPartitionInRange(partition, dateInterval)) {
          continue;
        }
        PartitionRow row = collectPartitionRow(partition, fileSystemView, tableMetadata, colStatsAvailable, storageConf);
        row.sizes.forEach(tableSizeHistogram::update);
        tableFileCountHistogram.update(row.fileCount);
        rows.add(row);
      }

      // Sort partitions by total bytes descending so output (and --top-n) shows the largest first.
      rows.sort(Comparator.comparingLong((PartitionRow r) -> r.totalBytes).reversed());

      TableCharacteristics characteristics = cfg.analyzeTableCharacteristics
          ? analyzeCharacteristics(metaClient, rows)
          : null;

      OutputFormat output = parseOutputFormat(cfg.output);
      if (output == OutputFormat.JSON) {
        emitJson(basePath, rows, tableSizeHistogram, tableFileCountHistogram, mdtEnabled, characteristics);
      } else {
        emitTable(basePath, rows, tableSizeHistogram, tableFileCountHistogram, mdtEnabled, characteristics);
      }
    } catch (HoodieException he) {
      throw he;
    } catch (Exception e) {
      throw new HoodieException("Failure while analyzing table layout for " + basePath, e);
    }
  }

  /**
   * Applies the optional date interval to a partition name. A partition is in range when:
   * 1. no interval is specified, or the partition name does not contain a date;
   * 2. only a start date is specified and the partition date is on or after it;
   * 3. only an end date is specified and the partition date is before it;
   * 4. both are specified and the partition date lies in [startDate, endDate).
   */
  private static boolean isPartitionInRange(String partition, LocalDate[] dateInterval) {
    if (dateInterval == null) {
      return true;
    }
    LocalDate partitionDate = getPartitionDate(partition);
    if (partitionDate == null) {
      return true;
    }
    LocalDate startDate = dateInterval[0];
    LocalDate endDate = dateInterval[1];
    boolean afterStart = startDate == null || !partitionDate.isBefore(startDate);
    boolean beforeEnd = endDate == null || partitionDate.isBefore(endDate);
    return afterStart && beforeEnd;
  }

  private PartitionRow collectPartitionRow(String partition, HoodieTableFileSystemView fileSystemView,
                                           HoodieTableMetadata tableMetadata, boolean colStatsAvailable,
                                           StorageConfiguration<?> storageConf) {
    List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList());

    // The per-partition histogram is only rendered under --enable-partition-stats. Skip the
    // allocation when it isn't needed: UniformReservoir eagerly allocates an AtomicLongArray
    // of its capacity (8 bytes per slot) on construction, so a 1M-slot reservoir is ~8 MB
    // per partition and would explode driver heap on tables with many partitions. When we
    // do build it, use a 4096-slot reservoir, plenty of headroom for the per-partition
    // file count in practice, while keeping the per-partition cost at ~32 KB.
    Histogram partitionSizeHist = cfg.partitionStats ? new Histogram(new UniformReservoir(4096)) : null;
    long partitionTotalBytes = 0L;
    List<Long> sizes = new ArrayList<>(baseFiles.size());
    for (HoodieBaseFile baseFile : baseFiles) {
      long size = baseFile.getFileSize();
      if (partitionSizeHist != null) {
        partitionSizeHist.update(size);
      }
      sizes.add(size);
      partitionTotalBytes += size;
    }

    Long partitionRowCount = null;
    if (cfg.includeRowCounts && !baseFiles.isEmpty()) {
      partitionRowCount = computeRowCount(partition, baseFiles, tableMetadata, colStatsAvailable, storageConf);
    }
    return new PartitionRow(partition, baseFiles.size(), partitionTotalBytes, sizes, partitionSizeHist, partitionRowCount);
  }

  // ---- table characteristics (micro / small / hot detectors) ----------------

  /**
   * Runs the three table-characteristic detectors over the per-partition row set. Each
   * detector's verdict is independent; the umbrella flag {@code --analyze-table-characteristics}
   * gates whether any of them run, and thresholds are individually configurable.
   */
  private TableCharacteristics analyzeCharacteristics(HoodieTableMetaClient metaClient, List<PartitionRow> rows) {
    TableCharacteristics tc = new TableCharacteristics();
    tc.tableType = metaClient.getTableType();
    tc.tableAgeDays = computeTableAgeDays(metaClient);
    tc.numPartitions = rows.size();

    runMicroPartitionDetector(rows, tc);
    runSmallFileDetector(metaClient, rows, tc);

    try {
      computeHotPartitions(metaClient, tc);
    } catch (Exception e) {
      log.warn("Hot-partition detection failed: {}", e.getMessage());
      tc.hotPartitionDetectionFailed = true;
    }
    return tc;
  }

  /**
   * Micro-partition verdict: table is "micro" when partition count exceeds the count threshold,
   * OR (on mature tables) any partition has many small files. The size rule is gated by
   * table age, since new tables legitimately have small partitions.
   */
  private void runMicroPartitionDetector(List<PartitionRow> rows, TableCharacteristics tc) {
    boolean countTrigger = tc.numPartitions > cfg.microPartitionCountThreshold;
    boolean sizeTriggerEligible = tc.tableAgeDays >= cfg.microPartitionMinAgeDays;
    int sizeMatches = 0;
    if (sizeTriggerEligible) {
      for (PartitionRow r : rows) {
        if (r.fileCount >= cfg.microPartitionMinFiles && r.avgFileSize() < cfg.microPartitionMaxAvgBytes) {
          sizeMatches++;
        }
      }
    }
    tc.microCountTrigger = countTrigger;
    tc.microSizeTrigger = sizeMatches > 0;
    tc.microSizeMatchCount = sizeMatches;
    tc.microPartitioned = countTrigger || tc.microSizeTrigger;
    // Top-K evidence: partitions with the smallest avg file size among those satisfying the
    // micro file-count gate. Independent of the verdict; useful even when the verdict is "no".
    if (cfg.printTopK > 0) {
      tc.microTopKSmallest.addAll(collectTopKSmallest(rows, cfg.microPartitionMinFiles, cfg.printTopK));
    }
  }

  /**
   * Small-file pile-up verdict (CLEAN / MODERATE / SEVERE / SKIPPED) based on per-partition
   * prevalence. Count partitions with >= minFiles as "qualifying"; among those, count partitions
   * with avgFileSize < threshold as "flagged". Verdict thresholds are flagged/qualifying ratio
   * against the moderate/severe pct gates. Skipped when the table has too few commits, since
   * the signal is unreliable on freshly-ingested tables.
   */
  private void runSmallFileDetector(HoodieTableMetaClient metaClient, List<PartitionRow> rows, TableCharacteristics tc) {
    long totalIngestCommits = countTotalIngestCommits(metaClient);
    tc.smallFileTableIngestCommitCount = totalIngestCommits;
    if (totalIngestCommits < cfg.smallFilesMinTableCommits) {
      tc.smallFileVerdict = SmallFileVerdict.SKIPPED;
      return;
    }
    int qualifying = 0;
    int flagged = 0;
    for (PartitionRow r : rows) {
      if (r.fileCount < cfg.smallFilesMinFilesPerPartition) {
        continue;
      }
      qualifying++;
      if (r.avgFileSize() < cfg.smallFilesThresholdBytes) {
        flagged++;
      }
    }
    tc.smallFileQualifyingPartitions = qualifying;
    tc.smallFileFlaggedPartitions = flagged;
    tc.smallFileFlaggedPct = qualifying == 0 ? 0.0 : (double) flagged / qualifying;
    tc.smallFileVerdict = classifySmallFileVerdict(qualifying, tc.smallFileFlaggedPct);
    // Top-K evidence: partitions with the smallest avg file size among the qualifying set.
    if (cfg.printTopK > 0) {
      tc.smallFileTopKSmallest.addAll(collectTopKSmallest(rows, cfg.smallFilesMinFilesPerPartition, cfg.printTopK));
    }
  }

  /**
   * Returns the top-K partitions with the smallest avg file size among those satisfying the
   * given file-count gate, in ascending avg-file-size order.
   */
  private static List<SmallestPartition> collectTopKSmallest(List<PartitionRow> rows, int minFiles, int k) {
    List<SmallestPartition> qualifying = new ArrayList<>();
    for (PartitionRow r : rows) {
      if (r.fileCount < minFiles) {
        continue;
      }
      qualifying.add(new SmallestPartition(r.partition, r.fileCount, r.totalBytes, r.avgFileSize()));
    }
    qualifying.sort(Comparator.comparingLong((SmallestPartition p) -> p.avgFileSize)
        .thenComparingLong(p -> p.totalBytes));
    return qualifying.subList(0, Math.min(k, qualifying.size()));
  }

  private SmallFileVerdict classifySmallFileVerdict(int qualifying, double flaggedPct) {
    if (qualifying == 0) {
      return SmallFileVerdict.CLEAN;
    } else if (flaggedPct >= cfg.smallFilesSeverePct) {
      return SmallFileVerdict.SEVERE;
    } else if (flaggedPct >= cfg.smallFilesModeratePct) {
      return SmallFileVerdict.MODERATE;
    }
    return SmallFileVerdict.CLEAN;
  }

  /**
   * Estimates table age in days as the earlier of two cheap signals: the modification time of
   * {@code hoodie.properties} and the first instant on the active timeline. Neither is exact
   * on its own. The properties file is rewritten on upgrades and when metadata partitions
   * change, so its mtime can be later than table creation; the active timeline's first
   * instant moves forward as commits are archived. Taking the earlier of the two never
   * over-estimates age, and is only an under-estimate when both signals are later than the
   * true creation time. The age gates the size-based micro-partition rule, so an
   * under-estimate errs on the side of not flagging.
   */
  private long computeTableAgeDays(HoodieTableMetaClient metaClient) {
    LocalDate today = LocalDate.now();
    long ageDays = 0;
    try {
      StoragePath propsPath = new StoragePath(metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
      long mtime = metaClient.getStorage().getPathInfo(propsPath).getModificationTime();
      if (mtime > 0) {
        LocalDate modified = Instant.ofEpochMilli(mtime).atZone(ZoneId.systemDefault()).toLocalDate();
        ageDays = Math.max(ageDays, ChronoUnit.DAYS.between(modified, today));
      }
    } catch (Exception e) {
      log.warn("Failed to read hoodie.properties modification time for table age: {}", e.getMessage());
    }

    try {
      Option<HoodieInstant> firstOpt = metaClient.getActiveTimeline().firstInstant();
      if (firstOpt.isPresent()) {
        String ts = firstOpt.get().requestedTime();
        // Hudi instant times are yyyyMMddHHmmssSSS. Parse the date portion.
        if (ts.length() >= 8) {
          LocalDate first = LocalDate.parse(ts.substring(0, 8), DateTimeFormatter.ofPattern("yyyyMMdd"));
          ageDays = Math.max(ageDays, ChronoUnit.DAYS.between(first, today));
        }
      }
    } catch (Exception e) {
      log.warn("Failed to derive table age from the active timeline: {}", e.getMessage());
    }
    return ageDays;
  }

  /**
   * Counts total completed ingest commits (commit + deltacommit + replacecommit) across
   * active and archived timelines. Used to gate the small-file verdict: tables with too
   * few commits aren't worth scoring because the signal is dominated by initial-write
   * noise. The archived timeline is only consulted when the active timeline alone is under
   * the gate, to avoid the archive scan on mature tables.
   *
   * <p>Replacecommits from clustering are excluded, since they aren't ingests and would
   * inflate the count on tables with frequent clustering. INSERT_OVERWRITE /
   * INSERT_OVERWRITE_TABLE replacecommits are real ingests and are kept. Distinguishing the
   * two requires reading each replacecommit's metadata, which is only done for the active
   * timeline: the archived timeline is loaded without instant details, so its replacecommits
   * are skipped and the count is a lower bound there.
   */
  private long countTotalIngestCommits(HoodieTableMetaClient metaClient) {
    try {
      long activeCount = countIngestCommits(metaClient.getActiveTimeline(), true);
      if (activeCount >= cfg.smallFilesMinTableCommits) {
        return activeCount;
      }
      // Need archived to see if the table has actually received enough commits.
      long archivedCount = countIngestCommits(metaClient.getArchivedTimeline(), false);
      return activeCount + archivedCount;
    } catch (Exception e) {
      log.warn("Failed to count ingest commits: {}", e.getMessage());
      return 0;
    }
  }

  /**
   * Counts completed ingest commits on a single timeline. Commit/deltacommit are counted
   * unconditionally; each replacecommit is read to distinguish real ingests
   * (INSERT_OVERWRITE / INSERT_OVERWRITE_TABLE) from clustering, which is skipped. When
   * {@code detailsReadable} is false, replacecommits are skipped rather than read.
   */
  private long countIngestCommits(HoodieTimeline timeline, boolean detailsReadable) {
    return timeline.filterCompletedInstants().getInstantsAsStream()
        .filter(i -> {
          String action = i.getAction();
          if (action.equals(HoodieTimeline.COMMIT_ACTION) || action.equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
            return true;
          }
          if (!action.equals(HoodieTimeline.REPLACE_COMMIT_ACTION) || !detailsReadable) {
            return false;
          }
          try {
            return timeline.readCommitMetadata(i).getOperationType() != WriteOperationType.CLUSTER;
          } catch (Exception e) {
            // Best-effort: an unreadable replacecommit body shouldn't fail the whole run.
            // Skip it; undercounting is preferable to misclassifying.
            log.warn("Skipping replacecommit {} during ingest-commit count: {}", i.requestedTime(), e.toString());
            return false;
          }
        }).count();
  }

  /**
   * Scans the last N completed ingest commits (commit + deltacommit + replacecommit, excluding
   * compaction and clustering operations) and accumulates per-partition commit-frequency / bytes /
   * records, then flags partitions whose participation share exceeds the threshold.
   * Replacecommit is kept because INSERT_OVERWRITE/INSERT_OVERWRITE_TABLE are real ingests;
   * clustering also writes replacecommit on older table versions but is filtered out by the
   * operation-type check.
   *
   * <p>Known limitation: write stats for log files (Merge-on-Read delta commits) are noisier
   * than base-file write stats, so the resulting "hot" scoring favors partitions that get
   * rewritten frequently over partitions that are appended to. For log-heavy Merge-on-Read
   * workloads the hot-partition signal may under-report.
   */
  private void computeHotPartitions(HoodieTableMetaClient metaClient, TableCharacteristics tc) {
    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    List<HoodieInstant> instants = candidateIngestInstantsNewestFirst(timeline);

    Map<String, HotPartitionCounters> counters = new LinkedHashMap<>();
    int kept = aggregateHotPartitionCounters(timeline, instants, counters);
    tc.hotWindowEffectiveCommits = kept;
    tc.hotPartitions.addAll(flagHotPartitions(counters, kept));
  }

  /**
   * Most-recent first. We walk until we've accumulated {@code hotWindowCommits} post-filter
   * ingest commits (true ingests, not COMPACT/CLUSTER). The window can grow to fill
   * arbitrarily many raw instants on tables where compaction/clustering dominate recent
   * activity, bounded only by the active timeline size.
   */
  private static List<HoodieInstant> candidateIngestInstantsNewestFirst(HoodieActiveTimeline timeline) {
    return timeline.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .filter(i -> {
          String action = i.getAction();
          return action.equals(HoodieTimeline.COMMIT_ACTION)
              || action.equals(HoodieTimeline.DELTA_COMMIT_ACTION)
              || action.equals(HoodieTimeline.REPLACE_COMMIT_ACTION);
        })
        .sorted(Comparator.comparing(HoodieInstant::requestedTime).reversed())
        .collect(Collectors.toList());
  }

  /**
   * Walks the candidate instants and accumulates per-partition counters until we have
   * {@code hotWindowCommits} true ingest commits. Returns the actual number kept (the
   * effective window size).
   */
  private int aggregateHotPartitionCounters(HoodieActiveTimeline timeline, List<HoodieInstant> instants,
                                            Map<String, HotPartitionCounters> counters) {
    int kept = 0;
    for (HoodieInstant inst : instants) {
      if (kept >= cfg.hotWindowCommits) {
        break;
      }
      try {
        HoodieCommitMetadata cm = timeline.readCommitMetadata(inst);
        WriteOperationType op = cm.getOperationType();
        // Exclude compaction + clustering: they touch many partitions per commit and
        // would skew hotness toward "every partition is hot".
        if (op == WriteOperationType.COMPACT || op == WriteOperationType.CLUSTER) {
          continue;
        }
        kept++;
        Map<String, List<HoodieWriteStat>> partitionToWriteStats = cm.getPartitionToWriteStats();
        if (partitionToWriteStats == null) {
          continue;
        }
        for (Map.Entry<String, List<HoodieWriteStat>> e : partitionToWriteStats.entrySet()) {
          counters.computeIfAbsent(e.getKey(), k -> new HotPartitionCounters()).add(e.getValue());
        }
      } catch (Exception e) {
        // Best-effort: an empty/corrupt instant body shouldn't fail the whole run. Log at WARN
        // so the operator sees which instants were skipped.
        log.warn("Skipping instant {} during hot-partition scan: {}", inst.requestedTime(), e.toString());
      }
    }
    return kept;
  }

  /**
   * Returns partitions that appear in at least {@code hotPartitionCommitShare} of the
   * effective window, sorted by commitCount desc, bytesWritten desc.
   */
  private List<HotPartition> flagHotPartitions(Map<String, HotPartitionCounters> counters, int kept) {
    List<HotPartition> hot = new ArrayList<>();
    // When no ingest commits fell in the window (e.g., recent activity was all COMPACT/CLUSTER),
    // ceil(share * 0) is 0 and every partition would falsely be flagged as hot. Bail early.
    if (kept == 0) {
      return hot;
    }
    long minCommitCount = hotPartitionMinCommitCount(kept);
    for (Map.Entry<String, HotPartitionCounters> e : counters.entrySet()) {
      HotPartitionCounters c = e.getValue();
      if (c.commitCount >= minCommitCount) {
        hot.add(new HotPartition(e.getKey(), c.commitCount, c.bytesWritten, c.recordsWritten));
      }
    }
    hot.sort(Comparator.comparingLong((HotPartition h) -> h.commitCount).reversed()
        .thenComparing(Comparator.comparingLong((HotPartition h) -> h.bytesWritten).reversed()));
    return hot;
  }

  private long hotPartitionMinCommitCount(int effectiveWindow) {
    return (long) Math.ceil(cfg.hotPartitionCommitShare * effectiveWindow);
  }

  /**
   * Computes the total record count for one partition. Prefers metadata-table column stats
   * when available (one lookup per partition), falls back to Parquet footer reads per file.
   * Returns null on any error so callers can render "-" rather than failing the run.
   *
   * <p>Partial-fallback behavior: if column stats return data for some files but not others
   * (common right after a write before the index catches up), the successful lookups are
   * kept and only the misses fall back to footer reads.
   */
  private Long computeRowCount(String partition, List<HoodieBaseFile> baseFiles, HoodieTableMetadata tableMetadata,
                               boolean colStatsAvailable, StorageConfiguration<?> storageConf) {
    long total = 0L;
    List<HoodieBaseFile> filesNeedingFooterRead;

    if (colStatsAvailable) {
      filesNeedingFooterRead = new ArrayList<>();
      try {
        List<Pair<String, String>> partitionFilePairs = new ArrayList<>();
        for (HoodieBaseFile bf : baseFiles) {
          partitionFilePairs.add(Pair.of(partition, bf.getFileName()));
        }
        Map<Pair<String, String>, HoodieMetadataColumnStats> stats = tableMetadata.getColumnStats(partitionFilePairs, ROW_COUNT_COLUMN);
        for (HoodieBaseFile bf : baseFiles) {
          HoodieMetadataColumnStats cs = stats.get(Pair.of(partition, bf.getFileName()));
          if (cs == null || cs.getValueCount() == null) {
            filesNeedingFooterRead.add(bf);
          } else {
            total += cs.getValueCount();
          }
        }
      } catch (Exception e) {
        log.warn("Metadata column-stats lookup failed for partition {}, falling back to footer reads: {}", partition, e.getMessage());
        // The column-stats batch failed entirely; treat every file as missing.
        total = 0L;
        filesNeedingFooterRead = new ArrayList<>(baseFiles);
      }
    } else {
      filesNeedingFooterRead = baseFiles;
    }

    Configuration hadoopConf = storageConf.unwrapAs(Configuration.class);
    for (HoodieBaseFile bf : filesNeedingFooterRead) {
      try {
        total += readParquetRowCount(bf.getPath(), hadoopConf);
      } catch (Exception e) {
        log.warn("Failed to read row count from {}: {}", bf.getPath(), e.getMessage());
        return null;
      }
    }
    return total;
  }

  /**
   * Reads the record count from a Parquet file's footer. Only the footer is read, not the row
   * groups themselves.
   */
  private static long readParquetRowCount(String pathStr, Configuration hadoopConf) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathStr), hadoopConf))) {
      return reader.getRecordCount();
    }
  }

  // ---- output ---------------------------------------------------------------

  /**
   * Sums per-partition record counts; null when row counts were not requested, or when any
   * partition's count is unknown (so a partial sum is never reported as a table total).
   */
  private Long tableTotalRows(List<PartitionRow> rows) {
    if (!cfg.includeRowCounts || rows.isEmpty()) {
      return null;
    }
    long sum = 0L;
    for (PartitionRow r : rows) {
      if (r.numRecords == null) {
        return null;
      }
      sum += r.numRecords;
    }
    return sum;
  }

  private List<PartitionRow> visibleRows(List<PartitionRow> rows) {
    return cfg.topN > 0 && rows.size() > cfg.topN ? rows.subList(0, cfg.topN) : rows;
  }

  private void emitTable(String basePath, List<PartitionRow> rows,
                         Histogram tableSizeHist, Histogram tableFileCountHist,
                         boolean mdtEnabled, TableCharacteristics characteristics) {
    long tableTotalBytes = rows.stream().mapToLong(r -> r.totalBytes).sum();
    long tableTotalFiles = rows.stream().mapToLong(r -> r.fileCount).sum();
    Long tableTotalRows = tableTotalRows(rows);

    System.out.println("Table: " + basePath + "  (MDT=" + (mdtEnabled ? "on" : "off") + ")");
    System.out.println();

    if (cfg.partitionStats && !rows.isEmpty()) {
      emitPartitionTable(rows);
    }

    if (cfg.tableStats || !cfg.partitionStats) {
      Snapshot ts = tableSizeHist.getSnapshot();
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
      if (tableTotalRows != null) {
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

  private void emitPartitionTable(List<PartitionRow> rows) {
    List<PartitionRow> visible = visibleRows(rows);
    String[] headers = cfg.includeRowCounts
        ? new String[]{"partition", "files", "totalBytes", "min", "max", "mean", "p50", "p95", "numRecords", "avgRowSize"}
        : new String[]{"partition", "files", "totalBytes", "min", "max", "mean", "p50", "p95"};
    List<String[]> tableRows = new ArrayList<>(visible.size());
    for (PartitionRow r : visible) {
      Snapshot s = r.sizeHist.getSnapshot();
      List<String> cols = new ArrayList<>(Arrays.asList(
          displayName(r.partition),
          Long.toString(r.fileCount),
          formatBytes(r.totalBytes),
          formatBytes((long) s.getMin()),
          formatBytes((long) s.getMax()),
          formatBytes((long) s.getMean()),
          formatBytes((long) s.getMedian()),
          formatBytes((long) s.getValue(0.95))));
      if (cfg.includeRowCounts) {
        cols.add(r.numRecords == null ? "-" : Long.toString(r.numRecords));
        cols.add(r.numRecords == null || r.numRecords == 0 ? "-" : formatBytes(r.totalBytes / r.numRecords));
      }
      tableRows.add(cols.toArray(new String[0]));
    }
    printRowsWithHeaders(headers, tableRows);
    if (cfg.topN > 0 && rows.size() > cfg.topN) {
      System.out.println();
      System.out.println("(showing top " + cfg.topN + " of " + rows.size()
          + " partitions by totalBytes; raise --top-n to see more)");
    }
    System.out.println();
  }

  private void emitJson(String basePath, List<PartitionRow> rows,
                        Histogram tableSizeHist, Histogram tableFileCountHist,
                        boolean mdtEnabled, TableCharacteristics characteristics) {
    long tableTotalBytes = rows.stream().mapToLong(r -> r.totalBytes).sum();
    long tableTotalFiles = rows.stream().mapToLong(r -> r.fileCount).sum();
    Long tableTotalRows = tableTotalRows(rows);

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
      List<PartitionRow> visible = visibleRows(rows);
      sb.append(",\n  \"partitions\": [");
      for (int i = 0; i < visible.size(); i++) {
        PartitionRow r = visible.get(i);
        if (i > 0) {
          sb.append(",");
        }
        sb.append("\n    {\"partition\": ").append(quote(r.partition))
            .append(", \"files\": ").append(r.fileCount)
            .append(", \"totalBytes\": ").append(r.totalBytes)
            .append(", \"sizeStats\": ").append(snapshotToJson(r.sizeHist.getSnapshot()));
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
    System.out.println(sb);
  }

  /**
   * Emits skew metrics over per-partition total bytes: coefficient of variation, Gini,
   * top-N share, and an outlier list for partitions whose totalBytes exceeds mean+2sigma.
   */
  private void emitSkewSection(List<PartitionRow> rows, long tableTotalBytes) {
    if (rows.size() < 2 || tableTotalBytes == 0) {
      return;
    }
    SkewMetrics skew = SkewMetrics.of(rows);

    System.out.println("Partition-size skew:");
    System.out.printf("  CV (stdev/mean)        = %.3f%n", skew.cv);
    System.out.printf("  Gini coefficient       = %.3f  (0=equal, 1=one-partition-takes-all)%n", skew.gini);
    System.out.printf("  largest partition share = %.1f%%  (%s)%n",
        100.0 * rows.get(0).totalBytes / tableTotalBytes, displayName(rows.get(0).partition));
    System.out.printf("  top-%d partitions share  = %.1f%%%n",
        skew.topN, 100.0 * skew.topNBytes / tableTotalBytes);

    List<PartitionRow> outliers = skew.outliers(rows);
    if (!outliers.isEmpty()) {
      System.out.printf("  outliers (>mean+2σ = %s): %d partition(s)%n",
          formatBytes((long) skew.outlierThreshold), outliers.size());
      int shown = Math.min(outliers.size(), 5);
      for (int i = 0; i < shown; i++) {
        PartitionRow r = outliers.get(i);
        System.out.printf("    %s  totalBytes=%s  files=%d%n", displayName(r.partition), formatBytes(r.totalBytes), r.fileCount);
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
      System.out.printf("    count rule:  numPartitions=%d > threshold=%d%n", tc.numPartitions, cfg.microPartitionCountThreshold);
    } else {
      System.out.printf("    count rule:  numPartitions=%d (threshold=%d)%n", tc.numPartitions, cfg.microPartitionCountThreshold);
    }
    if (tc.sizeRuleEligible(cfg)) {
      System.out.printf("    size rule:   %d partition(s) match (files>=%d AND avgSize<%s)  tableAge=%dd%n",
          tc.microSizeMatchCount, cfg.microPartitionMinFiles, formatBytes(cfg.microPartitionMaxAvgBytes), tc.tableAgeDays);
    } else {
      System.out.printf("    size rule:   skipped, table age %dd < %dd minimum%n", tc.tableAgeDays, cfg.microPartitionMinAgeDays);
    }
    emitTopKSmallest("    top-" + cfg.printTopK + " smallest (files>=" + cfg.microPartitionMinFiles + ")", tc.microTopKSmallest);

    // Small files (table-level verdict based on prevalence)
    if (tc.smallFileVerdict == SmallFileVerdict.SKIPPED) {
      System.out.printf("  Small-file pile-up: SKIPPED (table has only %d ingest commits; need >= %d)%n",
          tc.smallFileTableIngestCommitCount, cfg.smallFilesMinTableCommits);
    } else {
      System.out.printf("  Small-file pile-up: %s%n", tc.smallFileVerdict.name());
      System.out.printf("    %d of %d qualifying partitions flagged (%.1f%%; threshold=%s, min-files=%d)%n",
          tc.smallFileFlaggedPartitions, tc.smallFileQualifyingPartitions,
          100.0 * tc.smallFileFlaggedPct, formatBytes(cfg.smallFilesThresholdBytes), cfg.smallFilesMinFilesPerPartition);
      System.out.printf("    moderate >= %.0f%%, severe >= %.0f%%%n", 100.0 * cfg.smallFilesModeratePct, 100.0 * cfg.smallFilesSeverePct);
      if (tc.smallFileFlaggedPartitions > 0) {
        System.out.println("    (note: MOR log files not counted; base-file-only)");
      }
      emitTopKSmallest("    top-" + cfg.printTopK + " smallest (files>=" + cfg.smallFilesMinFilesPerPartition + ")", tc.smallFileTopKSmallest);
    }

    // Hot partitions
    System.out.printf("  Hot partitions (last %d ingest commits, excl. compaction/clustering):%n", tc.hotWindowEffectiveCommits);
    if (tc.hotPartitions.isEmpty()) {
      System.out.println("    (none flagged)");
    } else {
      int shown = Math.min(10, tc.hotPartitions.size());
      for (int i = 0; i < shown; i++) {
        HotPartition h = tc.hotPartitions.get(i);
        System.out.printf("    %s  commits=%d/%d (%.0f%%)  bytes=%s  records=%d%n",
            displayName(h.partition), h.commitCount, tc.hotWindowEffectiveCommits,
            h.commitSharePct(tc.hotWindowEffectiveCommits), formatBytes(h.bytesWritten), h.recordsWritten);
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
          displayName(p.partition), p.fileCount, formatBytes(p.totalBytes), formatBytes(p.avgFileSize));
    }
  }

  // ---- detector verdicts as JSON --------------------------------------------

  /**
   * Renders the detector section. Each detector is one entry of {@code detectors[]} with a
   * {@code status}, a one-line {@code summary}, {@code findings[]} written as self-contained
   * actionable sentences, and {@code effectiveConfigs} where {@code effective.*} keys are the
   * thresholds the detector used and {@code observed.*} keys are what it measured.
   */
  private String characteristicsToJson(TableCharacteristics tc) {
    StringBuilder sb = new StringBuilder("{");
    sb.append("\n    \"tableAgeDays\": ").append(tc.tableAgeDays);
    sb.append(",\n    \"numPartitions\": ").append(tc.numPartitions);
    sb.append(",\n    \"overallStatus\": ").append(quote(tc.overallStatus().name()));
    sb.append(",\n    \"detectors\": [");
    sb.append("\n      ").append(microPartitionDetectorJson(tc));
    sb.append(",\n      ").append(smallFilesDetectorJson(tc));
    sb.append(",\n      ").append(hotPartitionsDetectorJson(tc));
    sb.append("\n    ]");
    sb.append("\n  }");
    return sb.toString();
  }

  private String microPartitionDetectorJson(TableCharacteristics tc) {
    boolean sizeRuleEligible = tc.sizeRuleEligible(cfg);
    String sizeThreshold = formatBytes(cfg.microPartitionMaxAvgBytes);

    List<String> summaryParts = new ArrayList<>();
    List<String> findings = new ArrayList<>();
    if (tc.microCountTrigger) {
      summaryParts.add(tc.numPartitions + " partitions exceed the count threshold of " + cfg.microPartitionCountThreshold);
      findings.add("Table has " + tc.numPartitions + " partitions, above the threshold of " + cfg.microPartitionCountThreshold
          + "; revisit the partitioning scheme (coarser partition columns, or bucketing / clustering on the column instead of "
          + "partitioning by it) so each partition holds enough data to fill full-size files.");
    } else {
      summaryParts.add(tc.numPartitions + " partitions (count threshold " + cfg.microPartitionCountThreshold + ")");
    }
    if (!sizeRuleEligible) {
      summaryParts.add("size rule skipped because the table is " + tc.tableAgeDays + " days old (needs " + cfg.microPartitionMinAgeDays + ")");
    } else if (tc.microSizeTrigger) {
      summaryParts.add(tc.microSizeMatchCount + " partitions hold >= " + cfg.microPartitionMinFiles
          + " base files averaging under " + sizeThreshold);
      findings.add(tc.microSizeMatchCount + " partitions hold at least " + cfg.microPartitionMinFiles
          + " base files averaging under " + sizeThreshold + " on a table " + tc.tableAgeDays
          + " days old; run clustering (HoodieClusteringJob) to merge them, and check that the writer's small-file limit "
          + "and max file size let ingestion bin-pack new records into existing files.");
    } else {
      summaryParts.add("no partition holds >= " + cfg.microPartitionMinFiles + " base files averaging under " + sizeThreshold);
    }

    StringBuilder sb = new StringBuilder("{");
    sb.append("\"name\": ").append(quote(MICRO_PARTITION_DETECTOR));
    sb.append(", \"status\": ").append(quote(tc.microPartitioned ? DetectorStatus.FLAGGED.name() : DetectorStatus.CLEAN.name()));
    sb.append(", \"summary\": ").append(quote(String.join("; ", summaryParts) + "."));
    sb.append(", \"findings\": ").append(stringArrayToJson(findings));
    sb.append(", \"effectiveConfigs\": {");
    sb.append("\"effective.micro.partition.count.threshold\": ").append(cfg.microPartitionCountThreshold);
    sb.append(", \"effective.micro.partition.min.files\": ").append(cfg.microPartitionMinFiles);
    sb.append(", \"effective.micro.partition.max.avg.bytes\": ").append(cfg.microPartitionMaxAvgBytes);
    sb.append(", \"effective.micro.partition.min.age.days\": ").append(cfg.microPartitionMinAgeDays);
    sb.append(", \"observed.num.partitions\": ").append(tc.numPartitions);
    sb.append(", \"observed.table.age.days\": ").append(tc.tableAgeDays);
    sb.append(", \"observed.count.rule.triggered\": ").append(tc.microCountTrigger);
    sb.append(", \"observed.size.rule.eligible\": ").append(sizeRuleEligible);
    sb.append(", \"observed.size.rule.triggered\": ").append(tc.microSizeTrigger);
    sb.append(", \"observed.size.rule.matching.partitions\": ").append(tc.microSizeMatchCount);
    sb.append("}");
    appendTopKSmallestJson(sb, tc.microTopKSmallest);
    sb.append("}");
    return sb.toString();
  }

  private String smallFilesDetectorJson(TableCharacteristics tc) {
    String threshold = formatBytes(cfg.smallFilesThresholdBytes);
    String flaggedPct = String.format(Locale.ROOT, "%.1f%%", 100.0 * tc.smallFileFlaggedPct);
    String summary;
    List<String> findings = new ArrayList<>();
    if (tc.smallFileVerdict == SmallFileVerdict.SKIPPED) {
      summary = "Skipped: table has only " + tc.smallFileTableIngestCommitCount + " ingest commits (needs >= "
          + cfg.smallFilesMinTableCommits + ") so the small-file signal is not yet reliable.";
    } else {
      summary = tc.smallFileFlaggedPartitions + " of " + tc.smallFileQualifyingPartitions + " partitions with >= "
          + cfg.smallFilesMinFilesPerPartition + " base files average under " + threshold + " (" + flaggedPct
          + "; MODERATE at >= " + String.format(Locale.ROOT, "%.0f%%", 100.0 * cfg.smallFilesModeratePct)
          + ", SEVERE at >= " + String.format(Locale.ROOT, "%.0f%%", 100.0 * cfg.smallFilesSeverePct) + ").";
      if (tc.smallFileVerdict != SmallFileVerdict.CLEAN) {
        findings.add(tc.smallFileFlaggedPartitions + " of " + tc.smallFileQualifyingPartitions + " qualifying partitions (" + flaggedPct
            + ") average under " + threshold + " per base file; run clustering (HoodieClusteringJob) to merge small files, and review "
            + "hoodie.parquet.small.file.limit and hoodie.parquet.max.file.size on the writer so new writes bin-pack into existing files.");
      }
      if (tc.smallFileFlaggedPartitions > 0 && tc.tableType == HoodieTableType.MERGE_ON_READ) {
        findings.add("Only base files are measured; on this Merge-on-Read table log files are not counted, so the actual pile-up can be "
            + "larger than reported.");
      }
    }

    StringBuilder sb = new StringBuilder("{");
    sb.append("\"name\": ").append(quote(SMALL_FILES_DETECTOR));
    sb.append(", \"status\": ").append(quote(tc.smallFileVerdict.name()));
    sb.append(", \"summary\": ").append(quote(summary));
    sb.append(", \"findings\": ").append(stringArrayToJson(findings));
    sb.append(", \"effectiveConfigs\": {");
    sb.append("\"effective.small.files.threshold.bytes\": ").append(cfg.smallFilesThresholdBytes);
    sb.append(", \"effective.small.files.min.files.per.partition\": ").append(cfg.smallFilesMinFilesPerPartition);
    sb.append(", \"effective.small.files.moderate.pct\": ").append(cfg.smallFilesModeratePct);
    sb.append(", \"effective.small.files.severe.pct\": ").append(cfg.smallFilesSeverePct);
    sb.append(", \"effective.small.files.min.table.commits\": ").append(cfg.smallFilesMinTableCommits);
    sb.append(", \"observed.ingest.commit.count\": ").append(tc.smallFileTableIngestCommitCount);
    sb.append(", \"observed.qualifying.partitions\": ").append(tc.smallFileQualifyingPartitions);
    sb.append(", \"observed.flagged.partitions\": ").append(tc.smallFileFlaggedPartitions);
    sb.append(", \"observed.flagged.pct\": ").append(String.format(Locale.ROOT, "%.4f", tc.smallFileFlaggedPct));
    sb.append("}");
    appendTopKSmallestJson(sb, tc.smallFileTopKSmallest);
    sb.append("}");
    return sb.toString();
  }

  private String hotPartitionsDetectorJson(TableCharacteristics tc) {
    DetectorStatus status = tc.hotPartitionStatus();
    int window = tc.hotWindowEffectiveCommits;
    String sharePct = String.format(Locale.ROOT, "%.0f%%", 100.0 * cfg.hotPartitionCommitShare);
    String summary;
    List<String> findings = new ArrayList<>();
    if (tc.hotPartitionDetectionFailed) {
      summary = "Skipped: hot-partition detection failed while reading the active timeline; see the log for the instant that could not be read.";
    } else if (window == 0) {
      summary = "Skipped: no completed ingest commits on the active timeline (compaction and clustering are excluded).";
    } else {
      summary = tc.hotPartitions.size() + " partition(s) were written by >= " + sharePct + " of the last " + window
          + " ingest commits (compaction and clustering excluded).";
      int shown = Math.min(10, tc.hotPartitions.size());
      for (int i = 0; i < shown; i++) {
        HotPartition h = tc.hotPartitions.get(i);
        findings.add("Partition " + quoteForSentence(h.partition) + " was written by " + h.commitCount + " of the last " + window
            + " ingest commits (" + String.format(Locale.ROOT, "%.0f%%", h.commitSharePct(window)) + "), "
            + formatBytes(h.bytesWritten) + " and " + h.recordsWritten + " records.");
      }
      if (tc.hotPartitions.size() > shown) {
        findings.add((tc.hotPartitions.size() - shown) + " more hot partitions are listed under partitions[].");
      }
      if (!tc.hotPartitions.isEmpty()) {
        findings.add("If these are not the partitions expected to absorb current writes (for example the latest date partition of a "
            + "time-partitioned table), revisit the partition key so writes spread across partitions; otherwise size the writer's "
            + "small-file handling and clustering around these partitions.");
      }
    }

    StringBuilder sb = new StringBuilder("{");
    sb.append("\"name\": ").append(quote(HOT_PARTITIONS_DETECTOR));
    sb.append(", \"status\": ").append(quote(status.name()));
    sb.append(", \"summary\": ").append(quote(summary));
    sb.append(", \"findings\": ").append(stringArrayToJson(findings));
    sb.append(", \"effectiveConfigs\": {");
    sb.append("\"effective.hot.window.commits\": ").append(cfg.hotWindowCommits);
    sb.append(", \"effective.hot.partition.commit.share\": ").append(cfg.hotPartitionCommitShare);
    sb.append(", \"effective.hot.partition.min.commit.count\": ").append(hotPartitionMinCommitCount(window));
    sb.append(", \"observed.hot.window.effective.commits\": ").append(window);
    sb.append(", \"observed.hot.partition.count\": ").append(tc.hotPartitions.size());
    sb.append("}");
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
    return sb.toString();
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

  private static String stringArrayToJson(List<String> values) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(quote(values.get(i)));
    }
    return sb.append("]").toString();
  }

  // ---- shared helpers -------------------------------------------------------

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

  private static String displayName(String partition) {
    return partition.isEmpty() ? "(root)" : partition;
  }

  private static String quoteForSentence(String partition) {
    return "'" + displayName(partition) + "'";
  }

  private static String formatBytes(long bytes) {
    return getFileSizeUnit(bytes);
  }

  /** Renders a JSON string literal, escaping quotes, backslashes and control characters. */
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
    SkewMetrics skew = SkewMetrics.of(rows);
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"cv\": ").append(String.format(Locale.ROOT, "%.4f", skew.cv));
    sb.append(", \"gini\": ").append(String.format(Locale.ROOT, "%.4f", skew.gini));
    sb.append(", \"largestPartitionShare\": ")
        .append(String.format(Locale.ROOT, "%.4f", (double) rows.get(0).totalBytes / tableTotalBytes));
    sb.append(", \"top").append(skew.topN).append("Share\": ")
        .append(String.format(Locale.ROOT, "%.4f", (double) skew.topNBytes / tableTotalBytes));
    sb.append(", \"outliers\": [");
    boolean first = true;
    for (PartitionRow r : skew.outliers(rows)) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append("{\"partition\": ").append(quote(r.partition))
          .append(", \"totalBytes\": ").append(r.totalBytes)
          .append(", \"files\": ").append(r.fileCount).append("}");
    }
    sb.append("]}");
    return sb.toString();
  }

  enum OutputFormat { TABLE, JSON }

  /** Verdict of a detector; the small-file detector uses {@link SmallFileVerdict} tiers instead. */
  enum DetectorStatus { CLEAN, FLAGGED, SKIPPED }

  /** Small-file verdict tiers based on prevalence (flagged / qualifying ratio). */
  enum SmallFileVerdict { CLEAN, MODERATE, SEVERE, SKIPPED }

  /** Aggregated per-partition stats, accumulated during the iteration and sorted for output. */
  static class PartitionRow {
    final String partition;
    final long fileCount;
    final long totalBytes;
    final List<Long> sizes;
    final Histogram sizeHist;
    final Long numRecords;

    PartitionRow(String partition, long fileCount, long totalBytes, List<Long> sizes, Histogram sizeHist, Long numRecords) {
      this.partition = partition;
      this.fileCount = fileCount;
      this.totalBytes = totalBytes;
      this.sizes = sizes;
      this.sizeHist = sizeHist;
      this.numRecords = numRecords;
    }

    long avgFileSize() {
      return fileCount == 0 ? 0 : totalBytes / fileCount;
    }
  }

  /** Skew statistics over per-partition total bytes; rows must be sorted by totalBytes desc. */
  static class SkewMetrics {
    final double cv;
    final double gini;
    final double outlierThreshold;
    final int topN;
    final long topNBytes;

    private SkewMetrics(double cv, double gini, double outlierThreshold, int topN, long topNBytes) {
      this.cv = cv;
      this.gini = gini;
      this.outlierThreshold = outlierThreshold;
      this.topN = topN;
      this.topNBytes = topNBytes;
    }

    static SkewMetrics of(List<PartitionRow> rows) {
      double[] sizes = rows.stream().mapToDouble(r -> (double) r.totalBytes).toArray();
      double mean = Arrays.stream(sizes).average().orElse(0);
      double var = Arrays.stream(sizes).map(v -> (v - mean) * (v - mean)).average().orElse(0);
      double stdev = Math.sqrt(var);
      int topN = Math.min(10, rows.size());
      long topNBytes = 0;
      for (int i = 0; i < topN; i++) {
        topNBytes += rows.get(i).totalBytes;
      }
      return new SkewMetrics(mean == 0 ? 0 : stdev / mean, giniCoefficient(sizes), mean + 2 * stdev, topN, topNBytes);
    }

    List<PartitionRow> outliers(List<PartitionRow> rows) {
      return rows.stream().filter(r -> r.totalBytes > outlierThreshold).collect(Collectors.toList());
    }
  }

  /**
   * Result of the {@code --analyze-table-characteristics} detectors. Each verdict block
   * is independent; the umbrella class only collates them for output.
   */
  static class TableCharacteristics {
    // Common context.
    HoodieTableType tableType;
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
    SmallFileVerdict smallFileVerdict;
    long smallFileTableIngestCommitCount;
    int smallFileQualifyingPartitions;
    int smallFileFlaggedPartitions;
    double smallFileFlaggedPct;
    // Top-K partitions (by lowest avg file size) among small-file "qualifying" partitions.
    // Populated when --print-top-k > 0. Empty otherwise.
    final List<SmallestPartition> smallFileTopKSmallest = new ArrayList<>();

    // Hot-partition verdict.
    int hotWindowEffectiveCommits;
    boolean hotPartitionDetectionFailed;
    final List<HotPartition> hotPartitions = new ArrayList<>();

    boolean sizeRuleEligible(Config cfg) {
      return tableAgeDays >= cfg.microPartitionMinAgeDays;
    }

    DetectorStatus hotPartitionStatus() {
      if (hotPartitionDetectionFailed || hotWindowEffectiveCommits == 0) {
        return DetectorStatus.SKIPPED;
      }
      return hotPartitions.isEmpty() ? DetectorStatus.CLEAN : DetectorStatus.FLAGGED;
    }

    /**
     * FLAGGED if any detector found something, CLEAN otherwise. Never SKIPPED: the
     * micro-partition count rule always runs, so at least one detector always judges.
     */
    DetectorStatus overallStatus() {
      boolean smallFilesFlagged = smallFileVerdict == SmallFileVerdict.MODERATE || smallFileVerdict == SmallFileVerdict.SEVERE;
      if (microPartitioned || smallFilesFlagged || hotPartitionStatus() == DetectorStatus.FLAGGED) {
        return DetectorStatus.FLAGGED;
      }
      return DetectorStatus.CLEAN;
    }
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

  /** Running per-partition counters while scanning the hot-partition window. */
  static class HotPartitionCounters {
    long commitCount;
    long bytesWritten;
    long recordsWritten;

    void add(List<HoodieWriteStat> writeStats) {
      commitCount++;
      for (HoodieWriteStat ws : writeStats) {
        bytesWritten += ws.getTotalWriteBytes();
        // numWrites is the file's total rows post-write (includes carried-over rows on
        // updates), which double-counts. Use the per-operation counters instead so a
        // commit that touches N rows reports N records, not file-row-count.
        recordsWritten += ws.getNumInserts() + ws.getNumUpdateWrites() + ws.getNumDeletes();
      }
    }
  }

  /** One hot-partition row, flagged because it appears in a large share of recent ingests. */
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

    double commitSharePct(int effectiveWindow) {
      return effectiveWindow == 0 ? 0 : 100.0 * commitCount / effectiveWindow;
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
      if (parts.length == 2) {
        dateString = parts[1].trim();
      }
    }

    try {
      return LocalDate.parse(dateString, DATE_FORMATTER);
    } catch (DateTimeParseException dtpe) {
      log.error("Partition name {} must conform to date format if --start-date, --end-date, or --num-days are specified. ", partition, dtpe);
    }
    return null;
  }

  private static String getFileSizeUnit(double size) {
    int counter = 0;
    while (size > 1024 && counter < FILE_SIZE_UNITS.length - 1) {
      size /= 1024;
      counter++;
    }

    return String.format("%.2f %s", size, FILE_SIZE_UNITS[counter]);
  }
}
