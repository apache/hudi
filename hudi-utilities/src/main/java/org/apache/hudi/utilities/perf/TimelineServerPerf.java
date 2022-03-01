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

package org.apache.hudi.utilities.perf;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TimelineServerPerf implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(TimelineServerPerf.class);
  private final Config cfg;
  private transient TimelineService timelineServer;
  private final boolean useExternalTimelineServer;
  private String hostAddr;

  public TimelineServerPerf(Config cfg) throws IOException {
    this.cfg = cfg;
    useExternalTimelineServer = (cfg.serverHost != null);
    TimelineService.Config timelineServiceConf = cfg.getTimelineServerConfig();
    this.timelineServer = new TimelineService(
        new HoodieLocalEngineContext(FSUtils.prepareHadoopConf(new Configuration())),
        new Configuration(), timelineServiceConf, FileSystem.get(new Configuration()),
        TimelineService.buildFileSystemViewManager(timelineServiceConf,
            new SerializableConfiguration(FSUtils.prepareHadoopConf(new Configuration()))));
  }

  private void setHostAddrFromSparkConf(SparkConf sparkConf) {
    String hostAddr = sparkConf.get("spark.driver.host", null);
    if (hostAddr != null) {
      LOG.info("Overriding hostIp to (" + hostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = hostAddr;
    } else {
      LOG.warn("Unable to find driver bind address from spark config");
    }
  }

  public void run() throws IOException {
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("hudi-view-perf-" + cfg.basePath, cfg.sparkMaster);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(engineContext, cfg.basePath, cfg.useFileListingFromMetadata, true);
    Collections.shuffle(allPartitionPaths);
    List<String> selected = allPartitionPaths.stream().filter(p -> !p.contains("error")).limit(cfg.maxPartitions)
        .collect(Collectors.toList());

    if (!useExternalTimelineServer) {
      this.timelineServer.startService();
      setHostAddrFromSparkConf(jsc.getConf());
    } else {
      this.hostAddr = cfg.serverHost;
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(timelineServer.getConf()).setBasePath(cfg.basePath).setLoadActiveTimelineOnLoad(true).build();
    SyncableFileSystemView fsView = new RemoteHoodieTableFileSystemView(this.hostAddr, cfg.serverPort, metaClient);

    String reportDir = cfg.reportDir;
    metaClient.getFs().mkdirs(new Path(reportDir));

    String dumpPrefix = UUID.randomUUID().toString();
    System.out.println("First Iteration to load all partitions");
    Dumper d = new Dumper(metaClient.getFs(), new Path(reportDir, String.format("1_%s.csv", dumpPrefix)));
    d.init();
    d.dump(runLookups(jsc, selected, fsView, 1, 0));
    d.close();
    System.out.println("\n\n\n First Iteration is done");

    Dumper d2 = new Dumper(metaClient.getFs(), new Path(reportDir, String.format("2_%s.csv", dumpPrefix)));
    d2.init();
    d2.dump(runLookups(jsc, selected, fsView, cfg.numIterations, cfg.numCoresPerExecutor));
    d2.close();

    System.out.println("\n\n\nDumping all File Slices");
    selected.forEach(p -> fsView.getAllFileSlices(p).forEach(s -> System.out.println("\tMyFileSlice=" + s)));

    // Waiting for curl queries
    if (!useExternalTimelineServer && cfg.waitForManualQueries) {
      System.out.println("Timeline Server Host Address=" + hostAddr + ", port=" + timelineServer.getServerPort());
      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // skip it
        }
      }
    }
  }

  public List<PerfStats> runLookups(JavaSparkContext jsc, List<String> partitionPaths, SyncableFileSystemView fsView,
      int numIterations, int concurrency) {
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    context.setJobStatus(this.getClass().getSimpleName(), "Lookup all performance stats");
    return context.flatMap(partitionPaths, p -> {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(100);
      final List<PerfStats> result = new ArrayList<>();
      final List<ScheduledFuture<PerfStats>> futures = new ArrayList<>();
      List<FileSlice> slices = fsView.getLatestFileSlices(p).collect(Collectors.toList());
      String fileId = slices.isEmpty() ? "dummyId"
          : slices.get(new Random(Double.doubleToLongBits(Math.random())).nextInt(slices.size())).getFileId();
      IntStream.range(0, concurrency).forEach(i -> futures.add(executor.schedule(() -> runOneRound(fsView, p, fileId,
          i, numIterations), 0, TimeUnit.NANOSECONDS)));
      futures.forEach(x -> {
        try {
          result.add(x.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
      System.out.println("SLICES are=");
      slices.forEach(s -> System.out.println("\t\tFileSlice=" + s));
      return result.stream();
    }, cfg.numExecutors);
  }

  private static PerfStats runOneRound(SyncableFileSystemView fsView, String partition, String fileId, int id,
      int numIterations) {
    Histogram latencyHistogram = new Histogram(new UniformReservoir(10000));
    for (int i = 0; i < numIterations; i++) {
      long beginTs = System.currentTimeMillis();
      Option<FileSlice> c = fsView.getLatestFileSlice(partition, fileId);
      long endTs = System.currentTimeMillis();
      System.out.println("Latest File Slice for part=" + partition + ", fileId=" + fileId + ", Slice=" + c + ", Time="
          + (endTs - beginTs));
      latencyHistogram.update(endTs - beginTs);
    }
    return new PerfStats(partition, id, latencyHistogram.getSnapshot());
  }

  private static class Dumper implements Serializable {

    private final Path dumpPath;
    private final FileSystem fileSystem;
    private FSDataOutputStream outputStream;

    public Dumper(FileSystem fs, Path dumpPath) {
      this.dumpPath = dumpPath;
      this.fileSystem = fs;
    }

    public void init() throws IOException {
      outputStream = fileSystem.create(dumpPath, true);
      addHeader();
    }

    private void addHeader() throws IOException {
      String header = "Partition,Thread,Min,Max,Mean,Median,75th,95th\n";
      outputStream.write(header.getBytes());
      outputStream.flush();
    }

    public void dump(List<PerfStats> stats) {
      stats.forEach(x -> {
        String row = String.format("%s,%d,%d,%d,%f,%f,%f,%f\n", x.partition, x.id, x.minTime, x.maxTime, x.meanTime,
            x.medianTime, x.p75, x.p95);
        System.out.println(row);
        try {
          outputStream.write(row.getBytes());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    public void close() throws IOException {
      outputStream.close();
    }
  }

  private static class PerfStats implements Serializable {

    private final String partition;
    private final int id;
    private final long minTime;
    private final long maxTime;
    private final double meanTime;
    private final double medianTime;
    private final double p95;
    private final double p75;

    public PerfStats(String partition, int id, Snapshot s) {
      this(partition, id, s.getMin(), s.getMax(), s.getMean(), s.getMedian(), s.get95thPercentile(),
          s.get75thPercentile());
    }

    public PerfStats(String partition, int id, long minTime, long maxTime, double meanTime, double medianTime,
        double p95, double p75) {
      this.partition = partition;
      this.id = id;
      this.minTime = minTime;
      this.maxTime = maxTime;
      this.meanTime = meanTime;
      this.medianTime = medianTime;
      this.p95 = p95;
      this.p75 = p75;
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-b"}, description = "Base Path", required = true)
    public String basePath = "";

    @Parameter(names = {"--report-dir", "-rd"}, description = "Dir where reports are added", required = true)
    public String reportDir = "";

    @Parameter(names = {"--max-partitions", "-m"}, description = "Mx partitions to be loaded")
    public Integer maxPartitions = 1000;

    @Parameter(names = {"--num-executors", "-e"}, description = "num executors")
    public Integer numExecutors = 10;

    @Parameter(names = {"--num-cores", "-c"}, description = "num cores")
    public Integer numCoresPerExecutor = 10;

    @Parameter(names = {"--num-iterations", "-i"}, description = "Number of iterations for each partitions")
    public Integer numIterations = 10;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
    public Integer serverPort = 26754;

    @Parameter(names = {"--server-host", "-sh"},
        description = " Server Host (Set it for externally managed timeline service")
    public String serverHost = null;

    @Parameter(names = {"--view-storage", "-st"}, description = "View Storage Type. Default - SPILLABLE_DISK")
    public FileSystemViewStorageType viewStorageType = FileSystemViewStorageType.SPILLABLE_DISK;

    @Parameter(names = {"--max-view-mem-per-table", "-mv"},
        description = "Maximum view memory per table in MB to be used for storing file-groups."
            + " Overflow file-groups will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Integer maxViewMemPerTableInMB = 2048;

    @Parameter(names = {"--mem-overhead-fraction-pending-compaction", "-cf"},
        description = "Memory Fraction of --max-view-mem-per-table to be allocated for managing pending compaction"
            + " storage. Overflow entries will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Double memFractionForCompactionPerTable = 0.001;

    @Parameter(names = {"--base-store-path", "-sp"},
        description = "Directory where spilled view entries will be stored. Used for SPILLABLE_DISK storage type")
    public String baseStorePathForFileGroups = FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue();

    @Parameter(names = {"--rocksdb-path", "-rp"}, description = "Root directory for RocksDB")
    public String rocksDBPath = FileSystemViewStorageConfig.ROCKSDB_BASE_PATH.defaultValue();

    @Parameter(names = {"--wait-for-manual-queries", "-ww"})
    public Boolean waitForManualQueries = false;

    @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
    public Boolean useFileListingFromMetadata = HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;

    @Parameter(names = {"--help", "-h"})
    public Boolean help = false;

    public TimelineService.Config getTimelineServerConfig() {
      TimelineService.Config c = new TimelineService.Config();
      c.viewStorageType = viewStorageType;
      c.baseStorePathForFileGroups = baseStorePathForFileGroups;
      c.maxViewMemPerTableInMB = maxViewMemPerTableInMB;
      c.memFractionForCompactionPerTable = memFractionForCompactionPerTable;
      c.rocksDBPath = rocksDBPath;
      c.serverPort = serverPort;
      return c;
    }
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    TimelineServerPerf perf = new TimelineServerPerf(cfg);
    perf.run();
  }
}
