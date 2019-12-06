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

package org.apache.hudi.index.hbase;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;

/**
 * Hoodie Index implementation backed by HBase.
 */
public class HBaseIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  public static final String DEFAULT_SPARK_EXECUTOR_INSTANCES_CONFIG_NAME = "spark.executor.instances";
  public static final String DEFAULT_SPARK_DYNAMIC_ALLOCATION_ENABLED_CONFIG_NAME = "spark.dynamicAllocation.enabled";
  public static final String DEFAULT_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG_NAME =
      "spark.dynamicAllocation.maxExecutors";

  private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
  private static final byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
  private static final byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
  private static final byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");
  private static final int SLEEP_TIME_MILLISECONDS = 100;

  private static Logger logger = LogManager.getLogger(HBaseIndex.class);
  private static Connection hbaseConnection = null;
  private HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = null;
  private float qpsFraction;
  private int maxQpsPerRegionServer;
  /**
   * multiPutBatchSize will be computed and re-set in updateLocation if
   * {@link HoodieIndexConfig.HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP} is set to true.
   */
  private Integer multiPutBatchSize;
  private Integer numRegionServersForTable;
  private final String tableName;
  private HbasePutBatchSizeCalculator putBatchSizeCalculator;

  public HBaseIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getHbaseTableName();
    addShutDownHook();
    init(config);
  }

  private void init(HoodieWriteConfig config) {
    this.multiPutBatchSize = config.getHbaseIndexGetBatchSize();
    this.qpsFraction = config.getHbaseIndexQPSFraction();
    this.maxQpsPerRegionServer = config.getHbaseIndexMaxQPSPerRegionServer();
    this.putBatchSizeCalculator = new HbasePutBatchSizeCalculator();
    this.hBaseIndexQPSResourceAllocator = createQPSResourceAllocator(this.config);
  }

  @VisibleForTesting
  public HBaseIndexQPSResourceAllocator createQPSResourceAllocator(HoodieWriteConfig config) {
    try {
      logger.info("createQPSResourceAllocator :" + config.getHBaseQPSResourceAllocatorClass());
      final HBaseIndexQPSResourceAllocator resourceAllocator = (HBaseIndexQPSResourceAllocator) ReflectionUtils
          .loadClass(config.getHBaseQPSResourceAllocatorClass(), config);
      return resourceAllocator;
    } catch (Exception e) {
      logger.warn("error while instantiating HBaseIndexQPSResourceAllocator", e);
    }
    return new DefaultHBaseQPSResourceAllocator(config);
  }

  @Override
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
    throw new UnsupportedOperationException("HBase index does not implement check exist");
  }

  private Connection getHBaseConnection() {
    Configuration hbaseConfig = HBaseConfiguration.create();
    String quorum = config.getHbaseZkQuorum();
    hbaseConfig.set("hbase.zookeeper.quorum", quorum);
    String zkZnodeParent = config.getHBaseZkZnodeParent();
    if (zkZnodeParent != null) {
      hbaseConfig.set("zookeeper.znode.parent", zkZnodeParent);
    }
    String port = String.valueOf(config.getHbaseZkPort());
    hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
    try {
      return ConnectionFactory.createConnection(hbaseConfig);
    } catch (IOException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.HBASE,
          quorum + ":" + port);
    }
  }

  /**
   * Since we are sharing the HbaseConnection across tasks in a JVM, make sure the HbaseConnectio is closed when JVM
   * exits.
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          hbaseConnection.close();
        } catch (Exception e) {
          // fail silently for any sort of exception
        }
      }
    });
  }

  /**
   * Ensure that any resources used for indexing are released here.
   */
  public void close() {
    this.hBaseIndexQPSResourceAllocator.releaseQPSResources();
  }

  private Get generateStatement(String key) throws IOException {
    return new Get(Bytes.toBytes(key)).setMaxVersions(1).addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
        .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN).addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN);
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty()
        && (commitTimeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs))
            || HoodieTimeline.compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(), commitTs,
                HoodieTimeline.GREATER));
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
      HoodieTableMetaClient metaClient) {

    return (Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>>) (partitionNum,
        hoodieRecordIterator) -> {

      Integer multiGetBatchSize = config.getHbaseIndexGetBatchSize();

      // Grab the global HBase connection
      synchronized (HBaseIndex.class) {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
          hbaseConnection = getHBaseConnection();
        }
      }
      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      HTable hTable = null;
      try {
        hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
        List<Get> statements = new ArrayList<>();
        List<HoodieRecord> currentBatchOfRecords = new LinkedList<>();
        // Do the tagging.
        while (hoodieRecordIterator.hasNext()) {
          HoodieRecord rec = hoodieRecordIterator.next();
          statements.add(generateStatement(rec.getRecordKey()));
          currentBatchOfRecords.add(rec);
          // iterator till we reach batch size
          if (statements.size() >= multiGetBatchSize || !hoodieRecordIterator.hasNext()) {
            // get results for batch from Hbase
            Result[] results = doGet(hTable, statements);
            // clear statements to be GC'd
            statements.clear();
            for (Result result : results) {
              // first, attempt to grab location from HBase
              HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
              if (result.getRow() != null) {
                String keyFromResult = Bytes.toString(result.getRow());
                String commitTs = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                String fileId = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
                String partitionPath = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                if (checkIfValidCommit(metaClient, commitTs)) {
                  currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                      currentRecord.getData());
                  currentRecord.unseal();
                  currentRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                  currentRecord.seal();
                  taggedRecords.add(currentRecord);
                  // the key from Result and the key being processed should be same
                  assert (currentRecord.getRecordKey().contentEquals(keyFromResult));
                } else { // if commit is invalid, treat this as a new taggedRecord
                  taggedRecords.add(currentRecord);
                }
              } else {
                taggedRecords.add(currentRecord);
              }
            }
          }
        }
      } catch (IOException e) {
        throw new HoodieIndexException("Failed to Tag indexed locations because of exception with HBase Client", e);
      } finally {
        if (hTable != null) {
          try {
            hTable.close();
          } catch (IOException e) {
            // Ignore
          }
        }

      }
      return taggedRecords.iterator();
    };
  }

  private Result[] doGet(HTable hTable, List<Get> keys) throws IOException {
    sleepForTime(SLEEP_TIME_MILLISECONDS);
    return hTable.get(keys);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    return recordRDD.mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true);
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {

    return (Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>>) (partition, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();
      // Grab the global HBase connection
      synchronized (HBaseIndex.class) {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
          hbaseConnection = getHBaseConnection();
        }
      }
      HTable hTable = null;
      try {
        hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
        while (statusIterator.hasNext()) {
          WriteStatus writeStatus = statusIterator.next();
          List<Put> puts = new ArrayList<>();
          List<Delete> deletes = new ArrayList<>();
          try {
            for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
              if (!writeStatus.isErrored(rec.getKey())) {
                Option<HoodieRecordLocation> loc = rec.getNewLocation();
                if (loc.isPresent()) {
                  if (rec.getCurrentLocation() != null) {
                    // This is an update, no need to update index
                    continue;
                  }
                  Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN, Bytes.toBytes(loc.get().getInstantTime()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN, Bytes.toBytes(loc.get().getFileId()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN, Bytes.toBytes(rec.getPartitionPath()));
                  puts.add(put);
                } else {
                  // Delete existing index for a deleted record
                  Delete delete = new Delete(Bytes.toBytes(rec.getRecordKey()));
                  deletes.add(delete);
                }
              }
              if (puts.size() + deletes.size() < multiPutBatchSize) {
                continue;
              }
              doPutsAndDeletes(hTable, puts, deletes);
            }
            // process remaining puts and deletes, if any
            doPutsAndDeletes(hTable, puts, deletes);
          } catch (Exception e) {
            Exception we = new Exception("Error updating index for " + writeStatus, e);
            logger.error(we);
            writeStatus.setGlobalError(we);
          }
          writeStatusList.add(writeStatus);
        }
      } catch (IOException e) {
        throw new HoodieIndexException("Failed to Update Index locations because of exception with HBase Client", e);
      } finally {
        if (hTable != null) {
          try {
            hTable.close();
          } catch (IOException e) {
            // Ignore
          }
        }
      }
      return writeStatusList.iterator();
    };
  }

  /**
   * Helper method to facilitate performing puts and deletes in Hbase.
   */
  private void doPutsAndDeletes(HTable hTable, List<Put> puts, List<Delete> deletes) throws IOException {
    if (puts.size() > 0) {
      hTable.put(puts);
    }
    if (deletes.size() > 0) {
      hTable.delete(deletes);
    }
    hTable.flushCommits();
    puts.clear();
    deletes.clear();
    sleepForTime(SLEEP_TIME_MILLISECONDS);
  }

  private static void sleepForTime(int sleepTimeMs) {
    try {
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException e) {
      logger.error("Sleep interrupted during throttling", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    final HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = createQPSResourceAllocator(this.config);
    setPutBatchSize(writeStatusRDD, hBaseIndexQPSResourceAllocator, jsc);
    logger.info("multiPutBatchSize: before hbase puts" + multiPutBatchSize);
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(config.getWriteStatusStorageLevel());
    return writeStatusJavaRDD;
  }

  private void setPutBatchSize(JavaRDD<WriteStatus> writeStatusRDD,
      HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator, final JavaSparkContext jsc) {
    if (config.getHbaseIndexPutBatchSizeAutoCompute()) {
      SparkConf conf = jsc.getConf();
      int maxExecutors = conf.getInt(DEFAULT_SPARK_EXECUTOR_INSTANCES_CONFIG_NAME, 1);
      if (conf.getBoolean(DEFAULT_SPARK_DYNAMIC_ALLOCATION_ENABLED_CONFIG_NAME, false)) {
        maxExecutors =
            Math.max(maxExecutors, conf.getInt(DEFAULT_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG_NAME, 1));
      }

      /*
       * Each writeStatus represents status information from a write done in one of the IOHandles. If a writeStatus has
       * any insert, it implies that the corresponding task contacts HBase for doing puts, since we only do puts for
       * inserts from HBaseIndex.
       */
      final Tuple2<Long, Integer> numPutsParallelismTuple = getHBasePutAccessParallelism(writeStatusRDD);
      final long numPuts = numPutsParallelismTuple._1;
      final int hbasePutsParallelism = numPutsParallelismTuple._2;
      this.numRegionServersForTable = getNumRegionServersAliveForTable();
      final float desiredQPSFraction =
          hBaseIndexQPSResourceAllocator.calculateQPSFractionForPutsTime(numPuts, this.numRegionServersForTable);
      logger.info("Desired QPSFraction :" + desiredQPSFraction);
      logger.info("Number HBase puts :" + numPuts);
      logger.info("Hbase Puts Parallelism :" + hbasePutsParallelism);
      final float availableQpsFraction =
          hBaseIndexQPSResourceAllocator.acquireQPSResources(desiredQPSFraction, numPuts);
      logger.info("Allocated QPS Fraction :" + availableQpsFraction);
      multiPutBatchSize = putBatchSizeCalculator.getBatchSize(numRegionServersForTable, maxQpsPerRegionServer,
          hbasePutsParallelism, maxExecutors, SLEEP_TIME_MILLISECONDS, availableQpsFraction);
      logger.info("multiPutBatchSize :" + multiPutBatchSize);
    }
  }

  @VisibleForTesting
  public Tuple2<Long, Integer> getHBasePutAccessParallelism(final JavaRDD<WriteStatus> writeStatusRDD) {
    final JavaPairRDD<Long, Integer> insertOnlyWriteStatusRDD = writeStatusRDD
        .filter(w -> w.getStat().getNumInserts() > 0).mapToPair(w -> new Tuple2<>(w.getStat().getNumInserts(), 1));
    return insertOnlyWriteStatusRDD.fold(new Tuple2<>(0L, 0), (w, c) -> new Tuple2<>(w._1 + c._1, w._2 + c._2));
  }

  public static class HbasePutBatchSizeCalculator implements Serializable {

    private static final int MILLI_SECONDS_IN_A_SECOND = 1000;
    private static Logger logger = LogManager.getLogger(HbasePutBatchSizeCalculator.class);

    /**
     * Calculate putBatch size so that sum of requests across multiple jobs in a second does not exceed
     * maxQpsPerRegionServer for each Region Server. Multiplying qpsFraction to reduce the aggregate load on common RS
     * across topics. Assumption here is that all tables have regions across all RS, which is not necessarily true for
     * smaller tables. So, they end up getting a smaller share of QPS than they deserve, but it might be ok.
     * <p>
     * Example: int putBatchSize = batchSizeCalculator.getBatchSize(10, 16667, 1200, 200, 100, 0.1f)
     * </p>
     * <p>
     * Expected batchSize is 8 because in that case, total request sent to a Region Server in one second is:
     *
     * 8 (batchSize) * 200 (parallelism) * 10 (maxReqsInOneSecond) * 10 (numRegionServers) * 0.1 (qpsFraction)) =>
     * 16000. We assume requests get distributed to Region Servers uniformly, so each RS gets 1600 requests which
     * happens to be 10% of 16667 (maxQPSPerRegionServer), as expected.
     * </p>
     * <p>
     * Assumptions made here
     * <li>In a batch, writes get evenly distributed to each RS for that table. Since we do writes only in the case of
     * inserts and not updates, for this assumption to fail, inserts would have to be skewed towards few RS, likelihood
     * of which is less if Hbase table is pre-split and rowKeys are UUIDs (random strings). If this assumption fails,
     * then it is possible for some RS to receive more than maxQpsPerRegionServer QPS, but for simplicity, we are going
     * ahead with this model, since this is meant to be a lightweight distributed throttling mechanism without
     * maintaining a global context. So if this assumption breaks, we are hoping the HBase Master relocates hot-spot
     * regions to new Region Servers.
     *
     * </li>
     * <li>For Region Server stability, throttling at a second level granularity is fine. Although, within a second, the
     * sum of queries might be within maxQpsPerRegionServer, there could be peaks at some sub second intervals. So, the
     * assumption is that these peaks are tolerated by the Region Server (which at max can be maxQpsPerRegionServer).
     * </li>
     * </p>
     */
    public int getBatchSize(int numRegionServersForTable, int maxQpsPerRegionServer, int numTasksDuringPut,
        int maxExecutors, int sleepTimeMs, float qpsFraction) {
      int numRSAlive = numRegionServersForTable;
      int maxReqPerSec = (int) (qpsFraction * numRSAlive * maxQpsPerRegionServer);
      int numTasks = numTasksDuringPut;
      int maxParallelPuts = Math.max(1, Math.min(numTasks, maxExecutors));
      int maxReqsSentPerTaskPerSec = MILLI_SECONDS_IN_A_SECOND / sleepTimeMs;
      int multiPutBatchSize = Math.max(1, maxReqPerSec / (maxParallelPuts * maxReqsSentPerTaskPerSec));
      logger.info("HbaseIndexThrottling: qpsFraction :" + qpsFraction);
      logger.info("HbaseIndexThrottling: numRSAlive :" + numRSAlive);
      logger.info("HbaseIndexThrottling: maxReqPerSec :" + maxReqPerSec);
      logger.info("HbaseIndexThrottling: numTasks :" + numTasks);
      logger.info("HbaseIndexThrottling: maxExecutors :" + maxExecutors);
      logger.info("HbaseIndexThrottling: maxParallelPuts :" + maxParallelPuts);
      logger.info("HbaseIndexThrottling: maxReqsSentPerTaskPerSec :" + maxReqsSentPerTaskPerSec);
      logger.info("HbaseIndexThrottling: numRegionServersForTable :" + numRegionServersForTable);
      logger.info("HbaseIndexThrottling: multiPutBatchSize :" + multiPutBatchSize);
      return multiPutBatchSize;
    }
  }

  private Integer getNumRegionServersAliveForTable() {
    // This is being called in the driver, so there is only one connection
    // from the driver, so ok to use a local connection variable.
    if (numRegionServersForTable == null) {
      try (Connection conn = getHBaseConnection()) {
        RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
        numRegionServersForTable = Math
            .toIntExact(regionLocator.getAllRegionLocations().stream().map(e -> e.getServerName()).distinct().count());
        return numRegionServersForTable;
      } catch (IOException e) {
        logger.error(e);
        throw new RuntimeException(e);
      }
    }
    return numRegionServersForTable;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Rollback in HbaseIndex is managed via method {@link #checkIfValidCommit()}
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  @VisibleForTesting
  public void setHbaseConnection(Connection hbaseConnection) {
    HBaseIndex.hbaseConnection = hbaseConnection;
  }
}
