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

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Base class for hbase index to determine the mapping from uuid.
 */
public abstract class BaseHoodieHBaseIndex<T extends HoodieRecordPayload, I, K, O, P> extends HoodieIndex<T, I, K, O, P> {

  public static final String DEFAULT_SPARK_EXECUTOR_INSTANCES_CONFIG_NAME = "spark.executor.instances";
  public static final String DEFAULT_SPARK_DYNAMIC_ALLOCATION_ENABLED_CONFIG_NAME = "spark.dynamicAllocation.enabled";
  public static final String DEFAULT_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG_NAME =
      "spark.dynamicAllocation.maxExecutors";

  public static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
  public static final byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
  public static final byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
  public static final byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");
  public static final int SLEEP_TIME_MILLISECONDS = 100;

  private static final Logger LOG = LogManager.getLogger(BaseHoodieHBaseIndex.class);
  public static Connection hbaseConnection = null;
  private HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = null;
  public float qpsFraction;
  public int maxQpsPerRegionServer;
  /**
   * multiPutBatchSize will be computed and re-set in updateLocation if
   * {@link HoodieHBaseIndexConfig#HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP} is set to true.
   */
  public Integer multiPutBatchSize;
  public Integer numRegionServersForTable;
  public final String tableName;
  public HBasePutBatchSizeCalculator putBatchSizeCalculator;

  protected BaseHoodieHBaseIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getHbaseTableName();
    addShutDownHook();
    init(config);
  }

  private void init(HoodieWriteConfig config) {
    this.multiPutBatchSize = config.getHbaseIndexGetBatchSize();
    this.qpsFraction = config.getHbaseIndexQPSFraction();
    this.maxQpsPerRegionServer = config.getHbaseIndexMaxQPSPerRegionServer();
    this.putBatchSizeCalculator = new HBasePutBatchSizeCalculator();
    this.hBaseIndexQPSResourceAllocator = createQPSResourceAllocator(this.config);
  }

  public HBaseIndexQPSResourceAllocator createQPSResourceAllocator(HoodieWriteConfig config) {
    try {
      LOG.info("createQPSResourceAllocator :" + config.getHBaseQPSResourceAllocatorClass());
      return (HBaseIndexQPSResourceAllocator) ReflectionUtils
          .loadClass(config.getHBaseQPSResourceAllocatorClass(), config);
    } catch (Exception e) {
      LOG.warn("error while instantiating HBaseIndexQPSResourceAllocator", e);
    }
    return new DefaultHBaseQPSResourceAllocator(config);
  }

  @Override
  public P fetchRecordLocation(K hoodieKeys,
                               HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) {
    throw new UnsupportedOperationException("HBase index does not implement check exist");
  }

  public Connection getHBaseConnection() {
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
   * Since we are sharing the HBaseConnection across tasks in a JVM, make sure the HBaseConnection is closed when JVM
   * exits.
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        hbaseConnection.close();
      } catch (Exception e) {
        // fail silently for any sort of exception
      }
    }));
  }

  /**
   * Ensure that any resources used for indexing are released here.
   */
  @Override
  public void close() {
    this.hBaseIndexQPSResourceAllocator.releaseQPSResources();
  }

  public Get generateStatement(String key) throws IOException {
    return new Get(Bytes.toBytes(key)).setMaxVersions(1).addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
        .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN).addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN);
  }

  public boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty()
        && (commitTimeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs))
        || HoodieTimeline.compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(), HoodieTimeline.GREATER_THAN, commitTs
    ));
  }

  public Result[] doGet(HTable hTable, List<Get> keys) throws IOException {
    sleepForTime(SLEEP_TIME_MILLISECONDS);
    return hTable.get(keys);
  }

  /**
   * Helper method to facilitate performing mutations (including puts and deletes) in Hbase.
   */
  public void doMutations(BufferedMutator mutator, List<Mutation> mutations) throws IOException {
    if (mutations.isEmpty()) {
      return;
    }
    mutator.mutate(mutations);
    mutator.flush();
    mutations.clear();
    sleepForTime(SLEEP_TIME_MILLISECONDS);
  }

  private static void sleepForTime(int sleepTimeMs) {
    try {
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException e) {
      LOG.error("Sleep interrupted during throttling", e);
      throw new RuntimeException(e);
    }
  }

  public static class HBasePutBatchSizeCalculator implements Serializable {

    private static final int MILLI_SECONDS_IN_A_SECOND = 1000;
    private static final org.apache.log4j.Logger LOG = LogManager.getLogger(HBasePutBatchSizeCalculator.class);

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
      int maxReqPerSec = (int) (qpsFraction * numRegionServersForTable * maxQpsPerRegionServer);
      int maxParallelPuts = Math.max(1, Math.min(numTasksDuringPut, maxExecutors));
      int maxReqsSentPerTaskPerSec = MILLI_SECONDS_IN_A_SECOND / sleepTimeMs;
      int multiPutBatchSize = Math.max(1, maxReqPerSec / (maxParallelPuts * maxReqsSentPerTaskPerSec));
      LOG.info("HbaseIndexThrottling: qpsFraction :" + qpsFraction);
      LOG.info("HbaseIndexThrottling: numRSAlive :" + numRegionServersForTable);
      LOG.info("HbaseIndexThrottling: maxReqPerSec :" + maxReqPerSec);
      LOG.info("HbaseIndexThrottling: numTasks :" + numTasksDuringPut);
      LOG.info("HbaseIndexThrottling: maxExecutors :" + maxExecutors);
      LOG.info("HbaseIndexThrottling: maxParallelPuts :" + maxParallelPuts);
      LOG.info("HbaseIndexThrottling: maxReqsSentPerTaskPerSec :" + maxReqsSentPerTaskPerSec);
      LOG.info("HbaseIndexThrottling: numRegionServersForTable :" + numRegionServersForTable);
      LOG.info("HbaseIndexThrottling: multiPutBatchSize :" + multiPutBatchSize);
      return multiPutBatchSize;
    }
  }

  public Integer getNumRegionServersAliveForTable() {
    // This is being called in the driver, so there is only one connection
    // from the driver, so ok to use a local connection variable.
    if (numRegionServersForTable == null) {
      try (Connection conn = getHBaseConnection()) {
        RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
        numRegionServersForTable = Math
            .toIntExact(regionLocator.getAllRegionLocations().stream().map(HRegionLocation::getServerName).distinct().count());
        return numRegionServersForTable;
      } catch (IOException e) {
        LOG.error(String.valueOf(e));
        throw new RuntimeException(e);
      }
    }
    return numRegionServersForTable;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
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

  public void setHbaseConnection(Connection hbaseConnection) {
    BaseHoodieHBaseIndex.hbaseConnection = hbaseConnection;
  }

}
