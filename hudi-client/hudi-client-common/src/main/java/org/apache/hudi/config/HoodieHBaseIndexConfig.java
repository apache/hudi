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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@ConfigClassProperty(name = "HBase Index Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control indexing behavior "
        + "(when HBase based indexing is enabled), which tags incoming "
        + "records as either inserts or updates to older records.")
public class HoodieHBaseIndexConfig extends HoodieConfig {

  public static final ConfigProperty<String> HBASE_ZKQUORUM = ConfigProperty
      .key("hoodie.index.hbase.zkquorum")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase ZK Quorum url to connect to");

  public static final ConfigProperty<String> HBASE_ZKPORT = ConfigProperty
      .key("hoodie.index.hbase.zkport")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase ZK Quorum port to connect to");

  public static final ConfigProperty<String> HBASE_TABLENAME = ConfigProperty
      .key("hoodie.index.hbase.table")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase Table name to use as the index. "
          + "Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table");

  public static final ConfigProperty<Integer> HBASE_GET_BATCH_SIZE = ConfigProperty
      .key("hoodie.index.hbase.get.batch.size")
      .defaultValue(100)
      .withDocumentation("Controls the batch size for performing gets against HBase. "
          + "Batching improves throughput, by saving round trips.");

  public static final ConfigProperty<String> HBASE_ZK_ZNODEPARENT_CFG = ConfigProperty
      .key("hoodie.index.hbase.zknode.path")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. This is the root znode that will contain "
          + "all the znodes created/used by HBase");

  public static final ConfigProperty<Integer> HBASE_PUT_BATCH_SIZE = ConfigProperty
      .key("hoodie.index.hbase.put.batch.size")
      .defaultValue(100)
      .withDocumentation("Controls the batch size for performing puts against HBase. "
          + "Batching improves throughput, by saving round trips.");

  public static final ConfigProperty<String> HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG = ConfigProperty
      .key("hoodie.index.hbase.qps.allocator.class")
      .defaultValue(DefaultHBaseQPSResourceAllocator.class.getName())
      .withDocumentation("Property to set which implementation of HBase QPS resource allocator to be used, which"
          + "controls the batching rate dynamically.");

  public static final ConfigProperty<String> HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE = ConfigProperty
      .key("hoodie.index.hbase.put.batch.size.autocompute")
      .defaultValue("false")
      .withDocumentation("Property to set to enable auto computation of put batch size");

  public static final ConfigProperty<Float> HBASE_QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.qps.fraction")
      .defaultValue(0.5f)
      .withDocumentation("Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3"
          + " jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then"
          + " this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively."
          + " Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.");

  public static final ConfigProperty<Integer> HBASE_MAX_QPS_PER_REGION_SERVER = ConfigProperty
      .key("hoodie.index.hbase.max.qps.per.region.server")
      .defaultValue(1000)
      .withDocumentation("Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to\n"
          + " limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this\n"
          + " value based on global indexing throughput needs and most importantly, how much the HBase installation in use is\n"
          + " able to tolerate without Region Servers going down.");

  public static final ConfigProperty<Boolean> HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG = ConfigProperty
      .key("hoodie.index.hbase.dynamic_qps")
      .defaultValue(false)
      .withDocumentation("Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on write volume.");

  public static final ConfigProperty<String> HBASE_MIN_QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.min.qps.fraction")
      .noDefaultValue()
      .withDocumentation("Minimum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads");

  public static final ConfigProperty<String> HBASE_MAX_QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.max.qps.fraction")
      .noDefaultValue()
      .withDocumentation("Maximum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads");

  public static final ConfigProperty<Integer> HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS = ConfigProperty
      .key("hoodie.index.hbase.desired_puts_time_in_secs")
      .defaultValue(600)
      .withDocumentation("");

  public static final ConfigProperty<String> HBASE_SLEEP_MS_PUT_BATCH = ConfigProperty
      .key("hoodie.index.hbase.sleep.ms.for.put.batch")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> HBASE_SLEEP_MS_GET_BATCH = ConfigProperty
      .key("hoodie.index.hbase.sleep.ms.for.get.batch")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<Integer> HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG = ConfigProperty
      .key("hoodie.index.hbase.zk.session_timeout_ms")
      .defaultValue(60 * 1000)
      .withDocumentation("Session timeout value to use for Zookeeper failure detection, for the HBase client."
          + "Lower this value, if you want to fail faster.");

  public static final ConfigProperty<Integer> HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG = ConfigProperty
      .key("hoodie.index.hbase.zk.connection_timeout_ms")
      .defaultValue(15 * 1000)
      .withDocumentation("Timeout to use for establishing connection with zookeeper, from HBase client.");

  public static final ConfigProperty<String> HBASE_ZK_PATH_QPS_ROOT_CFG = ConfigProperty
      .key("hoodie.index.hbase.zkpath.qps_root")
      .defaultValue("/QPS_ROOT")
      .withDocumentation("chroot in zookeeper, to use for all qps allocation co-ordination.");

  public static final ConfigProperty<Boolean> HBASE_INDEX_UPDATE_PARTITION_PATH_CFG = ConfigProperty
      .key("hoodie.hbase.index.update.partition.path")
      .defaultValue(false)
      .withDocumentation("Only applies if index type is HBASE. "
          + "When an already existing record is upserted to a new partition compared to whats in storage, "
          + "this config when set, will delete old record in old paritition "
          + "and will insert it as new record in new partition.");

  public static final ConfigProperty<Boolean> HBASE_INDEX_ROLLBACK_SYNC_CFG = ConfigProperty
      .key("hoodie.index.hbase.rollback.sync")
      .defaultValue(false)
      .withDocumentation("When set to true, the rollback method will delete the last failed task index. "
          + "The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback");

  /** @deprecated Use {@link #HBASE_ZKQUORUM} and its methods instead */
  @Deprecated
  public static final String HBASE_ZKQUORUM_PROP = HBASE_ZKQUORUM.key();
  /** @deprecated Use {@link #HBASE_ZKPORT} and its methods instead */
  @Deprecated
  public static final String HBASE_ZKPORT_PROP = HBASE_ZKPORT.key();
  /** @deprecated Use {@link #HBASE_TABLENAME} and its methods instead */
  @Deprecated
  public static final String HBASE_TABLENAME_PROP = HBASE_TABLENAME.key();
  /** @deprecated Use {@link #HBASE_GET_BATCH_SIZE} and its methods instead */
  @Deprecated
  public static final String HBASE_GET_BATCH_SIZE_PROP = HBASE_GET_BATCH_SIZE.key();
  /** @deprecated Use {@link #HBASE_ZK_ZNODEPARENT_CFG} and its methods instead */
  @Deprecated
  public static final String HBASE_ZK_ZNODEPARENT = HBASE_ZK_ZNODEPARENT_CFG.key();
  /** @deprecated Use {@link #HBASE_PUT_BATCH_SIZE} and its methods instead */
  @Deprecated
  public static final String HBASE_PUT_BATCH_SIZE_PROP = HBASE_PUT_BATCH_SIZE.key();
  /** @deprecated Use {@link #HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG} and its methods instead */
  @Deprecated
  public static final String HBASE_INDEX_QPS_ALLOCATOR_CLASS = HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG.key();
  /** @deprecated Use {@link #HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG} and its methods instead */
  @Deprecated
  public static final String DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS = HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG.defaultValue();
  /** @deprecated Use {@link #HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE} and its methods instead */
  @Deprecated
  public static final String HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP = HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE.key();
  /** @deprecated Use {@link #HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE} and its methods instead */
  @Deprecated
  public static final String DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE = HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE.defaultValue();
  /** @deprecated Use {@link #HBASE_MAX_QPS_FRACTION} and its methods instead */
  @Deprecated
  public static final String HBASE_QPS_FRACTION_PROP = HBASE_QPS_FRACTION.key();
  /** @deprecated Use {@link #HBASE_MAX_QPS_PER_REGION_SERVER} and its methods instead */
  @Deprecated
  public static final String HBASE_MAX_QPS_PER_REGION_SERVER_PROP = HBASE_MAX_QPS_PER_REGION_SERVER.key();
  @Deprecated
  public static final int DEFAULT_HBASE_BATCH_SIZE = 100;
  /** @deprecated Use {@link #HBASE_MAX_QPS_PER_REGION_SERVER} and its methods instead */
  @Deprecated
  public static final int DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER = HBASE_MAX_QPS_PER_REGION_SERVER.defaultValue();
  /** @deprecated Use {@link #HBASE_QPS_FRACTION} and its methods instead */
  @Deprecated
  public static final float DEFAULT_HBASE_QPS_FRACTION = HBASE_QPS_FRACTION.defaultValue();
  /** @deprecated Use {@link #HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG} and its methods instead */
  @Deprecated
  public static final String HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG.key();
  /** @deprecated Use {@link #HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG} and its methods instead */
  @Deprecated
  public static final boolean DEFAULT_HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG.defaultValue();
  /** @deprecated Use {@link #HBASE_MIN_QPS_FRACTION} and its methods instead */
  @Deprecated
  public static final String HBASE_MIN_QPS_FRACTION_PROP = HBASE_MIN_QPS_FRACTION.key();
  /** @deprecated Use {@link #HBASE_MAX_QPS_FRACTION} and its methods instead */
  @Deprecated
  public static final String HBASE_MAX_QPS_FRACTION_PROP = HBASE_MAX_QPS_FRACTION.key();
  /** @deprecated Use {@link #HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS} and its methods instead */
  @Deprecated
  public static final String HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS.key();
  /** @deprecated Use {@link #HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS} and its methods instead */
  @Deprecated
  public static final int DEFAULT_HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS.defaultValue();
  /** @deprecated Use {@link #HBASE_SLEEP_MS_PUT_BATCH} and its methods instead */
  @Deprecated
  public static final String HBASE_SLEEP_MS_PUT_BATCH_PROP = HBASE_SLEEP_MS_PUT_BATCH.key();
  /** @deprecated Use {@link #HBASE_SLEEP_MS_GET_BATCH} and its methods instead */
  @Deprecated
  public static final String HBASE_SLEEP_MS_GET_BATCH_PROP = HBASE_SLEEP_MS_GET_BATCH.key();
  /** @deprecated Use {@link #HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG} and its methods instead */
  @Deprecated
  public static final String HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS = HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG.key();
  /** @deprecated Use {@link #HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG} and its methods instead */
  @Deprecated
  public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG.defaultValue();
  /** @deprecated Use {@link #HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG} and its methods instead */
  @Deprecated
  public static final String HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS = HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG.key();
  /** @deprecated Use {@link #HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG} and its methods instead */
  @Deprecated
  public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG.defaultValue();
  /** @deprecated Use {@link #HBASE_ZK_PATH_QPS_ROOT_CFG} and its methods instead */
  @Deprecated
  public static final String HBASE_ZK_PATH_QPS_ROOT = HBASE_ZK_PATH_QPS_ROOT_CFG.key();
  /** @deprecated Use {@link #HBASE_ZK_PATH_QPS_ROOT_CFG} and its methods instead */
  @Deprecated
  public static final String DEFAULT_HBASE_ZK_PATH_QPS_ROOT = HBASE_ZK_PATH_QPS_ROOT_CFG.defaultValue();
  /** @deprecated Use {@link #HBASE_INDEX_UPDATE_PARTITION_PATH_CFG} and its methods instead */
  @Deprecated
  public static final String HBASE_INDEX_UPDATE_PARTITION_PATH = HBASE_INDEX_UPDATE_PARTITION_PATH_CFG.key();
  /** @deprecated Use {@link #HBASE_INDEX_UPDATE_PARTITION_PATH_CFG} and its methods instead */
  @Deprecated
  public static final Boolean DEFAULT_HBASE_INDEX_UPDATE_PARTITION_PATH = HBASE_INDEX_UPDATE_PARTITION_PATH_CFG.defaultValue();
  /** @deprecated Use {@link #HBASE_INDEX_ROLLBACK_SYNC_CFG} and its methods instead */
  @Deprecated
  public static final String HBASE_INDEX_ROLLBACK_SYNC = HBASE_INDEX_ROLLBACK_SYNC_CFG.key();
  /** @deprecated Use {@link #HBASE_INDEX_ROLLBACK_SYNC_CFG} and its methods instead */
  @Deprecated
  public static final Boolean DEFAULT_HBASE_INDEX_ROLLBACK_SYNC = HBASE_INDEX_ROLLBACK_SYNC_CFG.defaultValue();

  private HoodieHBaseIndexConfig() {
    super();
  }

  public static HoodieHBaseIndexConfig.Builder newBuilder() {
    return new HoodieHBaseIndexConfig.Builder();
  }

  public static class Builder {

    private final HoodieHBaseIndexConfig hBaseIndexConfig = new HoodieHBaseIndexConfig();

    public HoodieHBaseIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hBaseIndexConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieHBaseIndexConfig.Builder fromProperties(Properties props) {
      this.hBaseIndexConfig.getProps().putAll(props);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkQuorum(String zkString) {
      hBaseIndexConfig.setValue(HBASE_ZKQUORUM, zkString);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkPort(int port) {
      hBaseIndexConfig.setValue(HBASE_ZKPORT, String.valueOf(port));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseTableName(String tableName) {
      hBaseIndexConfig.setValue(HBASE_TABLENAME, tableName);
      return this;
    }

    public Builder hbaseZkZnodeQPSPath(String zkZnodeQPSPath) {
      hBaseIndexConfig.setValue(HBASE_ZK_PATH_QPS_ROOT_CFG, zkZnodeQPSPath);
      return this;
    }

    public Builder hbaseIndexGetBatchSize(int getBatchSize) {
      hBaseIndexConfig.setValue(HBASE_GET_BATCH_SIZE, String.valueOf(getBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSize(int putBatchSize) {
      hBaseIndexConfig.setValue(HBASE_PUT_BATCH_SIZE, String.valueOf(putBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSizeAutoCompute(boolean putBatchSizeAutoCompute) {
      hBaseIndexConfig.setValue(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE, String.valueOf(putBatchSizeAutoCompute));
      return this;
    }

    public Builder hbaseIndexDesiredPutsTime(int desiredPutsTime) {
      hBaseIndexConfig.setValue(HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECONDS, String.valueOf(desiredPutsTime));
      return this;
    }

    public Builder hbaseIndexShouldComputeQPSDynamically(boolean shouldComputeQPsDynamically) {
      hBaseIndexConfig.setValue(HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY_CFG, String.valueOf(shouldComputeQPsDynamically));
      return this;
    }

    public Builder hbaseIndexQPSFraction(float qpsFraction) {
      hBaseIndexConfig.setValue(HBASE_QPS_FRACTION, String.valueOf(qpsFraction));
      return this;
    }

    public Builder hbaseIndexMinQPSFraction(float minQPSFraction) {
      hBaseIndexConfig.setValue(HBASE_MIN_QPS_FRACTION, String.valueOf(minQPSFraction));
      return this;
    }

    public Builder hbaseIndexMaxQPSFraction(float maxQPSFraction) {
      hBaseIndexConfig.setValue(HBASE_MAX_QPS_FRACTION, String.valueOf(maxQPSFraction));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenPutBatch(int sleepMsBetweenPutBatch) {
      hBaseIndexConfig.setValue(HBASE_SLEEP_MS_PUT_BATCH, String.valueOf(sleepMsBetweenPutBatch));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenGetBatch(int sleepMsBetweenGetBatch) {
      hBaseIndexConfig.setValue(HBASE_SLEEP_MS_GET_BATCH, String.valueOf(sleepMsBetweenGetBatch));
      return this;
    }

    public Builder hbaseIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hBaseIndexConfig.setValue(HBASE_INDEX_UPDATE_PARTITION_PATH_CFG, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder hbaseIndexRollbackSync(boolean rollbackSync) {
      hBaseIndexConfig.setValue(HBASE_INDEX_ROLLBACK_SYNC_CFG, String.valueOf(rollbackSync));
      return this;
    }

    public Builder withQPSResourceAllocatorType(String qpsResourceAllocatorClass) {
      hBaseIndexConfig.setValue(HBASE_INDEX_QPS_ALLOCATOR_CLASS_CFG, qpsResourceAllocatorClass);
      return this;
    }

    public Builder hbaseIndexZkSessionTimeout(int zkSessionTimeout) {
      hBaseIndexConfig.setValue(HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS_CFG, String.valueOf(zkSessionTimeout));
      return this;
    }

    public Builder hbaseIndexZkConnectionTimeout(int zkConnectionTimeout) {
      hBaseIndexConfig.setValue(HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS_CFG, String.valueOf(zkConnectionTimeout));
      return this;
    }

    public Builder hbaseZkZnodeParent(String zkZnodeParent) {
      hBaseIndexConfig.setValue(HBASE_ZK_ZNODEPARENT_CFG, zkZnodeParent);
      return this;
    }

    /**
     * <p>
     * Method to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
     * limit the aggregate QPS generated across various jobs to an HBase Region Server.
     * </p>
     * <p>
     * It is recommended to set this value based on your global indexing throughput needs and most importantly, how much
     * your HBase installation is able to tolerate without Region Servers going down.
     * </p>
     */
    public HoodieHBaseIndexConfig.Builder hbaseIndexMaxQPSPerRegionServer(int maxQPSPerRegionServer) {
      // This should be same across various jobs
      hBaseIndexConfig.setValue(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER,
          String.valueOf(maxQPSPerRegionServer));
      return this;
    }

    public HoodieHBaseIndexConfig build() {
      hBaseIndexConfig.setDefaults(HoodieHBaseIndexConfig.class.getName());
      return hBaseIndexConfig;
    }

  }
}
