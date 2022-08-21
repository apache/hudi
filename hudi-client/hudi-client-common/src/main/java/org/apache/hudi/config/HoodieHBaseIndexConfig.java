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

  public static final ConfigProperty<String> ZKQUORUM = ConfigProperty
      .key("hoodie.index.hbase.zkquorum")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase ZK Quorum url to connect to");

  public static final ConfigProperty<String> ZKPORT = ConfigProperty
      .key("hoodie.index.hbase.zkport")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase ZK Quorum port to connect to");

  public static final ConfigProperty<String> TABLENAME = ConfigProperty
      .key("hoodie.index.hbase.table")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. HBase Table name to use as the index. "
          + "Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table");

  public static final ConfigProperty<Integer> GET_BATCH_SIZE = ConfigProperty
      .key("hoodie.index.hbase.get.batch.size")
      .defaultValue(100)
      .withDocumentation("Controls the batch size for performing gets against HBase. "
          + "Batching improves throughput, by saving round trips.");

  public static final ConfigProperty<String> ZK_NODE_PATH = ConfigProperty
      .key("hoodie.index.hbase.zknode.path")
      .noDefaultValue()
      .withDocumentation("Only applies if index type is HBASE. This is the root znode that will contain "
          + "all the znodes created/used by HBase");

  public static final ConfigProperty<Integer> PUT_BATCH_SIZE = ConfigProperty
      .key("hoodie.index.hbase.put.batch.size")
      .defaultValue(100)
      .withDocumentation("Controls the batch size for performing puts against HBase. "
          + "Batching improves throughput, by saving round trips.");

  public static final ConfigProperty<String> QPS_ALLOCATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.index.hbase.qps.allocator.class")
      .defaultValue(DefaultHBaseQPSResourceAllocator.class.getName())
      .withDocumentation("Property to set which implementation of HBase QPS resource allocator to be used, which"
          + "controls the batching rate dynamically.");

  public static final ConfigProperty<Boolean> PUT_BATCH_SIZE_AUTO_COMPUTE = ConfigProperty
      .key("hoodie.index.hbase.put.batch.size.autocompute")
      .defaultValue(false)
      .withDocumentation("Property to set to enable auto computation of put batch size");

  public static final ConfigProperty<Float> QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.qps.fraction")
      .defaultValue(0.5f)
      .withDocumentation("Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3"
          + " jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then"
          + " this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively."
          + " Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.");

  public static final ConfigProperty<Integer> MAX_QPS_PER_REGION_SERVER = ConfigProperty
      .key("hoodie.index.hbase.max.qps.per.region.server")
      .defaultValue(1000)
      .withDocumentation("Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to\n"
          + " limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this\n"
          + " value based on global indexing throughput needs and most importantly, how much the HBase installation in use is\n"
          + " able to tolerate without Region Servers going down.");

  public static final ConfigProperty<Boolean> COMPUTE_QPS_DYNAMICALLY = ConfigProperty
      .key("hoodie.index.hbase.dynamic_qps")
      .defaultValue(false)
      .withDocumentation("Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on write volume.");

  public static final ConfigProperty<String> MIN_QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.min.qps.fraction")
      .noDefaultValue()
      .withDocumentation("Minimum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads");

  public static final ConfigProperty<String> MAX_QPS_FRACTION = ConfigProperty
      .key("hoodie.index.hbase.max.qps.fraction")
      .noDefaultValue()
      .withDocumentation("Maximum for HBASE_QPS_FRACTION_PROP to stabilize skewed write workloads");

  public static final ConfigProperty<Integer> DESIRED_PUTS_TIME_IN_SECONDS = ConfigProperty
      .key("hoodie.index.hbase.desired_puts_time_in_secs")
      .defaultValue(600)
      .withDocumentation("");

  public static final ConfigProperty<String> SLEEP_MS_FOR_PUT_BATCH = ConfigProperty
      .key("hoodie.index.hbase.sleep.ms.for.put.batch")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> SLEEP_MS_FOR_GET_BATCH = ConfigProperty
      .key("hoodie.index.hbase.sleep.ms.for.get.batch")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<Integer> ZK_SESSION_TIMEOUT_MS = ConfigProperty
      .key("hoodie.index.hbase.zk.session_timeout_ms")
      .defaultValue(60 * 1000)
      .withDocumentation("Session timeout value to use for Zookeeper failure detection, for the HBase client."
          + "Lower this value, if you want to fail faster.");

  public static final ConfigProperty<Integer> ZK_CONNECTION_TIMEOUT_MS = ConfigProperty
      .key("hoodie.index.hbase.zk.connection_timeout_ms")
      .defaultValue(15 * 1000)
      .withDocumentation("Timeout to use for establishing connection with zookeeper, from HBase client.");

  public static final ConfigProperty<String> ZKPATH_QPS_ROOT = ConfigProperty
      .key("hoodie.index.hbase.zkpath.qps_root")
      .defaultValue("/QPS_ROOT")
      .withDocumentation("chroot in zookeeper, to use for all qps allocation co-ordination.");

  public static final ConfigProperty<Boolean> UPDATE_PARTITION_PATH_ENABLE = ConfigProperty
      .key("hoodie.hbase.index.update.partition.path")
      .defaultValue(false)
      .withDocumentation("Only applies if index type is HBASE. "
          + "When an already existing record is upserted to a new partition compared to whats in storage, "
          + "this config when set, will delete old record in old partition "
          + "and will insert it as new record in new partition.");

  public static final ConfigProperty<Boolean> ROLLBACK_SYNC_ENABLE = ConfigProperty
      .key("hoodie.index.hbase.rollback.sync")
      .defaultValue(false)
      .withDocumentation("When set to true, the rollback method will delete the last failed task index. "
          + "The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback");

  public static final ConfigProperty<String> SECURITY_AUTHENTICATION = ConfigProperty
      .key("hoodie.index.hbase.security.authentication")
      .defaultValue("simple")
      .withDocumentation("Property to decide if the hbase cluster secure authentication is enabled or not. "
          + "Possible values are 'simple' (no authentication), and 'kerberos'.");

  public static final ConfigProperty<String> KERBEROS_USER_KEYTAB = ConfigProperty
      .key("hoodie.index.hbase.kerberos.user.keytab")
      .noDefaultValue()
      .withDocumentation("File name of the kerberos keytab file for connecting to the hbase cluster.");

  public static final ConfigProperty<String> KERBEROS_USER_PRINCIPAL = ConfigProperty
      .key("hoodie.index.hbase.kerberos.user.principal")
      .noDefaultValue()
      .withDocumentation("The kerberos principal name for connecting to the hbase cluster.");

  public static final ConfigProperty<String> REGIONSERVER_PRINCIPAL = ConfigProperty
      .key("hoodie.index.hbase.regionserver.kerberos.principal")
      .noDefaultValue()
      .withDocumentation("The value of hbase.regionserver.kerberos.principal in hbase cluster.");

  public static final ConfigProperty<String> MASTER_PRINCIPAL = ConfigProperty
      .key("hoodie.index.hbase.master.kerberos.principal")
      .noDefaultValue()
      .withDocumentation("The value of hbase.master.kerberos.principal in hbase cluster.");

  public static final ConfigProperty<Integer> BUCKET_NUMBER = ConfigProperty
      .key("hoodie.index.hbase.bucket.number")
      .defaultValue(8)
      .withDocumentation("Only applicable when using RebalancedSparkHoodieHBaseIndex, same as hbase regions count can get the best performance");

  /**
   * @deprecated Use {@link #ZKQUORUM} and its methods instead
   */
  @Deprecated
  public static final String HBASE_ZKQUORUM_PROP = ZKQUORUM.key();
  /**
   * @deprecated Use {@link #ZKPORT} and its methods instead
   */
  @Deprecated
  public static final String HBASE_ZKPORT_PROP = ZKPORT.key();
  /**
   * @deprecated Use {@link #TABLENAME} and its methods instead
   */
  @Deprecated
  public static final String HBASE_TABLENAME_PROP = TABLENAME.key();
  /**
   * @deprecated Use {@link #GET_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HBASE_GET_BATCH_SIZE_PROP = GET_BATCH_SIZE.key();
  /**
   * @deprecated Use {@link #ZK_NODE_PATH} and its methods instead
   */
  @Deprecated
  public static final String HBASE_ZK_ZNODEPARENT = ZK_NODE_PATH.key();
  /**
   * @deprecated Use {@link #PUT_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HBASE_PUT_BATCH_SIZE_PROP = PUT_BATCH_SIZE.key();
  /**
   * @deprecated Use {@link #QPS_ALLOCATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String HBASE_INDEX_QPS_ALLOCATOR_CLASS = QPS_ALLOCATOR_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #QPS_ALLOCATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS = QPS_ALLOCATOR_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #PUT_BATCH_SIZE_AUTO_COMPUTE} and its methods instead
   */
  @Deprecated
  public static final String HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP = PUT_BATCH_SIZE_AUTO_COMPUTE.key();
  /**
   * @deprecated Use {@link #PUT_BATCH_SIZE_AUTO_COMPUTE} and its methods instead
   */
  @Deprecated
  public static final Boolean DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE = PUT_BATCH_SIZE_AUTO_COMPUTE.defaultValue();
  /**
   * @deprecated Use {@link #MAX_QPS_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String HBASE_QPS_FRACTION_PROP = QPS_FRACTION.key();
  /**
   * @deprecated Use {@link #MAX_QPS_PER_REGION_SERVER} and its methods instead
   */
  @Deprecated
  public static final String HBASE_MAX_QPS_PER_REGION_SERVER_PROP = MAX_QPS_PER_REGION_SERVER.key();
  @Deprecated
  public static final int DEFAULT_HBASE_BATCH_SIZE = 100;
  /**
   * @deprecated Use {@link #MAX_QPS_PER_REGION_SERVER} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER = MAX_QPS_PER_REGION_SERVER.defaultValue();
  /**
   * @deprecated Use {@link #QPS_FRACTION} and its methods instead
   */
  @Deprecated
  public static final float DEFAULT_HBASE_QPS_FRACTION = QPS_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #COMPUTE_QPS_DYNAMICALLY} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = COMPUTE_QPS_DYNAMICALLY.key();
  /**
   * @deprecated Use {@link #COMPUTE_QPS_DYNAMICALLY} and its methods instead
   */
  @Deprecated
  public static final boolean DEFAULT_HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = COMPUTE_QPS_DYNAMICALLY.defaultValue();
  /**
   * @deprecated Use {@link #MIN_QPS_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String HBASE_MIN_QPS_FRACTION_PROP = MIN_QPS_FRACTION.key();
  /**
   * @deprecated Use {@link #MAX_QPS_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String HBASE_MAX_QPS_FRACTION_PROP = MAX_QPS_FRACTION.key();
  /**
   * @deprecated Use {@link #DESIRED_PUTS_TIME_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = DESIRED_PUTS_TIME_IN_SECONDS.key();
  /**
   * @deprecated Use {@link #DESIRED_PUTS_TIME_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = DESIRED_PUTS_TIME_IN_SECONDS.defaultValue();
  /**
   * @deprecated Use {@link #SLEEP_MS_FOR_PUT_BATCH} and its methods instead
   */
  @Deprecated
  public static final String HBASE_SLEEP_MS_PUT_BATCH_PROP = SLEEP_MS_FOR_PUT_BATCH.key();
  /**
   * @deprecated Use {@link #SLEEP_MS_FOR_GET_BATCH} and its methods instead
   */
  @Deprecated
  public static final String HBASE_SLEEP_MS_GET_BATCH_PROP = SLEEP_MS_FOR_GET_BATCH.key();
  /**
   * @deprecated Use {@link #ZK_SESSION_TIMEOUT_MS} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS = ZK_SESSION_TIMEOUT_MS.key();
  /**
   * @deprecated Use {@link #ZK_SESSION_TIMEOUT_MS} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = ZK_SESSION_TIMEOUT_MS.defaultValue();
  /**
   * @deprecated Use {@link #ZK_CONNECTION_TIMEOUT_MS} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS = ZK_CONNECTION_TIMEOUT_MS.key();
  /**
   * @deprecated Use {@link #ZK_CONNECTION_TIMEOUT_MS} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = ZK_CONNECTION_TIMEOUT_MS.defaultValue();
  /**
   * @deprecated Use {@link #ZKPATH_QPS_ROOT} and its methods instead
   */
  @Deprecated
  public static final String HBASE_ZK_PATH_QPS_ROOT = ZKPATH_QPS_ROOT.key();
  /**
   * @deprecated Use {@link #ZKPATH_QPS_ROOT} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HBASE_ZK_PATH_QPS_ROOT = ZKPATH_QPS_ROOT.defaultValue();
  /**
   * @deprecated Use {@link #UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String HBASE_INDEX_UPDATE_PARTITION_PATH = UPDATE_PARTITION_PATH_ENABLE.key();
  /**
   * @deprecated Use {@link #UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final Boolean DEFAULT_HBASE_INDEX_UPDATE_PARTITION_PATH = UPDATE_PARTITION_PATH_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #ROLLBACK_SYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String HBASE_INDEX_ROLLBACK_SYNC = ROLLBACK_SYNC_ENABLE.key();
  /**
   * @deprecated Use {@link #ROLLBACK_SYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final Boolean DEFAULT_HBASE_INDEX_ROLLBACK_SYNC = ROLLBACK_SYNC_ENABLE.defaultValue();

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
      hBaseIndexConfig.setValue(ZKQUORUM, zkString);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkPort(int port) {
      hBaseIndexConfig.setValue(ZKPORT, String.valueOf(port));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseTableName(String tableName) {
      hBaseIndexConfig.setValue(TABLENAME, tableName);
      return this;
    }

    public Builder hbaseZkZnodeQPSPath(String zkZnodeQPSPath) {
      hBaseIndexConfig.setValue(ZKPATH_QPS_ROOT, zkZnodeQPSPath);
      return this;
    }

    public Builder hbaseIndexGetBatchSize(int getBatchSize) {
      hBaseIndexConfig.setValue(GET_BATCH_SIZE, String.valueOf(getBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSize(int putBatchSize) {
      hBaseIndexConfig.setValue(PUT_BATCH_SIZE, String.valueOf(putBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSizeAutoCompute(boolean putBatchSizeAutoCompute) {
      hBaseIndexConfig.setValue(PUT_BATCH_SIZE_AUTO_COMPUTE, String.valueOf(putBatchSizeAutoCompute));
      return this;
    }

    public Builder hbaseIndexDesiredPutsTime(int desiredPutsTime) {
      hBaseIndexConfig.setValue(DESIRED_PUTS_TIME_IN_SECONDS, String.valueOf(desiredPutsTime));
      return this;
    }

    public Builder hbaseIndexShouldComputeQPSDynamically(boolean shouldComputeQPsDynamically) {
      hBaseIndexConfig.setValue(COMPUTE_QPS_DYNAMICALLY, String.valueOf(shouldComputeQPsDynamically));
      return this;
    }

    public Builder hbaseIndexQPSFraction(float qpsFraction) {
      hBaseIndexConfig.setValue(QPS_FRACTION, String.valueOf(qpsFraction));
      return this;
    }

    public Builder hbaseIndexMinQPSFraction(float minQPSFraction) {
      hBaseIndexConfig.setValue(MIN_QPS_FRACTION, String.valueOf(minQPSFraction));
      return this;
    }

    public Builder hbaseIndexMaxQPSFraction(float maxQPSFraction) {
      hBaseIndexConfig.setValue(MAX_QPS_FRACTION, String.valueOf(maxQPSFraction));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenPutBatch(int sleepMsBetweenPutBatch) {
      hBaseIndexConfig.setValue(SLEEP_MS_FOR_PUT_BATCH, String.valueOf(sleepMsBetweenPutBatch));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenGetBatch(int sleepMsBetweenGetBatch) {
      hBaseIndexConfig.setValue(SLEEP_MS_FOR_GET_BATCH, String.valueOf(sleepMsBetweenGetBatch));
      return this;
    }

    public Builder hbaseIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hBaseIndexConfig.setValue(UPDATE_PARTITION_PATH_ENABLE, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder hbaseIndexRollbackSync(boolean rollbackSync) {
      hBaseIndexConfig.setValue(ROLLBACK_SYNC_ENABLE, String.valueOf(rollbackSync));
      return this;
    }

    public Builder withQPSResourceAllocatorType(String qpsResourceAllocatorClass) {
      hBaseIndexConfig.setValue(QPS_ALLOCATOR_CLASS_NAME, qpsResourceAllocatorClass);
      return this;
    }

    public Builder hbaseIndexZkSessionTimeout(int zkSessionTimeout) {
      hBaseIndexConfig.setValue(ZK_SESSION_TIMEOUT_MS, String.valueOf(zkSessionTimeout));
      return this;
    }

    public Builder hbaseIndexZkConnectionTimeout(int zkConnectionTimeout) {
      hBaseIndexConfig.setValue(ZK_CONNECTION_TIMEOUT_MS, String.valueOf(zkConnectionTimeout));
      return this;
    }

    public Builder hbaseZkZnodeParent(String zkZnodeParent) {
      hBaseIndexConfig.setValue(ZK_NODE_PATH, zkZnodeParent);
      return this;
    }

    public Builder hbaseSecurityAuthentication(String authentication) {
      hBaseIndexConfig.setValue(SECURITY_AUTHENTICATION, authentication);
      return this;
    }

    public Builder hbaseKerberosUserKeytab(String keytab) {
      hBaseIndexConfig.setValue(KERBEROS_USER_KEYTAB, keytab);
      return this;
    }

    public Builder hbaseKerberosUserPrincipal(String principal) {
      hBaseIndexConfig.setValue(KERBEROS_USER_PRINCIPAL, principal);
      return this;
    }

    public Builder hbaseKerberosRegionserverPrincipal(String principal) {
      hBaseIndexConfig.setValue(REGIONSERVER_PRINCIPAL, principal);
      return this;
    }

    public Builder hbaseKerberosMasterPrincipal(String principal) {
      hBaseIndexConfig.setValue(MASTER_PRINCIPAL, principal);
      return this;
    }

    public Builder hbaseRegionBucketNum(String bucketNum) {
      hBaseIndexConfig.setValue(BUCKET_NUMBER, bucketNum);
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
      hBaseIndexConfig.setValue(HoodieHBaseIndexConfig.MAX_QPS_PER_REGION_SERVER,
          String.valueOf(maxQPSPerRegionServer));
      return this;
    }

    public HoodieHBaseIndexConfig build() {
      hBaseIndexConfig.setDefaults(HoodieHBaseIndexConfig.class.getName());
      return hBaseIndexConfig;
    }

  }
}
