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

import org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HoodieHBaseIndexConfig extends DefaultHoodieConfig {

  public static final String HBASE_ZKQUORUM_PROP = "hoodie.index.hbase.zkquorum";
  public static final String HBASE_ZKPORT_PROP = "hoodie.index.hbase.zkport";
  public static final String HBASE_TABLENAME_PROP = "hoodie.index.hbase.table";
  public static final String HBASE_GET_BATCH_SIZE_PROP = "hoodie.index.hbase.get.batch.size";
  public static final String HBASE_ZK_ZNODEPARENT = "hoodie.index.hbase.zknode.path";
  /**
   * Note that if HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP is set to true, this batch size will not be honored for HBase
   * Puts.
   */
  public static final String HBASE_PUT_BATCH_SIZE_PROP = "hoodie.index.hbase.put.batch.size";

  /**
   * Property to set which implementation of HBase QPS resource allocator to be used.
   */
  public static final String HBASE_INDEX_QPS_ALLOCATOR_CLASS = "hoodie.index.hbase.qps.allocator.class";
  public static final String DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS = DefaultHBaseQPSResourceAllocator.class.getName();
  /**
   * Property to set to enable auto computation of put batch size.
   */
  public static final String HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP = "hoodie.index.hbase.put.batch.size.autocompute";
  public static final String DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE = "false";
  /**
   * Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3
   * jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then
   * this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public static final String HBASE_QPS_FRACTION_PROP = "hoodie.index.hbase.qps.fraction";
  /**
   * Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
   * limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this
   * value based on global indexing throughput needs and most importantly, how much the HBase installation in use is
   * able to tolerate without Region Servers going down.
   */
  public static String HBASE_MAX_QPS_PER_REGION_SERVER_PROP = "hoodie.index.hbase.max.qps.per.region.server";
  /**
   * Default batch size, used only for Get, but computed for Put.
   */
  public static final int DEFAULT_HBASE_BATCH_SIZE = 100;
  /**
   * A low default value.
   */
  public static final int DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER = 1000;
  /**
   * Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.
   */
  public static final float DEFAULT_HBASE_QPS_FRACTION = 0.5f;

  /**
   * Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on volume.
   */
  public static final String HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = "hoodie.index.hbase.dynamic_qps";
  public static final boolean DEFAULT_HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = false;
  /**
   * Min and Max for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads.
   */
  public static final String HBASE_MIN_QPS_FRACTION_PROP = "hoodie.index.hbase.min.qps.fraction";
  public static final String DEFAULT_HBASE_MIN_QPS_FRACTION_PROP = "0.002";

  public static final String HBASE_MAX_QPS_FRACTION_PROP = "hoodie.index.hbase.max.qps.fraction";
  public static final String DEFAULT_HBASE_MAX_QPS_FRACTION_PROP = "0.06";
  /**
   * Hoodie index desired puts operation time in seconds.
   */
  public static final String HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = "hoodie.index.hbase.desired_puts_time_in_secs";
  public static final int DEFAULT_HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = 600;
  public static final String HBASE_SLEEP_MS_PUT_BATCH_PROP = "hoodie.index.hbase.sleep.ms.for.put.batch";
  public static final String HBASE_SLEEP_MS_GET_BATCH_PROP = "hoodie.index.hbase.sleep.ms.for.get.batch";
  public static final String HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS = "hoodie.index.hbase.zk.session_timeout_ms";
  public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 60 * 1000;
  public static final String HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS =
      "hoodie.index.hbase.zk.connection_timeout_ms";
  public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 15 * 1000;
  public static final String HBASE_ZK_PATH_QPS_ROOT = "hoodie.index.hbase.zkpath.qps_root";
  public static final String DEFAULT_HBASE_ZK_PATH_QPS_ROOT = "/QPS_ROOT";

  public HoodieHBaseIndexConfig(final Properties props) {
    super(props);
  }

  public static HoodieHBaseIndexConfig.Builder newBuilder() {
    return new HoodieHBaseIndexConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieHBaseIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        this.props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public HoodieHBaseIndexConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkQuorum(String zkString) {
      props.setProperty(HBASE_ZKQUORUM_PROP, zkString);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkPort(int port) {
      props.setProperty(HBASE_ZKPORT_PROP, String.valueOf(port));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseTableName(String tableName) {
      props.setProperty(HBASE_TABLENAME_PROP, tableName);
      return this;
    }

    public Builder hbaseZkZnodeQPSPath(String zkZnodeQPSPath) {
      props.setProperty(HBASE_ZK_PATH_QPS_ROOT, zkZnodeQPSPath);
      return this;
    }

    public Builder hbaseIndexGetBatchSize(int getBatchSize) {
      props.setProperty(HBASE_GET_BATCH_SIZE_PROP, String.valueOf(getBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSize(int putBatchSize) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_PROP, String.valueOf(putBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSizeAutoCompute(boolean putBatchSizeAutoCompute) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP, String.valueOf(putBatchSizeAutoCompute));
      return this;
    }

    public Builder hbaseIndexDesiredPutsTime(int desiredPutsTime) {
      props.setProperty(HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS, String.valueOf(desiredPutsTime));
      return this;
    }

    public Builder hbaseIndexShouldComputeQPSDynamically(boolean shouldComputeQPsDynamically) {
      props.setProperty(HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY, String.valueOf(shouldComputeQPsDynamically));
      return this;
    }

    public Builder hbaseIndexQPSFraction(float qpsFraction) {
      props.setProperty(HBASE_QPS_FRACTION_PROP, String.valueOf(qpsFraction));
      return this;
    }

    public Builder hbaseIndexMinQPSFraction(float minQPSFraction) {
      props.setProperty(HBASE_MIN_QPS_FRACTION_PROP, String.valueOf(minQPSFraction));
      return this;
    }

    public Builder hbaseIndexMaxQPSFraction(float maxQPSFraction) {
      props.setProperty(HBASE_MAX_QPS_FRACTION_PROP, String.valueOf(maxQPSFraction));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenPutBatch(int sleepMsBetweenPutBatch) {
      props.setProperty(HBASE_SLEEP_MS_PUT_BATCH_PROP, String.valueOf(sleepMsBetweenPutBatch));
      return this;
    }

    public Builder withQPSResourceAllocatorType(String qpsResourceAllocatorClass) {
      props.setProperty(HBASE_INDEX_QPS_ALLOCATOR_CLASS, qpsResourceAllocatorClass);
      return this;
    }

    public Builder hbaseIndexZkSessionTimeout(int zkSessionTimeout) {
      props.setProperty(HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS, String.valueOf(zkSessionTimeout));
      return this;
    }

    public Builder hbaseIndexZkConnectionTimeout(int zkConnectionTimeout) {
      props.setProperty(HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS, String.valueOf(zkConnectionTimeout));
      return this;
    }

    public Builder hbaseZkZnodeParent(String zkZnodeParent) {
      props.setProperty(HBASE_ZK_ZNODEPARENT, zkZnodeParent);
      return this;
    }

    /**
     * <p>
     * Method to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
     * limit the aggregate QPS generated across various jobs to an Hbase Region Server.
     * </p>
     * <p>
     * It is recommended to set this value based on your global indexing throughput needs and most importantly, how much
     * your HBase installation is able to tolerate without Region Servers going down.
     * </p>
     */
    public HoodieHBaseIndexConfig.Builder hbaseIndexMaxQPSPerRegionServer(int maxQPSPerRegionServer) {
      // This should be same across various jobs
      props.setProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP,
          String.valueOf(maxQPSPerRegionServer));
      return this;
    }

    public HoodieHBaseIndexConfig build() {
      HoodieHBaseIndexConfig config = new HoodieHBaseIndexConfig(props);
      setDefaultOnCondition(props, !props.containsKey(HBASE_GET_BATCH_SIZE_PROP), HBASE_GET_BATCH_SIZE_PROP,
          String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_PUT_BATCH_SIZE_PROP), HBASE_PUT_BATCH_SIZE_PROP,
          String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP),
          HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP, String.valueOf(DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_QPS_FRACTION_PROP), HBASE_QPS_FRACTION_PROP,
          String.valueOf(DEFAULT_HBASE_QPS_FRACTION));
      setDefaultOnCondition(props, !props.containsKey(HBASE_MAX_QPS_PER_REGION_SERVER_PROP),
          HBASE_MAX_QPS_PER_REGION_SERVER_PROP, String.valueOf(DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER));
      setDefaultOnCondition(props, !props.containsKey(HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY),
          HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY, String.valueOf(DEFAULT_HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY));
      setDefaultOnCondition(props, !props.containsKey(HBASE_INDEX_QPS_ALLOCATOR_CLASS), HBASE_INDEX_QPS_ALLOCATOR_CLASS,
          String.valueOf(DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS));
      setDefaultOnCondition(props, !props.containsKey(HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS),
          HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS, String.valueOf(DEFAULT_HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS));
      setDefaultOnCondition(props, !props.containsKey(HBASE_ZK_PATH_QPS_ROOT), HBASE_ZK_PATH_QPS_ROOT,
          String.valueOf(DEFAULT_HBASE_ZK_PATH_QPS_ROOT));
      setDefaultOnCondition(props, !props.containsKey(HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS),
          HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS, String.valueOf(DEFAULT_ZK_SESSION_TIMEOUT_MS));
      setDefaultOnCondition(props, !props.containsKey(HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS),
          HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS, String.valueOf(DEFAULT_ZK_CONNECTION_TIMEOUT_MS));
      setDefaultOnCondition(props, !props.containsKey(HBASE_INDEX_QPS_ALLOCATOR_CLASS), HBASE_INDEX_QPS_ALLOCATOR_CLASS,
          String.valueOf(DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS));
      return config;
    }

  }
}
