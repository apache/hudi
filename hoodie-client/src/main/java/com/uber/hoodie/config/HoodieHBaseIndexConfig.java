/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.config;

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
   * Note that if HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP is set to true, this batch size will not
   * be honored for HBase Puts
   */
  public static final String HBASE_PUT_BATCH_SIZE_PROP = "hoodie.index.hbase.put.batch.size";
  /**
   * Property to set to enable auto computation of put batch size
   */
  public static final String HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP = "hoodie.index.hbase.put.batch.size.autocompute";
  public static final String DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE = "false";
  /**
   * Property to set the fraction of the global share of QPS that should be allocated to this job.
   * Let's say there are 3 jobs which have input size in terms of number of rows required for
   * HbaseIndexing as x, 2x, 3x respectively. Then this fraction for the jobs would be (0.17) 1/6,
   * 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public static final String HBASE_QPS_FRACTION_PROP = "hoodie.index.hbase.qps.fraction";
  /**
   * Property to set maximum QPS allowed per Region Server. This should be same across various
   * jobs. This is intended to limit the aggregate QPS generated across various jobs to an Hbase
   * Region Server. It is recommended to set this value based on global indexing throughput needs
   * and most importantly, how much the HBase installation in use is able to tolerate without
   * Region Servers going down.
   */
  public static String HBASE_MAX_QPS_PER_REGION_SERVER_PROP = "hoodie.index.hbase.max.qps.per.region.server";
  /**
   * Default batch size, used only for Get, but computed for Put
   */
  public static final int DEFAULT_HBASE_BATCH_SIZE = 100;
  /**
   * A low default value.
   */
  public static final int DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER = 1000;
  /**
   * Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming
   * Region Servers
   */
  public static final float DEFAULT_HBASE_QPS_FRACTION = 0.5f;

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

    public HoodieHBaseIndexConfig.Builder hbaseIndexGetBatchSize(int getBatchSize) {
      props.setProperty(HBASE_GET_BATCH_SIZE_PROP, String.valueOf(getBatchSize));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseIndexPutBatchSize(int putBatchSize) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_PROP, String.valueOf(putBatchSize));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseIndexPutBatchSizeAutoCompute(
        boolean putBatchSizeAutoCompute) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP,
          String.valueOf(putBatchSizeAutoCompute));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseIndexQPSFraction(float qpsFraction) {
      props.setProperty(HoodieHBaseIndexConfig.HBASE_QPS_FRACTION_PROP,
          String.valueOf(qpsFraction));
      return this;
    }

    public Builder hbaseZkZnodeParent(String zkZnodeParent) {
      props.setProperty(HBASE_ZK_ZNODEPARENT, zkZnodeParent);
      return this;
    }

    /**
     * <p>
     * Method to set maximum QPS allowed per Region Server. This should be same across various
     * jobs. This is intended to limit the aggregate QPS generated across various jobs to an
     * Hbase Region Server.
     * </p>
     * <p>
     * It is recommended to set this value based on your global indexing throughput needs and
     * most importantly, how much your HBase installation is able to tolerate without Region
     * Servers going down.
     * </p>
     */
    public HoodieHBaseIndexConfig.Builder hbaseIndexMaxQPSPerRegionServer(
        int maxQPSPerRegionServer) {
      // This should be same across various jobs
      props.setProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP,
          String.valueOf(maxQPSPerRegionServer));
      return this;
    }

    public HoodieHBaseIndexConfig build() {
      HoodieHBaseIndexConfig config = new HoodieHBaseIndexConfig(props);
      setDefaultOnCondition(props, !props.containsKey(HBASE_GET_BATCH_SIZE_PROP),
          HBASE_GET_BATCH_SIZE_PROP, String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_PUT_BATCH_SIZE_PROP),
          HBASE_PUT_BATCH_SIZE_PROP, String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP),
          HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP,
          String.valueOf(DEFAULT_HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE));
      setDefaultOnCondition(props, !props.containsKey(HBASE_QPS_FRACTION_PROP),
          HBASE_QPS_FRACTION_PROP, String.valueOf(DEFAULT_HBASE_QPS_FRACTION));
      setDefaultOnCondition(props,
          !props.containsKey(HBASE_MAX_QPS_PER_REGION_SERVER_PROP),
          HBASE_MAX_QPS_PER_REGION_SERVER_PROP, String.valueOf(
              DEFAULT_HBASE_MAX_QPS_PER_REGION_SERVER));
      return config;
    }

  }
}
