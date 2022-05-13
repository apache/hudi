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

package org.apache.hudi.internal;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * Helper class for HoodieDataSourceInternalWriter used by Spark datasource v2.
 */
public class DataSourceInternalWriterHelper {

  private static final Logger LOG = LogManager.getLogger(DataSourceInternalWriterHelper.class);
  public static final String INSTANT_TIME_OPT_KEY = "hoodie.instant.time";

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final SparkRDDWriteClient writeClient;
  private final HoodieTable hoodieTable;
  private final WriteOperationType operationType;
  private Map<String, String> extraMetadata;

  public DataSourceInternalWriterHelper(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkSession sparkSession, Configuration configuration, Map<String, String> extraMetadata) {
    this.instantTime = instantTime;
    this.operationType = WriteOperationType.BULK_INSERT;
    this.extraMetadata = extraMetadata;
    this.writeClient  = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext())), writeConfig);
    writeClient.setOperationType(operationType);
    writeClient.startCommitWithTime(instantTime);

    this.metaClient = HoodieTableMetaClient.builder().setConf(configuration).setBasePath(writeConfig.getBasePath()).build();
    this.metaClient.validateTableProperties(writeConfig.getProps());
    this.hoodieTable = HoodieSparkTable.create(writeConfig, new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext())), metaClient);
  }

  public boolean useCommitCoordinator() {
    return true;
  }

  public void onDataWriterCommit(String message) {
    LOG.info("Received commit of a data writer = " + message);
  }

  public void commit(List<HoodieWriteStat> writeStatList) {
    try {
      writeClient.commitStats(instantTime, writeStatList, Option.of(extraMetadata),
          CommitUtils.getCommitActionType(operationType, metaClient.getTableType()), Option.empty(), Option.empty());
    } catch (Exception ioe) {
      throw new HoodieException(ioe.getMessage(), ioe);
    } finally {
      writeClient.close();
    }
  }

  public void abort() {
    LOG.error("Commit " + instantTime + " aborted ");
    writeClient.rollback(instantTime);
    writeClient.close();
  }

  public void createInflightCommit() {
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(State.REQUESTED,
                          CommitUtils.getCommitActionType(operationType, metaClient.getTableType()),
                          instantTime), Option.empty());
  }

  public HoodieTable getHoodieTable() {
    return hoodieTable;
  }

  public WriteOperationType getWriteOperationType() {
    return operationType;
  }
}
