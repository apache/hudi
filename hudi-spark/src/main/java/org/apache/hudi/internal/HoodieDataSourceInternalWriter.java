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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of {@link DataSourceWriter} for datasource "hudi.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalWriter implements DataSourceWriter {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieDataSourceInternalWriter.class);
  public static final String INSTANT_TIME_OPT_KEY = "hoodie.instant.time";

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final HoodieWriteClient writeClient;
  private final HoodieTable hoodieTable;
  private final WriteOperationType operationType;

  public HoodieDataSourceInternalWriter(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
                                        SparkSession sparkSession, Configuration configuration) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.operationType = WriteOperationType.BULK_INSERT;
    this.writeClient  = new HoodieWriteClient<>(new JavaSparkContext(sparkSession.sparkContext()), writeConfig, true);
    writeClient.setOperationType(operationType);
    writeClient.startCommitWithTime(instantTime);
    this.metaClient = new HoodieTableMetaClient(configuration, writeConfig.getBasePath());
    this.hoodieTable = HoodieTable.create(metaClient, writeConfig, metaClient.getHadoopConf());
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime), Option.empty());
    if (WriteOperationType.BULK_INSERT == operationType) {
      return new HoodieBulkInsertDataInternalWriterFactory(hoodieTable, writeConfig, instantTime, structType);
    } else {
      throw new IllegalArgumentException("Write Operation Type + " + operationType + " not supported ");
    }
  }

  @Override
  public boolean useCommitCoordinator() {
    return true;
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    LOG.info("Received commit of a data writer =" + message);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<HoodieWriteStat> writeStatList = Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
            .flatMap(m -> m.getWriteStatuses().stream().map(m2 -> m2.getStat())).collect(Collectors.toList());

    try {
      writeClient.commitStats(instantTime, writeStatList, Option.empty());
    } catch (Exception ioe) {
      throw new HoodieException(ioe.getMessage(), ioe);
    } finally {
      writeClient.close();
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.error("Commit " + instantTime + " aborted ");
    writeClient.rollback(instantTime);
    writeClient.close();
  }
}
