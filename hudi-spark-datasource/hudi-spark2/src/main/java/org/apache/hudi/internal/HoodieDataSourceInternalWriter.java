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

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link DataSourceWriter} for datasource "hudi.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalWriter implements DataSourceWriter, StreamWriter {

  private final String instantTime;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final DataSourceInternalWriterHelper dataSourceInternalWriterHelper;
  private final boolean populateMetaFields;
  private final Boolean arePartitionRecordsSorted;

  public HoodieDataSourceInternalWriter(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
                                        SparkSession sparkSession, Configuration configuration, Map<String, String> extraMetadata,
                                        boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.populateMetaFields = populateMetaFields;
    this.arePartitionRecordsSorted = arePartitionRecordsSorted;
    this.dataSourceInternalWriterHelper = new DataSourceInternalWriterHelper(instantTime, writeConfig, structType,
        sparkSession, configuration, extraMetadata);
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    dataSourceInternalWriterHelper.createInflightCommit();
    if (WriteOperationType.BULK_INSERT == dataSourceInternalWriterHelper.getWriteOperationType()) {
      return new HoodieBulkInsertDataInternalWriterFactory(dataSourceInternalWriterHelper.getHoodieTable(),
          writeConfig, instantTime, structType, populateMetaFields, arePartitionRecordsSorted);
    } else {
      throw new IllegalArgumentException("Write Operation Type + " + dataSourceInternalWriterHelper.getWriteOperationType() + " not supported ");
    }
  }

  @Override
  public boolean useCommitCoordinator() {
    return dataSourceInternalWriterHelper.useCommitCoordinator();
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    dataSourceInternalWriterHelper.onDataWriterCommit(message.toString());
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    List<HoodieWriteStat> writeStatList = Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
        .flatMap(m -> m.getWriteStatuses().stream().map(HoodieInternalWriteStatus::getStat)).collect(Collectors.toList());
    dataSourceInternalWriterHelper.commit(writeStatList);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    dataSourceInternalWriterHelper.abort();
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<HoodieWriteStat> writeStatList = Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
        .flatMap(m -> m.getWriteStatuses().stream().map(HoodieInternalWriteStatus::getStat)).collect(Collectors.toList());
    dataSourceInternalWriterHelper.commit(writeStatList);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    dataSourceInternalWriterHelper.abort();
  }

}
