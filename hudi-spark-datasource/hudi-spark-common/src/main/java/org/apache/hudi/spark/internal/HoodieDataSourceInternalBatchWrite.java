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

package org.apache.hudi.spark.internal;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.DataSourceInternalWriterHelper;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link BatchWrite} for datasource "hudi.spark.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalBatchWrite implements BatchWrite {

  private final String instantTime;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final boolean arePartitionRecordsSorted;
  private final boolean populateMetaFields;
  private final DataSourceInternalWriterHelper dataSourceInternalWriterHelper;
  private Map<String, String> extraMetadata = new HashMap<>();

  public HoodieDataSourceInternalBatchWrite(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
                                            SparkSession jss, StorageConfiguration<?> storageConf, Map<String, String> properties, boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.populateMetaFields = populateMetaFields;
    this.arePartitionRecordsSorted = arePartitionRecordsSorted;
    this.extraMetadata = DataSourceUtils.getExtraMetadata(properties);
    this.dataSourceInternalWriterHelper = new DataSourceInternalWriterHelper(instantTime, writeConfig, structType,
        jss, storageConf, extraMetadata);
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
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
  public void commit(WriterCommitMessage[] messages) {
    List<WriteStatus> writeStatuses = Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
        .flatMap(m -> m.getWriteStatuses().stream()).collect(Collectors.toList());
    dataSourceInternalWriterHelper.commit(writeStatuses);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    dataSourceInternalWriterHelper.abort();
  }
}
