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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieRowCreateHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Hoodie's Implementation of {@link DataWriter<InternalRow>}. This is used in data source implementation for bulk insert.
 */
public class HoodieBulkInsertDataInternalWriter implements DataWriter<InternalRow> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieBulkInsertDataInternalWriter.class);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final List<HoodieInternalWriteStatus> writeStatusList = new ArrayList<>();

  private HoodieRowCreateHandle handle;
  private String lastKnownPartitionPath = null;
  private String fileIdPrefix = null;
  private int numFilesWritten = 0;

  public HoodieBulkInsertDataInternalWriter(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
      String instantTime, int taskPartitionId, long taskId, long taskEpochId,
      StructType structType) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.structType = structType;
    this.fileIdPrefix = UUID.randomUUID().toString();
  }

  @Override
  public void write(InternalRow record) throws IOException {
    try {
      String partitionPath = record.getUTF8String(
          HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();

      if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !handle.canWrite()) {
        LOG.info("Creating new file for partition path " + partitionPath);
        createNewHandle(partitionPath);
        lastKnownPartitionPath = partitionPath;
      }
      handle.write(record);
    } catch (Throwable t) {
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw t;
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    close();
    return new HoodieWriterCommitMessage(writeStatusList);
  }

  @Override
  public void abort() throws IOException {
  }

  private void createNewHandle(String partitionPath) throws IOException {
    if (null != handle) {
      close();
    }
    handle = new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
        instantTime, taskPartitionId, taskId, taskEpochId, structType);
  }

  public void close() throws IOException {
    if (null != handle) {
      writeStatusList.add(handle.close());
    }
  }

  protected String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }
}
