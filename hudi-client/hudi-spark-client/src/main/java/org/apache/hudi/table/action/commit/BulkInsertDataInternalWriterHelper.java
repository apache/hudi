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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.JavaScalaConverters;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Helper class for HoodieBulkInsertDataInternalWriter used by Spark datasource v2.
 */
@Slf4j
public class BulkInsertDataInternalWriterHelper {

  protected final String instantTime;
  protected final int taskPartitionId;
  protected final long taskId;
  protected final long taskEpochId;
  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final StructType structType;
  protected final Boolean arePartitionRecordsSorted;
  protected final List<WriteStatus> writeStatusList = new ArrayList<>();
  protected final String fileIdPrefix;
  protected final Map<String, HoodieRowCreateHandle> handles = new HashMap<>();
  protected final boolean populateMetaFields;
  protected final boolean shouldPreserveHoodieMetadata;
  protected final Option<BuiltinKeyGenerator> keyGeneratorOpt;
  protected final boolean simpleKeyGen;
  protected final int simplePartitionFieldIndex;
  protected final DataType simplePartitionFieldDataType;
  /**
   * NOTE: This is stored as Catalyst's internal {@link UTF8String} to avoid
   *       conversion (deserialization) b/w {@link UTF8String} and {@link String}
   */
  protected UTF8String lastKnownPartitionPath = null;
  protected HoodieRowCreateHandle handle;
  protected int numFilesWritten = 0;

  public BulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                            boolean populateMetaFields, boolean arePartitionRecordsSorted) {
    this(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType,
        populateMetaFields, arePartitionRecordsSorted, false);
  }

  public BulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                            boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.structType = structType;
    this.populateMetaFields = populateMetaFields;
    this.shouldPreserveHoodieMetadata = shouldPreserveHoodieMetadata;
    this.arePartitionRecordsSorted = arePartitionRecordsSorted;
    this.fileIdPrefix = UUID.randomUUID().toString();

    if (!populateMetaFields) {
      this.keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.getKeyGenerator(writeConfig.getProps());
    } else {
      this.keyGeneratorOpt = Option.empty();
    }

    if (keyGeneratorOpt.isPresent() && keyGeneratorOpt.get() instanceof SimpleKeyGenerator) {
      this.simpleKeyGen = true;
      this.simplePartitionFieldIndex = (Integer) structType.getFieldIndex(keyGeneratorOpt.get().getPartitionPathFields().get(0)).get();
      this.simplePartitionFieldDataType = structType.fields()[simplePartitionFieldIndex].dataType();
    } else {
      this.simpleKeyGen = false;
      this.simplePartitionFieldIndex = -1;
      this.simplePartitionFieldDataType = null;
    }
  }

  public void write(InternalRow row) throws IOException {
    try {
      UTF8String partitionPath = extractPartitionPath(row);
      if (lastKnownPartitionPath == null || !Objects.equals(lastKnownPartitionPath, partitionPath) || !handle.canWrite()) {
        handle = getRowCreateHandle(partitionPath.toString());
        // NOTE: It's crucial to make a copy here, since [[UTF8String]] could be pointing into
        //       a mutable underlying buffer
        lastKnownPartitionPath = partitionPath.clone();
      }

      boolean shouldDropPartitionColumns = writeConfig.shouldDropPartitionColumns();
      if (shouldDropPartitionColumns) {
        // Drop the partition columns from the row
        // Using the deprecated JavaConversions to be compatible with scala versions < 2.12. Once hudi support for scala versions < 2.12 is
        // stopped, can move this to JavaConverters.seqAsJavaList(...)
        List<String> partitionCols = JavaScalaConverters.convertScalaListToJavaList(HoodieDatasetBulkInsertHelper.getPartitionPathCols(this.writeConfig));
        Set<Integer> partitionIdx = new HashSet<Integer>();
        for (String col : partitionCols) {
          partitionIdx.add(this.structType.fieldIndex(col));
        }

        // Relies on InternalRow::toSeq(...) preserving the column ordering based on the supplied schema
        // Using the deprecated JavaConversions to be compatible with scala versions < 2.12.
        List<Object> cols = JavaScalaConverters.convertScalaListToJavaList(row.toSeq(structType));
        int idx = 0;
        List<Object> newCols = new ArrayList<Object>();
        for (Object o : cols) {
          if (!partitionIdx.contains(idx)) {
            newCols.add(o);
          }
          idx += 1;
        }
        InternalRow newRow = InternalRow.fromSeq(JavaScalaConverters.<Object>convertJavaListToScalaSeq(newCols));
        handle.write(newRow);
      } else {
        handle.write(row);
      }
    } catch (Throwable t) {
      log.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw t;
    }
  }

  public List<WriteStatus> getWriteStatuses() throws IOException {
    close();
    return writeStatusList;
  }

  public void abort() {
  }

  public void close() throws IOException {
    for (HoodieRowCreateHandle rowCreateHandle : handles.values()) {
      log.info("Closing bulk insert file {}", rowCreateHandle.getFileName());
      writeStatusList.add(rowCreateHandle.close());
    }
    handles.clear();
    handle = null;
  }

  protected UTF8String extractPartitionPath(InternalRow row) {
    if (populateMetaFields) {
      // In case meta-fields are materialized w/in the table itself, we can just simply extract
      // partition path from there
      //
      // NOTE: Helper keeps track of [[lastKnownPartitionPath]] as [[UTF8String]] to avoid
      //       conversion from Catalyst internal representation into a [[String]]
      return row.getUTF8String(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
    } else if (keyGeneratorOpt.isPresent()) {
      return keyGeneratorOpt.get().getPartitionPath(row, structType);
    } else {
      return UTF8String.EMPTY_UTF8;
    }
  }

  private HoodieRowCreateHandle getRowCreateHandle(String partitionPath) throws IOException {
    if (!handles.containsKey(partitionPath)) { // if there is no handle corresponding to the partition path
      // if records are sorted, we can close all existing handles
      if (arePartitionRecordsSorted) {
        close();
      }

      log.info("Creating new file for partition path {}", partitionPath);
      HoodieRowCreateHandle rowCreateHandle = createHandle(partitionPath);
      handles.put(partitionPath, rowCreateHandle);
    } else if (!handles.get(partitionPath).canWrite()) {
      // even if there is a handle to the partition path, it could have reached its max size threshold. So, we close the handle here and
      // create a new one.
      log.info("Rolling max-size file for partition path {}", partitionPath);
      writeStatusList.add(handles.remove(partitionPath).close());
      HoodieRowCreateHandle rowCreateHandle = createHandle(partitionPath);
      handles.put(partitionPath, rowCreateHandle);
    }
    return handles.get(partitionPath);
  }

  private HoodieRowCreateHandle createHandle(String partitionPath) {
    return new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
        instantTime, taskPartitionId, taskId, taskEpochId, structType, shouldPreserveHoodieMetadata);
  }

  protected String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }
}
