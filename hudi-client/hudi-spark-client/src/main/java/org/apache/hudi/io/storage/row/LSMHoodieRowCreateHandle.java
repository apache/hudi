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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

/**
 * Create handle with InternalRow for datasource implementation of bulk insert.
 */
public class LSMHoodieRowCreateHandle extends HoodieRowCreateHandle {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LogManager.getLogger(LSMHoodieRowCreateHandle.class);
  private final HoodieRecordMerger recordMerger;
  private final TypedProperties payloadProperties;
  private final Schema writeSchema;
  private UTF8String lastRecordKey = null;
  private HoodieSparkRecord lastMergedSparkRecord = null;

  public LSMHoodieRowCreateHandle(HoodieTable table,
                                  HoodieWriteConfig writeConfig,
                                  String partitionPath,
                                  String fileId,
                                  String instantTime,
                                  int taskPartitionId,
                                  long taskId,
                                  long taskEpochId,
                                  StructType structType,
                                  boolean shouldPreserveHoodieMetadata,
                                  int columnFamilyNumber,
                                  int levelNumber) {
    super(table, writeConfig, partitionPath, fileId, instantTime, taskPartitionId, taskId, 
        taskEpochId, structType, shouldPreserveHoodieMetadata, String.valueOf(columnFamilyNumber), String.valueOf(levelNumber));
    Schema oriSchema = new Schema.Parser().parse(writeConfig.getWriteSchema());
    this.writeSchema = AvroSchemaCache.intern(AvroConversionUtils.convertStructTypeToAvroSchema(structType,
        oriSchema.getName(), oriSchema.getNamespace()));
    this.recordMerger = writeConfig.getRecordMerger();
    this.payloadProperties = writeConfig.getPayloadConfig().getProps();
  }

  @Override
  public String makeFileName(String instantTime, String writeToken, String version, String levelNumber) {
    return FSUtils.makeLSMFileNameWithSuffix(fileId, instantTime, version, levelNumber,
        writeToken, table.getBaseFileExtension());
  }

  @Override
  protected UTF8String getFileNameMetaValue(String fileName) {
    return UTF8String.fromString(fileName.replace(FSUtils.LSM_TEMP_FILE_SUFFIX, ""));
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public void write(InternalRow row) throws IOException {
    try {
      if (writeConfig.getOperationType().equals(WriteOperationType.CLUSTER.name())) {
        if (populateMetaFields) {
          writeRow(row, row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD));
        } else {
          writeRowNoMetaFields(row);
        }
        return;
      }
      UTF8String recordKey = row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
      // 复制当前行，因为row可能是可重用的
      InternalRow currentRow = copyRow(row);
      HoodieSparkRecord currentRecord = new HoodieSparkRecord(currentRow, structType);

      // 检查是否与上一条记录的key相同
      if (lastRecordKey != null && lastRecordKey.equals(recordKey)) {
        // 相同key，合并记录
        Option<Pair<HoodieRecord, Schema>> res = recordMerger.merge(lastMergedSparkRecord,
            writeSchema, currentRecord, writeSchema, payloadProperties);
        if (res.isPresent()) {
          lastMergedSparkRecord = (HoodieSparkRecord)res.get().getLeft();
        } else {
          lastMergedSparkRecord = null;
        }
      } else {
        // 不同key，先写出上一条合并后的记录(如果存在)
        if (lastMergedSparkRecord != null) {
          if (populateMetaFields) {
            writeRow(lastMergedSparkRecord.getData(), lastRecordKey);
          } else {
            writeRowNoMetaFields(lastMergedSparkRecord.getData());
          }
        }

        // 更新为当前记录
        lastRecordKey = recordKey.clone(); // 克隆以确保引用安全
        lastMergedSparkRecord = currentRecord;
      }
    } catch (Exception e) {
      writeStatus.setGlobalError(e);
      throw e;
    }
  }

  // 辅助方法：复制行数据
  private InternalRow copyRow(InternalRow row) {
    // 根据具体实现选择合适的复制方法
    // 例如，可以使用GenericInternalRow或UnsafeRow的复制方法
    return row.copy(); // 假设InternalRow有copy方法
  }

  /**
   * when use spark write lsm, seqId just generate according to GLOBAL_SEQ_NO
   * @param row
   * @param recordKey
   */
  @Override
  public void writeRow(InternalRow row, UTF8String recordKey) {
    try {
      if (recordKey == null) {
        recordKey = row.getUTF8String(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
      }
      UTF8String partitionPath = row.getUTF8String(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
      // This is the only meta-field that is generated dynamically, hence conversion b/w
      // [[String]] and [[UTF8String]] is unavoidable if preserveHoodieMetadata is false
      UTF8String seqId = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD) :
          UTF8String.fromString(String.valueOf(GLOBAL_SEQ_NO.getAndIncrement()));
      UTF8String writeCommitTime = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD)
          : commitTime;
      InternalRow updatedRow = new HoodieInternalRow(writeCommitTime, seqId, recordKey,
          partitionPath, finalFilename, row, true);
      try {
        fileWriter.writeRow(recordKey, updatedRow);
        // NOTE: To avoid conversion on the hot-path we only convert [[UTF8String]] into [[String]]
        //       in cases when successful records' writes are being tracked
        writeStatus.markSuccess(writeStatus.isTrackingSuccessfulWrites() ? recordKey.toString() : null);
      } catch (Exception t) {
        writeStatus.markFailure(recordKey.toString(), t);
      }
    } catch (Exception e) {
      writeStatus.setGlobalError(e);
      throw e;
    }
  }

  private void flush() {
    if (lastMergedSparkRecord != null) {
      if (populateMetaFields) {
        writeRow(lastMergedSparkRecord.getData(), lastRecordKey);
      } else {
        writeRowNoMetaFields(lastMergedSparkRecord.getData());
      }
      lastMergedSparkRecord = null;
      lastRecordKey = null;
    }
  }

  @Override
  public HoodieInternalWriteStatus close() throws IOException {
    flush();
    return super.close();
  }
}
