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

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class HoodieRowUpdateHandle implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieRowUpdateHandle.class);
  private static final AtomicLong SEQGEN = new AtomicLong(1);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;
  private final HoodieInternalRowFileWriter fileWriter;
  private final String partitionPath;
  private final Path path;
  private final String fileId;
  private final StructType structType;
  private final String prevFileName;
  private final int shouldUpdateFieldIndex;
  private final Set<Integer> updateNullFieldIndexSet;
  Map<Integer, Pair<Integer, DataType>> updateValueFieldMap;
  private final FileSystem fs;
  private final HoodieInternalWriteStatus writeStatus;
  private final HoodieTimer currTimer;

  private boolean isFileUpdated = false;
  private long writeTimeAccumulator = 0;
  private long updateRecordsWritten = 0;

  private Long greatestSequenceNumber = -1L;

  public HoodieRowUpdateHandle(
      HoodieTable table,
      HoodieWriteConfig writeConfig,
      String partitionPath,
      String fileId,
      String instantTime,
      int taskPartitionId,
      long taskId,
      long taskEpochId,
      StructType structType,
      String prevFileName,
      int shouldUpdateFieldIndex,
      Set<Integer> updateNullFieldIndexSet,
      Map<Integer, Pair<Integer, DataType>> updateValueFieldMap) {
    this.partitionPath = partitionPath;
    this.table = table;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.fileId = fileId;
    this.currTimer = new HoodieTimer();
    this.currTimer.startTimer();
    this.fs = table.getMetaClient().getFs();
    this.path = makeNewPath(partitionPath);
    this.writeStatus = new HoodieInternalWriteStatus(!table.getIndex().isImplicitWithStorage(),
        writeConfig.getWriteStatusFailureFraction());
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setFileId(fileId);
    this.structType = structType;
    this.prevFileName = prevFileName;
    this.shouldUpdateFieldIndex = shouldUpdateFieldIndex;
    this.updateNullFieldIndexSet = updateNullFieldIndexSet;
    this.updateValueFieldMap = updateValueFieldMap;
    this.isFileUpdated = false;
    try {
      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              fs,
              instantTime,
              new Path(writeConfig.getBasePath()),
              FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath));
      partitionMetadata.trySave(taskPartitionId);
      createMarkerFile(partitionPath, FSUtils.makeDataFileName(this.instantTime, getWriteToken(), this.fileId, table.getBaseFileExtension()));
      this.fileWriter = createNewFileWriter(path, table, writeConfig, structType);
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }
    LOG.info("New handle created for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Writes an {@link InternalRow} to the underlying HoodieInternalRowFileWriter. Before writing, value for meta columns are computed as required
   * and wrapped in {@link HoodieInternalRow}. {@link HoodieInternalRow} is what gets written to HoodieInternalRowFileWriter.
   *
   * @param record instance of {@link InternalRow} that needs to be written to the fileWriter.
   * @throws IOException
   */
  public void write(InternalRow record) throws IOException {
    long start = System.currentTimeMillis();
    try {
      String partitionPath = record.getUTF8String(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(
          HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();
      String seqId = HoodieRecord.generateSequenceId(instantTime, taskPartitionId, SEQGEN.getAndIncrement());
      long sequenceNumber = Long.parseLong(seqId.split("_")[2]);
      if (sequenceNumber < greatestSequenceNumber) {
        throw new RuntimeException("Wrong sequence number, records should be sorted");
      } else {
        greatestSequenceNumber = sequenceNumber;
      }
      Integer recordKeyPos = HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS
          .get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      if (!record.isNullAt(shouldUpdateFieldIndex)) {
        isFileUpdated = true;
        updateRecordsWritten++;
        for (Integer updateNullFieldIndex : updateNullFieldIndexSet) {
          record.setNullAt(updateNullFieldIndex);
        }
        for (Entry<Integer, Pair<Integer, DataType>> entry : updateValueFieldMap.entrySet()) {
          record.update(entry.getKey(), record.get(entry.getValue().getKey(), entry.getValue().getValue()));
        }
      }
      String recordKey = null;
      if (!record.isNullAt(recordKeyPos)) {
        recordKey = record.getUTF8String(recordKeyPos).toString();
      }

      HoodieInternalRow internalRow = new HoodieInternalRow(instantTime, seqId, recordKey, partitionPath, path.getName(),
          new InternalRowWrapper(structType.length(), record));
      try {
        fileWriter.writeRow(recordKey, internalRow);
        writeStatus.markSuccess(recordKey);
      } catch (Throwable t) {
        writeStatus.markFailure(recordKey, t);
      }
    } catch (Throwable ge) {
      writeStatus.setGlobalError(ge);
      throw ge;
    }
    writeTimeAccumulator += (System.currentTimeMillis() - start);
  }

  public HoodieInternalWriteStatus close() throws IOException {
    long start = System.currentTimeMillis();
    fileWriter.close();
    if (isFileUpdated) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(partitionPath);
      stat.setNumWrites(writeStatus.getTotalRecords());
      stat.setNumDeletes(0);
      stat.setNumInserts(0);
      stat.setNumUpdateWrites(updateRecordsWritten);
      stat.setPrevCommit(prevFileName);
      stat.setFileId(fileId);
      stat.setPath(new Path(writeConfig.getBasePath()), path);
      long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), path);
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      long processTime = currTimer.endTimer();
      long writeTime = (System.currentTimeMillis() - start + writeTimeAccumulator) / 1000;
      LOG.info("Finish updating file " + getFileName() + " of partition " + partitionPath
          + ", takes " + processTime / 1000 + " seconds to process and " + writeTime
          + " seconds to write.");
      LOG.info("Finish updating file " + getFileName() + " of partition " + partitionPath
          + ", takes " + processTime / 1000 + " seconds to process and "
          + (System.currentTimeMillis() - start) / 1000 + " seconds to write.");
      runtimeStats.setTotalCreateTime(processTime);
      stat.setRuntimeStats(runtimeStats);
      writeStatus.setStat(stat);
      return writeStatus;
    } else {
      try {
        fs.delete(path, false);
      } catch (IOException e) {
        LOG.warn("Failed to delete unnecessary file " + path.toString());
      }
      long writeTime = (System.currentTimeMillis() - start + writeTimeAccumulator) / 1000;
      LOG.info("Skip updating file " + getFileName() + " of partition " + partitionPath
          + ", takes " + currTimer.endTimer() / 1000 + " seconds to process and "
          + writeTime + " seconds to write and delete.");
      return null;
    }
  }

  public boolean isFileUpdated() {
    return isFileUpdated;
  }

  public String getFileName() {
    return path.getName();
  }

  private Path makeNewPath(String partitionPath) {
    Path path = FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, getWriteToken(), fileId,
        tableConfig.getBaseFileFormat().getFileExtension()));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  private void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime)
        .create(partitionPath, dataFileName, IOType.CREATE);
  }

  private String getWriteToken() {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

  private HoodieInternalRowFileWriter createNewFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, StructType schema)
      throws IOException {
    return HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(
        path, hoodieTable, config, schema);
  }

  private static final class InternalRowWrapper extends InternalRow {

    private final int numFields;
    private final InternalRow internalRow;

    private InternalRowWrapper(int numFields, InternalRow internalRow) {
      this.numFields = numFields;
      this.internalRow = internalRow;
    }

    @Override
    public int numFields() {
      return numFields;
    }

    @Override
    public void setNullAt(int i) {
      validateIndex(i);
      internalRow.setNullAt(i);
    }

    @Override
    public void update(int i, Object value) {
      validateIndex(i);
      internalRow.update(i, value);
    }

    @Override
    public InternalRow copy() {
      InternalRow rowCopy = internalRow.copy();
      return new InternalRowWrapper(numFields, rowCopy);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      validateIndex(ordinal);
      return internalRow.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getLong(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getDouble(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      validateIndex(ordinal);
      return internalRow.getDecimal(ordinal, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getUTF8String(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getBinary(ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getInterval(ordinal);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      validateIndex(ordinal);
      return internalRow.getStruct(ordinal, numFields);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getArray(ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      validateIndex(ordinal);
      return internalRow.getMap(ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      validateIndex(ordinal);
      return internalRow.get(ordinal, dataType);
    }

    private void validateIndex(int oridinal) {
      if (oridinal >= numFields) {
        throw new IllegalArgumentException("Ordinal " + oridinal + " must less than num fields " + numFields);
      }
    }
  }
}
