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

import org.apache.hudi.client.model.HoodieRowDataCreation;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.io.storage.row.lance.HoodieFlinkLanceArrowUtils;
import org.apache.hudi.io.storage.row.lance.LanceRowDataWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.function.Function;

/**
 * Lance writer for Flink {@link RowData} base files.
 */
public class HoodieRowDataLanceWriter extends HoodieBaseLanceWriter<RowData, String>
    implements HoodieRowDataFileWriter {

  private static final long MIN_RECORDS_FOR_SIZE_CHECK = 100L;
  private static final long MAX_RECORDS_FOR_SIZE_CHECK = 10000L;

  private final RowType rowType;
  private final Schema arrowSchema;
  private final String fileName;
  private final String instantTime;
  private final long maxFileSize;
  private final boolean utcTimestamp;
  private final boolean populateMetaFields;
  private final boolean withOperation;
  private final Function<Long, String> seqIdGenerator;
  private long recordCountForNextSizeCheck = MIN_RECORDS_FOR_SIZE_CHECK;

  public HoodieRowDataLanceWriter(
      StoragePath file,
      RowType rowType,
      String instantTime,
      TaskContextSupplier taskContextSupplier,
      Option<BloomFilter> bloomFilterOpt,
      long maxFileSize,
      long allocatorSize,
      long flushByteWatermark,
      boolean utcTimestamp,
      boolean populateMetaFields,
      boolean withOperation) {
    super(file, DEFAULT_BATCH_SIZE, allocatorSize, flushByteWatermark,
        bloomFilterOpt.map(HoodieBloomFilterRowDataWriteSupport::new));
    ValidationUtils.checkArgument(maxFileSize > 0, "maxFileSize must be a positive number");
    ValidationUtils.checkArgument(allocatorSize > 0, "allocatorSize must be a positive number");
    ValidationUtils.checkArgument(flushByteWatermark > 0, "flushByteWatermark must be a positive number");
    ValidationUtils.checkArgument(flushByteWatermark < allocatorSize,
        "flushByteWatermark (" + flushByteWatermark + ") must be less than allocatorSize ("
            + allocatorSize + ")");
    this.rowType = rowType;
    this.arrowSchema = HoodieFlinkLanceArrowUtils.toArrowSchema(rowType);
    this.fileName = file.getName();
    this.instantTime = instantTime;
    this.maxFileSize = maxFileSize;
    this.utcTimestamp = utcTimestamp;
    this.populateMetaFields = populateMetaFields;
    this.withOperation = withOperation;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  @Override
  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    if (writtenCount >= recordCountForNextSizeCheck) {
      long dataSize = getDataSize();
      long avgRecordSize = Math.max(dataSize / writtenCount, 1);
      if (dataSize > (maxFileSize - avgRecordSize * 2)) {
        return false;
      }
      recordCountForNextSizeCheck = writtenCount + Math.min(
          Math.max(MIN_RECORDS_FOR_SIZE_CHECK, (maxFileSize / avgRecordSize - writtenCount) / 2),
          MAX_RECORDS_FOR_SIZE_CHECK);
    }
    return true;
  }

  @Override
  public void writeRow(String key, RowData row) throws IOException {
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport -> bloomFilterWriteSupport.addKey(key));
    super.write(row);
  }

  @Override
  public void writeRowWithMetaData(HoodieKey key, RowData row) throws IOException {
    if (populateMetaFields) {
      RowData rowWithMeta = updateRecordMetadata(row, key, getWrittenRecordCount());
      writeRow(key.getRecordKey(), rowWithMeta);
    } else {
      writeRow(key.getRecordKey(), row);
    }
  }

  @Override
  protected ArrowWriter<RowData> createArrowWriter(VectorSchemaRoot root) {
    return new RowDataArrowWriter(root);
  }

  @Override
  protected Schema getArrowSchema() {
    return arrowSchema;
  }

  private class RowDataArrowWriter implements ArrowWriter<RowData> {
    private final VectorSchemaRoot root;
    private final LanceRowDataWriter writer;
    private int rowId;

    private RowDataArrowWriter(VectorSchemaRoot root) {
      this.root = root;
      this.writer = new LanceRowDataWriter(rowType, root.getFieldVectors(), utcTimestamp);
    }

    @Override
    public void write(RowData row) {
      writer.write(row, rowId++);
    }

    @Override
    public void finishBatch() {
      root.getFieldVectors().forEach(vector -> vector.setValueCount(rowId));
      root.setRowCount(rowId);
    }

    @Override
    public void reset() {
      rowId = 0;
    }
  }

  private RowData updateRecordMetadata(RowData row, HoodieKey key, long recordCount) {
    return HoodieRowDataCreation.create(instantTime, seqIdGenerator.apply(recordCount),
        key.getRecordKey(), key.getPartitionPath(), fileName, row, withOperation, true);
  }
}
