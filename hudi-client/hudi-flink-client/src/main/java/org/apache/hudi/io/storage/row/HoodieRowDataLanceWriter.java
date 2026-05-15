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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Lance writer for Flink {@link RowData} append-only base files.
 */
public class HoodieRowDataLanceWriter extends HoodieBaseLanceWriter<RowData, String>
    implements HoodieRowDataFileWriter {

  private static final long MIN_RECORDS_FOR_SIZE_CHECK = 100L;
  private static final long MAX_RECORDS_FOR_SIZE_CHECK = 10000L;

  private final RowType rowType;
  private final Schema arrowSchema;
  private final long maxFileSize;
  private long recordCountForNextSizeCheck = MIN_RECORDS_FOR_SIZE_CHECK;

  public HoodieRowDataLanceWriter(
      StoragePath file,
      RowType rowType,
      TaskContextSupplier taskContextSupplier,
      Option<BloomFilter> bloomFilterOpt,
      long maxFileSize,
      long allocatorSize,
      long flushByteWatermark) {
    super(file, DEFAULT_BATCH_SIZE, allocatorSize, flushByteWatermark,
        bloomFilterOpt.map(HoodieBloomFilterStringWriteSupport::new));
    ValidationUtils.checkArgument(maxFileSize > 0, "maxFileSize must be a positive number");
    ValidationUtils.checkArgument(allocatorSize > 0, "allocatorSize must be a positive number");
    ValidationUtils.checkArgument(flushByteWatermark > 0, "flushByteWatermark must be a positive number");
    ValidationUtils.checkArgument(flushByteWatermark < allocatorSize,
        "flushByteWatermark (" + flushByteWatermark + ") must be less than allocatorSize ("
            + allocatorSize + ")");
    this.rowType = rowType;
    this.arrowSchema = HoodieFlinkLanceArrowUtils.toArrowSchema(rowType);
    this.maxFileSize = maxFileSize;
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
    writeRow(key.getRecordKey(), row);
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
    private int rowId;

    private RowDataArrowWriter(VectorSchemaRoot root) {
      this.root = root;
    }

    @Override
    public void write(RowData row) {
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        HoodieFlinkLanceArrowUtils.writeValue(rowType.getTypeAt(i), root.getVector(i), rowId, row, i);
      }
      rowId++;
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

  @Override
  public void writeWithMetadata(HoodieKey key, HoodieRecord record, org.apache.hudi.common.schema.HoodieSchema schema,
                                java.util.Properties props) throws IOException {
    writeRowWithMetaData(key, (RowData) record.getData());
  }
}
