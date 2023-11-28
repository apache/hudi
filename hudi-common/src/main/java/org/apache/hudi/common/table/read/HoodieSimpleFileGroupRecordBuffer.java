/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;

/**
 * Used when the filegroup doesn't have any log files
 */
public class HoodieSimpleFileGroupRecordBuffer<T> extends HoodieBaseFileGroupRecordBuffer<T> {
  public HoodieSimpleFileGroupRecordBuffer(HoodieReaderContext<T> readerContext, Schema readerSchema, Schema baseFileSchema,
                                           Option<String> partitionNameOverrideOpt, Option<String[]> partitionPathFieldOpt, HoodieRecordMerger recordMerger,
                                           TypedProperties payloadProps) {
    super(readerContext, readerSchema, baseFileSchema, partitionNameOverrideOpt, partitionPathFieldOpt, recordMerger, payloadProps);
  }

  @Override
  protected boolean doHasNext() throws IOException {
    if (baseFileIterator.hasNext()) {
      nextRecord = readerContext.seal(baseFileIterator.next());
      return true;
    }
    return false;
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.SIMPLE;
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    throw new IllegalStateException("processDataBlock should never be called from HoodieSimpleFileGroupRecordBuffer");
  }

  @Override
  public void processNextDataRecord(T record, Map<String, Object> metadata, Object index) throws IOException {
    throw new IllegalStateException("processNextDataRecord should never be called from HoodieSimpleFileGroupRecordBuffer");
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    throw new IllegalStateException("processDeleteBlock should never be called from HoodieSimpleFileGroupRecordBuffer");

  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Object index) {
    throw new IllegalStateException("processNextDeletedRecord should never be called from HoodieSimpleFileGroupRecordBuffer");

  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    throw new IllegalStateException("containsLogRecord should never be called from HoodieSimpleFileGroupRecordBuffer");
  }
}
