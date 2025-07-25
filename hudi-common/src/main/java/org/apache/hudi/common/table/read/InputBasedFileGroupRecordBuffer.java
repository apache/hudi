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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * This is a special record buffer that caches input records,
 * which is used to merge with records from base file.
 */
public class InputBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T> {
  private final Iterator<HoodieRecord<T>> inputRecordIterator;

  public InputBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                         HoodieTableMetaClient hoodieTableMetaClient,
                                         RecordMergeMode recordMergeMode,
                                         PartialUpdateMode partialUpdateMode,
                                         TypedProperties props,
                                         Option<String> orderingFieldName,
                                         Iterator<HoodieRecord<T>> inputRecordIterator,
                                         UpdateProcessor<T> updateProcessor) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, orderingFieldName, updateProcessor);
    this.inputRecordIterator = inputRecordIterator;

    // Read input records into the buffer.
    populateRecordBuffer();
  }

  private void populateRecordBuffer() {
    if (null == inputRecordIterator) {
      throw new IllegalArgumentException("InputRecordIterator can not be null");
    }

    while (inputRecordIterator.hasNext()) {
      HoodieRecord<T> hoodieRecord = inputRecordIterator.next();
      BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(
          hoodieRecord,
          readerContext.getSchemaHandler().tableSchema,
          readerContext,
          orderingFieldName,
          props);
      records.put(hoodieRecord.getRecordKey(), bufferedRecord);
    }
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    throw new HoodieNotSupportedException("Method 'processDataBlock' is not supported");
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    throw new HoodieNotSupportedException("Method 'processNextDataRecord' is not supported");
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    throw new HoodieNotSupportedException("Method 'processDeleteBlock' is not supported");
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordKey) {
    throw new HoodieNotSupportedException("Method 'processNextDeletedRecord' is not supported");
  }
}
