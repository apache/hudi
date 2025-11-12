/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * A {@link FlinkIncrementalMergeHandle} that supports CONCAT write incrementally(small data buffers).
 *
 * <P>The records iterator for super constructor is reset as empty thus the initialization for new records
 * does nothing. This handle keep the iterator for itself to override the write behavior.
 */
public class FlinkIncrementalConcatHandle<T, I, K, O>
    extends FlinkIncrementalMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkIncrementalConcatHandle.class);

  // a representation of incoming records that tolerates duplicate keys
  private final Iterator<HoodieRecord<T>> recordItr;

  public FlinkIncrementalConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                      Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                      TaskContextSupplier taskContextSupplier, StoragePath basePath) {
    super(config, instantTime, hoodieTable, Collections.emptyIterator(), partitionPath, fileId, taskContextSupplier, basePath);
    this.recordItr = recordItr;
  }

  /**
   * Write old record as is w/o merging with incoming record.
   */
  @Override
  public void write(HoodieRecord oldRecord) {
    Schema oldSchema = config.populateMetaFields() ? writeSchemaWithMetaFields : writeSchema;
    String key = oldRecord.getRecordKey(oldSchema, keyGeneratorOpt);
    try {
      fileWriter.write(key, oldRecord, writeSchema);
    } catch (IOException | RuntimeException e) {
      String errMsg = String.format(
          "Failed to write old record into new file for key %s from old file %s to new file %s with writerSchema %s",
          key, getOldFilePath(), newFilePath, writeSchemaWithMetaFields.toString(true));
      LOG.debug("Old record is {}", oldRecord);
      throw new HoodieUpsertException(errMsg, e);
    }
    recordsWritten++;
  }

  @Override
  protected void writeIncomingRecords() throws IOException {
    while (recordItr.hasNext()) {
      HoodieRecord<T> record = recordItr.next();
      writeInsertRecord(record);
    }
  }
}
