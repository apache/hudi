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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Handle to concatenate new records to old records w/o any merging. If Operation is set to Inserts, and if {{@link HoodieWriteConfig#allowDuplicateInserts()}}
 * is set, this handle will be used instead of {@link HoodieMergeHandle}.
 *
 * Simplified Logic:
 * For every existing record
 *     Write the record as is
 * For all incoming records, write to file as is, without de-duplicating based on the record key.
 *
 * Illustration with simple data.
 * Incoming data:
 *     rec1_2, rec1_3, rec4_2, rec5_1, rec6_1
 * Existing data:
 *     rec1_1, rec2_1, rec3_1, rec4_1
 *
 * For every existing record, write to storage as is.
 *    => rec1_1, rec2_1, rec3_1 and rec4_1 is written to storage
 * Write all records from incoming set to storage
 *    => rec1_2, rec1_3, rec4_2, rec5_1 and rec6_1
 *
 * Final snapshot in storage
 * rec1_1, rec2_1, rec3_1, rec4_1, rec1_2, rec1_3, rec4_2, rec5_1, rec6_1
 *
 * Users should ensure there are no duplicates when "insert" operation is used and if the respective config is enabled. So, above scenario should not
 * happen and every batch should have new records to be inserted. Above example is for illustration purposes only.
 */
@NotThreadSafe
public class HoodieConcatHandle<T, I, K, O> extends HoodieMergeHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieConcatHandle.class);
  // a representation of incoming records that tolerates duplicate keys
  private final Iterator<HoodieRecord<T>> recordItr;

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                            TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, Collections.emptyIterator(), partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    this.recordItr = recordItr;
  }

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                            Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                            HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, Collections.emptyMap(), partitionPath, fileId, dataFileToBeMerged, taskContextSupplier,
        Option.empty());
    this.recordItr = keyToNewRecords.values().iterator();
  }

  /**
   * Write old record as is w/o merging with incoming record.
   */
  @Override
  public void write(HoodieRecord oldRecord) {
    Schema schema = useWriterSchemaForCompaction || withMetaFields ? tableSchemaWithMetaFields : tableSchema;
    String key = oldRecord.getRecordKey(schema, keyGeneratorOpt);
    try {
      // NOTE: We're enforcing preservation of the record metadata to keep existing semantic
      writeToFile(new HoodieKey(key, partitionPath), oldRecord, schema, config.getPayloadConfig().getProps(), true);
    } catch (IOException | RuntimeException e) {
      String errMsg = String.format("Failed to write old record into new file for key %s from old file %s to new file %s with writerSchema %s",
          key, getOldFilePath(), newFilePath, writeSchemaWithMetaFields.toString(true));
      LOG.debug("Old record is " + oldRecord);
      throw new HoodieUpsertException(errMsg, e);
    }
    recordsWritten++;
  }

  @Override
  protected void writeIncomingRecords() throws IOException {
    Schema schema = useWriterSchemaForCompaction ? tableSchemaWithMetaFields : tableSchema;
    while (recordItr.hasNext()) {
      HoodieRecord<T> record = recordItr.next();
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
      writeInsertRecord(record, schema);
    }
  }
}
