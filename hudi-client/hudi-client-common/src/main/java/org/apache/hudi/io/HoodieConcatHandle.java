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

import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.FormatMetaColumnHandler;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Handle to concatenate new records to old records w/o any merging. If Operation is set to Inserts, and if {{@link HoodieWriteConfig#allowDuplicateInserts()}}
 * is set, this handle will be used instead of {@link HoodieWriteMergeHandle}.
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
@Slf4j
public class HoodieConcatHandle<T, I, K, O> extends HoodieWriteMergeHandle<T, I, K, O> {
  // a representation of incoming records that tolerates duplicate keys
  private final Iterator<HoodieRecord<T>> recordItr;
  // Resolves the format meta columns of the records carried over from the base file, created for the first of them
  private FormatMetaColumnHandler<T> formatMetaColumnHandler;

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            MergeContext<T> mergeContext, String partitionPath, String fileId,
                            TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    // The parent must not consume the incoming records into keyToNewRecords: this handle
    // concatenates them after the existing records instead of merging by key. Hand the parent
    // an empty iterator (keeping the update count) and keep the real iterator locally.
    super(config, instantTime, hoodieTable, MergeContext.create(mergeContext.getNumUpdates(), Collections.emptyIterator()), partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    this.recordItr = mergeContext.getRecordIterator();
    checkFormatMetaColumns();
  }

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                            Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                            HoodieBaseFile baseFile, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, Collections.emptyMap(), partitionPath, fileId, baseFile, taskContextSupplier, Option.empty());
    this.recordItr = keyToNewRecords.values().iterator();
    checkFormatMetaColumns();
  }

  @Override
  protected List<FormatMetaColumn> getFormatMetaColumns() {
    return getTableFormatMetaColumns();
  }

  private void checkFormatMetaColumns() {
    if (!formatMetaColumns.isEmpty()) {
      // The values of the format meta columns of a record are resolved from its position in the base file, so every
      // record of the base file has to be read in the order and with the schema of the file. The new records of the
      // compaction path keep their metadata and do not get the values on write of the format meta columns.
      ValidationUtils.checkArgument(!preserveMetadata && !schemaOnReadEnabled && !baseFileToMerge.getBootstrapBaseFile().isPresent(),
          "Format meta columns cannot be carried over on the compaction path, from a bootstrap base file or with schema evolution on read");
    }
  }

  /**
   * Write old record as is w/o merging with incoming record.
   */
  @Override
  public void write(HoodieRecord oldRecord) {
    HoodieSchema oldSchema = metaFieldsMode == MetaFieldsMode.NONE ? writeSchema : writeSchemaWithMetaFields;
    String key = getRecordKey(oldRecord, oldSchema);
    try {
      if (!formatMetaColumns.isEmpty()) {
        resolveFormatMetaColumns(oldRecord);
      }
      // NOTE: We're enforcing preservation of the record metadata to keep existing semantic
      writeToFile(new HoodieKey(key, partitionPath), oldRecord, oldSchema, config.getPayloadConfig().getProps(), true);
    } catch (IOException | RuntimeException e) {
      String errMsg = String.format(
          "Failed to write old record into new file for key %s from old file %s to new file %s with writerSchema %s",
          key, getOldFilePath(), newFilePath, writeSchemaWithMetaFields.toString(true));
      log.debug("Old record is {}", oldRecord);
      throw new HoodieUpsertException(errMsg, e);
    }
    recordsWritten++;
  }

  /**
   * Resolves the format meta columns of the record, which is the next record of the base file and has the schema
   * {@link #writeSchemaWithMetaFields}.
   */
  private void resolveFormatMetaColumns(HoodieRecord<T> oldRecord) {
    if (formatMetaColumnHandler == null) {
      // Not created in the constructor, as the reader context of the records may be set after it
      formatMetaColumnHandler = createFormatMetaColumnHandler().get();
    }
    T data = oldRecord.getData();
    ValidationUtils.checkState(formatMetaColumnHandler.resolveNextBaseFileRecord(data, writeSchemaWithMetaFields) == data,
        "The format meta columns of a record have to be updated in place");
  }

  @Override
  protected void writeIncomingRecords() throws IOException {
    while (recordItr.hasNext()) {
      HoodieRecord<T> record = recordItr.next();
      if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
      writeInsertRecord(record);
    }
  }
}
