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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility class for write handle metadata.
 */
public class SecondaryIndexStreamingTracker {

  /**
   * The utility function used by Append Handle to generate secondary index stats for the file slice
   * considering the new log files added by the append handle. The function generates secondary index stats by
   * reading the file slice without the new log files and comparing it by reading file slice with the
   * new log files written by the handle.
   *
   * @param partitionPath             Partition path
   * @param fileId                    Corresponding file id
   * @param fileSliceOpt              File slice
   * @param newLogFiles               New log files
   * @param status                    Write status corresponding to the last log file written by the handle
   * @param hoodieTable               Hoodie Table
   * @param secondaryIndexDefns       Definitions for secondary index which need to be updated
   * @param config                    Write config
   * @param instantTime               Instant time of the commit
   * @param writeSchemaWithMetaFields Write schema with metadata fields
   */
  static void trackSecondaryIndexStats(String partitionPath, String fileId, Option<FileSlice> fileSliceOpt, List<String> newLogFiles, WriteStatus status,
                                       HoodieTable hoodieTable, List<HoodieIndexDefinition> secondaryIndexDefns,
                                       HoodieWriteConfig config, String instantTime, HoodieSchema writeSchemaWithMetaFields) {
    // TODO: @see <a href="https://issues.apache.org/jira/browse/HUDI-9533">HUDI-9533</a> Optimise the computation for multiple secondary indexes
    HoodieReaderContext readerContext = hoodieTable.getContext().getReaderContextFactory(hoodieTable.getMetaClient()).getContext();

    // For Append handle, we need to merge the records written by new log files with the existing file slice to check
    // the corresponding updates for secondary index. It is possible the records written in the new log files are ignored
    // by record merger.
    secondaryIndexDefns.forEach(def -> {
      // fetch primary key -> secondary index for prev file slice.
      Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice = fileSliceOpt.map(fileSlice -> {
        try {
          return SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(hoodieTable.getMetaClient(), readerContext, fileSlice, writeSchemaWithMetaFields,
              def, instantTime, config.getProps(), false);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to generate secondary index stats ", e);
        }
      }).orElse(Collections.emptyMap());

      // fetch primary key -> secondary index for latest file slice including inflight.
      FileSlice latestIncludingInflight = fileSliceOpt.orElse(new FileSlice(new HoodieFileGroupId(partitionPath, fileId), instantTime));
      // Add all the new log files to the file slice
      newLogFiles.stream().forEach(logFile -> latestIncludingInflight.addLogFile(new HoodieLogFile(new StoragePath(config.getBasePath(), logFile))));
      Map<String, String> recordKeyToSecondaryKeyForCurrentFileSlice;
      try {
        recordKeyToSecondaryKeyForCurrentFileSlice = SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(hoodieTable.getMetaClient(), readerContext,
            latestIncludingInflight, writeSchemaWithMetaFields, def, instantTime, config.getProps(), true);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to generate secondary index stats ", e);
      }

      // Iterate over record keys in current file slice and prepare the secondary index records based on
      // whether the record is a new record, record with updated secondary key or deleted record
      final String indexName = def.getIndexName();
      recordKeyToSecondaryKeyForCurrentFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!recordKeyToSecondaryKeyForPreviousFileSlice.containsKey(recordKey)) {
          status.getIndexStats().addSecondaryIndexStats(indexName, recordKey, secondaryKey, false);
        } else {
          // delete previous entry and insert new value if secondaryKey is different
          String previousSecondaryKey = recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey);
          if (!Objects.equals(previousSecondaryKey, secondaryKey)) {
            status.getIndexStats().addSecondaryIndexStats(indexName, recordKey, previousSecondaryKey, true);
            status.getIndexStats().addSecondaryIndexStats(indexName, recordKey, secondaryKey, false);
          }
        }
      });

      // Iterate over records in previous file slice and add delete secondary index records for records not present
      // in current file slice
      Map<String, String> finalRecordKeyToSecondaryKeyForCurrentFileSlice = recordKeyToSecondaryKeyForCurrentFileSlice;
      recordKeyToSecondaryKeyForPreviousFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!finalRecordKeyToSecondaryKeyForCurrentFileSlice.containsKey(recordKey)) {
          status.getIndexStats().addSecondaryIndexStats(indexName, recordKey, secondaryKey, true);
        }
      });
    });
  }

  /**
   * Utility function used by HoodieCreateHandle to generate secondary index stats for the corresponding hoodie record.
   *
   * @param record                    HoodieRecord
   * @param writeStatus               WriteStatus
   * @param writeSchemaWithMetaFields Schema with metadata fields
   * @param secondaryIndexDefns       Definitions for secondary index which need to be updated
   * @param config                    Hoodie write config
   */
  static void trackSecondaryIndexStats(HoodieRecord record, WriteStatus writeStatus, Schema writeSchemaWithMetaFields,
                                       List<HoodieIndexDefinition> secondaryIndexDefns, HoodieWriteConfig config) {
    // Add secondary index records for all the inserted records (including null values)
    secondaryIndexDefns.forEach(def -> {
      Object secondaryKey = record.getColumnValueAsJava(writeSchemaWithMetaFields, def.getSourceFieldsKey(), config.getProps());
      addSecondaryIndexStat(writeStatus, def.getIndexName(), record.getRecordKey(), secondaryKey, false);
    });
  }

  /**
   * The utility function used by Merge Handle to generate secondary index stats for the corresponding record.
   * It considers the new merged version of the record and compares it with the older version of the record to generate
   * secondary index stats.
   *
   * @param hoodieKey                 The hoodie key
   * @param combinedRecord            New record merged with the old record
   * @param oldRecord                 The old record
   * @param isDelete                  Whether the record is a DELETE
   * @param writeStatus               The Write status
   * @param writeSchemaWithMetaFields The write schema with metadata fields
   * @param newSchemaSupplier         The schema supplier for the new record
   * @param secondaryIndexDefns       Definitions for secondary index which need to be updated
   * @param keyGeneratorOpt           Option containing key generator
   * @param config                    Hoodie write config
   */
  static <T> void trackSecondaryIndexStats(@Nullable HoodieKey hoodieKey, HoodieRecord combinedRecord, @Nullable HoodieRecord<T> oldRecord, boolean isDelete,
                                           WriteStatus writeStatus, Schema writeSchemaWithMetaFields, Supplier<Schema> newSchemaSupplier,
                                           List<HoodieIndexDefinition> secondaryIndexDefns, Option<BaseKeyGenerator> keyGeneratorOpt, HoodieWriteConfig config) {

    secondaryIndexDefns.forEach(def -> {
      String secondaryIndexSourceField = def.getSourceFieldsKey();

      // Handle three cases explicitly:
      // 1. Old record does not exist (INSERT operation)
      // 2. Old record exists with a value (could be null value)
      // 3. New record state after operation

      boolean hasOldValue = oldRecord != null;
      Object oldSecondaryKey = null;

      if (hasOldValue) {
        oldSecondaryKey = oldRecord.getColumnValueAsJava(writeSchemaWithMetaFields, secondaryIndexSourceField, config.getProps());
      }

      // For new/combined record
      boolean hasNewValue = false;
      Object newSecondaryKey = null;

      if (!isDelete) {
        Schema newSchema = newSchemaSupplier.get();
        newSecondaryKey = combinedRecord.getColumnValueAsJava(newSchema, secondaryIndexSourceField, config.getProps());
        hasNewValue = true;
      }

      // Determine if we need to update the secondary index
      boolean shouldUpdate;
      if (!hasOldValue && !hasNewValue) {
        // Case 4: Neither old nor new value exists - do nothing
        shouldUpdate = false;
      } else if (hasOldValue && hasNewValue) {
        // Both old and new values exist - check if they differ
        shouldUpdate = !Objects.equals(oldSecondaryKey, newSecondaryKey);
      } else {
        // One exists but not the other - need to update
        shouldUpdate = true;
      }

      // All possible cases:
      // 1. Old record exists, new record does not exist - delete old secondary index entry
      // 2. Old record exists, new record exists - update secondary index entry
      // 3. Old record does not exist, new record exists - add new secondary index entry
      // 4. Old record does not exist, new record does not exist - do nothing
      if (shouldUpdate) {
        String recordKey = Option.ofNullable(hoodieKey).map(HoodieKey::getRecordKey)
            .or(() -> Option.ofNullable(oldRecord).map(rec -> rec.getRecordKey(writeSchemaWithMetaFields, keyGeneratorOpt)))
            .orElseGet(combinedRecord::getRecordKey);

        // Delete old secondary index entry if old record exists.
        if (hasOldValue) {
          addSecondaryIndexStat(writeStatus, def.getIndexName(), recordKey, oldSecondaryKey, true);
        }

        // Add new secondary index entry if new value exists (including null values)
        if (hasNewValue) {
          addSecondaryIndexStat(writeStatus, def.getIndexName(), recordKey, newSecondaryKey, false);
        }
      }
    });
  }

  /**
   * The utility function used by Merge Handle to generate secondary index stats for the corresponding record.
   * It considers the new merged version of the record and compares it with the older version of the record to generate
   * secondary index stats.
   *
   * @param hoodieKey                 The hoodie key
   * @param combinedRecordOpt         New record merged with the old record
   * @param oldRecord                 The old record
   * @param isDelete                  Whether the record is a DELETE
   * @param writeStatus               The Write status
   * @param secondaryIndexDefns       Definitions for secondary index which need to be updated
   */
  static <T> void trackSecondaryIndexStats(HoodieKey hoodieKey, Option<BufferedRecord<T>> combinedRecordOpt, @Nullable BufferedRecord<T> oldRecord, boolean isDelete,
                                           WriteStatus writeStatus, List<HoodieIndexDefinition> secondaryIndexDefns, RecordContext<T> recordContext) {

    secondaryIndexDefns.forEach(def -> {
      String secondaryIndexSourceField = def.getSourceFieldsKey();

      // Handle three cases explicitly:
      // 1. Old record does not exist (INSERT operation)
      // 2. Old record exists with a value (could be null value)
      // 3. New record state after operation

      boolean hasOldValue = oldRecord != null;
      Object oldSecondaryKey = null;

      if (hasOldValue) {
        Schema schema = recordContext.decodeAvroSchema(oldRecord.getSchemaId());
        oldSecondaryKey = recordContext.getTypeConverter().castToString(recordContext.getValue(oldRecord.getRecord(), HoodieSchema.fromAvroSchema(schema), secondaryIndexSourceField));
      }

      // For new/combined record
      boolean hasNewValue = false;
      Object newSecondaryKey = null;

      if (combinedRecordOpt.isPresent() && !isDelete) {
        Schema schema = recordContext.decodeAvroSchema(combinedRecordOpt.get().getSchemaId());
        newSecondaryKey = recordContext.getTypeConverter().castToString(recordContext.getValue(combinedRecordOpt.get().getRecord(), HoodieSchema.fromAvroSchema(schema), secondaryIndexSourceField));
        hasNewValue = true;
      }

      // Determine if we need to update the secondary index
      boolean shouldUpdate;
      if (!hasOldValue && !hasNewValue) {
        // Case 4: Neither old nor new value exists - do nothing
        shouldUpdate = false;
      } else if (hasOldValue && hasNewValue) {
        // Both old and new values exist - check if they differ
        shouldUpdate = !Objects.equals(oldSecondaryKey, newSecondaryKey);
      } else {
        // One exists but not the other - need to update
        shouldUpdate = true;
      }

      // All possible cases:
      // 1. Old record exists, new record does not exist - delete old secondary index entry
      // 2. Old record exists, new record exists - update secondary index entry
      // 3. Old record does not exist, new record exists - add new secondary index entry
      // 4. Old record does not exist, new record does not exist - do nothing
      if (shouldUpdate) {
        String recordKey = hoodieKey.getRecordKey();

        // Delete old secondary index entry if old record exists.
        if (hasOldValue) {
          addSecondaryIndexStat(writeStatus, def.getIndexName(), recordKey, oldSecondaryKey, true);
        }

        // Add new secondary index entry if new value exists (including null values)
        if (hasNewValue) {
          addSecondaryIndexStat(writeStatus, def.getIndexName(), recordKey, newSecondaryKey, false);
        }
      }
    });
  }

  private static void addSecondaryIndexStat(WriteStatus writeStatus, String secondaryIndexPartitionPath, String recordKey, Object secKey, boolean isDeleted) {
    // Convert null to string representation - null values are valid in secondary indexes
    String secKeyStr = secKey == null ? null : secKey.toString();
    writeStatus.getIndexStats().addSecondaryIndexStats(secondaryIndexPartitionPath, recordKey, secKeyStr, isDeleted);
  }
}
