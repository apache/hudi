package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class WriteHandleMetadataUtils {

  /**
   * The utility function used by Append Handle to generate secondary index stats for the file slice
   * considering the new log files added by the handle. The function generates secondary index stats by
   * reading the file slice without the new log files and comparing it by reading file slice with the
   * new log files written by the handle.
   *
   * @param partitionPath - Partition path
   * @param fileId - Corresponding file id
   * @param fileSliceOpt - File slice
   * @param newLogFiles - New log files
   * @param status - Write status corresponding to the last log file written by the handle
   * @param hoodieTable - Hoodie Table
   * @param taskContextSupplier - Task context supplier
   * @param secondaryIndexDefns - Definitions for secondary index which need to be updated
   * @param config - Write config
   * @param instantTime - Instant time of the commit
   * @param writeSchemaWithMetaFields - Write schema with metadata fields
   */
  static void trackMetadataIndexStatsForStreamingMetadataWrites(String partitionPath, String fileId, Option<FileSlice> fileSliceOpt, List<String> newLogFiles, WriteStatus status,
                                                                HoodieTable hoodieTable, TaskContextSupplier taskContextSupplier, List<Pair<String, HoodieIndexDefinition>> secondaryIndexDefns,
                                                                HoodieWriteConfig config, String instantTime, Schema writeSchemaWithMetaFields) {
    // TODO: @see <a href="https://issues.apache.org/jira/browse/HUDI-9533">HUDI-9533</a> Optimise the computation for multiple secondary indexes
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(hoodieTable.getStorageConf(), taskContextSupplier);
    HoodieReaderContext readerContext = engineContext.getReaderContextFactory(hoodieTable.getMetaClient()).getContext();

    // For Append handle, we need to merge the records written by new log files with the existing file slice to check
    // the corresponding updates for secondary index. It is possible the records written in the new log files are ignored
    // by record merger.
    secondaryIndexDefns.forEach(secondaryIndexDefnPair -> {
      // fetch primary key -> secondary index for prev file slice.
      Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice = fileSliceOpt.map(fileSlice -> {
        try {
          return SecondaryIndexRecordGenerationUtils.getRecordKeyToSecondaryKey(hoodieTable.getMetaClient(), readerContext, fileSlice, writeSchemaWithMetaFields,
              secondaryIndexDefnPair.getValue(), instantTime, config.getProps(), false);
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
            latestIncludingInflight, writeSchemaWithMetaFields, secondaryIndexDefnPair.getValue(), instantTime, config.getProps(), true);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to generate secondary index stats ", e);
      }

      // Iterate over record keys in current file slice and prepare the secondary index records based on
      // whether the record is a new record, record with updated secondary key or deleted record
      recordKeyToSecondaryKeyForCurrentFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!recordKeyToSecondaryKeyForPreviousFileSlice.containsKey(recordKey)) {
          status.getIndexStats().addSecondaryIndexStats(secondaryIndexDefnPair.getKey(), recordKey, secondaryKey, false);
        } else {
          // delete previous entry and insert new value if secondaryKey is different
          String previousSecondaryKey = recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey);
          if (!previousSecondaryKey.equals(secondaryKey)) {
            status.getIndexStats().addSecondaryIndexStats(secondaryIndexDefnPair.getKey(), recordKey, previousSecondaryKey, true);
            status.getIndexStats().addSecondaryIndexStats(secondaryIndexDefnPair.getKey(), recordKey, secondaryKey, false);
          }
        }
      });

      // Iterate over records in previous file slice and add delete secondary index records for records not present
      // in current file slice
      Map<String, String> finalRecordKeyToSecondaryKeyForCurrentFileSlice = recordKeyToSecondaryKeyForCurrentFileSlice;
      recordKeyToSecondaryKeyForPreviousFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!finalRecordKeyToSecondaryKeyForCurrentFileSlice.containsKey(recordKey)) {
          status.getIndexStats().addSecondaryIndexStats(secondaryIndexDefnPair.getKey(), recordKey, secondaryKey, true);
        }
      });
    });
  }

  /**
   * Utility function used by HoodieCreateHandle to generate secondary index stats for the corresponding hoodie record.
   *
   * @param record - HoodieRecord
   * @param writeStatus - WriteStatus
   * @param writeSchemaWithMetaFields - Schema with metadata fields
   * @param secondaryIndexDefns - Definitions for secondary index which need to be updated
   * @param hoodieTable - Hoodie table
   * @param taskContextSupplier - Task context supplier
   */
  static void trackMetadataIndexStats(HoodieRecord record, WriteStatus writeStatus, Schema writeSchemaWithMetaFields, List<Pair<String, HoodieIndexDefinition>> secondaryIndexDefns,
                                      HoodieTable hoodieTable, TaskContextSupplier taskContextSupplier) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(hoodieTable.getStorageConf(), taskContextSupplier);
    HoodieReaderContext readerContext = engineContext.getReaderContextFactory(hoodieTable.getMetaClient()).getContext();

    // Add secondary index records for all the inserted records
    secondaryIndexDefns.forEach(secondaryIndexPartitionPathFieldPair -> {
      String secondaryIndexSourceField = String.join(".", secondaryIndexPartitionPathFieldPair.getValue().getSourceFields());
      if (record instanceof HoodieAvroIndexedRecord) {
        Object secondaryKey = readerContext.getValue(record.getData(), writeSchemaWithMetaFields, secondaryIndexSourceField);
        if (secondaryKey != null) {
          writeStatus.getIndexStats().addSecondaryIndexStats(secondaryIndexPartitionPathFieldPair.getKey(), record.getRecordKey(), secondaryKey.toString(), false);
        }
      }
    });
  }

  /**
   * The utility function used by Merge Handle to generate secondary index stats for the corresponding record.
   * It considers the new merged version of the record and compares it with the older version of the record to generate
   * secondary index stats.
   *
   * @param hoodieKeyOpt - Option containing the value of hoodie key
   * @param combinedRecordOpt - New record merged with the old record
   * @param oldRecordOpt - Old record option
   * @param isDelete - Whether the record is being deleted
   * @param writeStatus - Write status
   * @param writeSchemaWithMetaFields - Write schema with metadata fields
   * @param newSchemaSupplier - Schema supplier for the new record
   * @param secondaryIndexDefns - Definitions for secondary index which need to be updated
   * @param keyGeneratorOpt - Option containing key generator
   * @param hoodieTable - Hoodie Table
   * @param taskContextSupplier - Task context supplier
   */
  static <T> void trackMetadataIndexStats(Option<HoodieKey> hoodieKeyOpt, Option<HoodieRecord> combinedRecordOpt, Option<HoodieRecord<T>> oldRecordOpt, boolean isDelete,
                                          WriteStatus writeStatus, Schema writeSchemaWithMetaFields, Supplier<Schema> newSchemaSupplier,
                                          List<Pair<String, HoodieIndexDefinition>> secondaryIndexDefns, Option<BaseKeyGenerator> keyGeneratorOpt, HoodieTable hoodieTable,
                                          TaskContextSupplier taskContextSupplier) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(hoodieTable.getStorageConf(), taskContextSupplier);
    HoodieReaderContext readerContext = engineContext.getReaderContextFactory(hoodieTable.getMetaClient()).getContext();

    secondaryIndexDefns.forEach(secondaryIndexPartitionPathFieldPair -> {
      String secondaryIndexSourceField = String.join(".", secondaryIndexPartitionPathFieldPair.getValue().getSourceFields());
      Option<Object> oldSecondaryKeyOpt = Option.empty();
      Option<Object> newSecondaryKeyOpt = Option.empty();
      if (oldRecordOpt.isPresent() && oldRecordOpt.get() instanceof HoodieAvroIndexedRecord) {
        HoodieRecord<T> oldRecord = oldRecordOpt.get();
        Object oldSecondaryKey = readerContext.getValue(oldRecord.getData(), writeSchemaWithMetaFields, secondaryIndexSourceField);
        if (oldSecondaryKey != null) {
          oldSecondaryKeyOpt = Option.of(oldSecondaryKey);
        }
      }

      if (combinedRecordOpt.isPresent() && combinedRecordOpt.get() instanceof HoodieAvroIndexedRecord && !isDelete) {
        Schema newSchema = newSchemaSupplier.get();
        Object secondaryKey = readerContext.getValue(combinedRecordOpt.get().getData(), newSchema, secondaryIndexSourceField);
        if (secondaryKey != null) {
          newSecondaryKeyOpt = Option.of(secondaryKey);
        }
      }

      boolean shouldUpdate = true;
      if (oldSecondaryKeyOpt.isPresent() && newSecondaryKeyOpt.isPresent()) {
        // If new secondary key is different from old secondary key, update secondary index records
        shouldUpdate = !oldSecondaryKeyOpt.get().equals(newSecondaryKeyOpt.get());
      }
      if (shouldUpdate) {
        String recordKey = hoodieKeyOpt.map(HoodieKey::getRecordKey)
            .or(() -> oldRecordOpt.map(rec -> rec.getRecordKey(writeSchemaWithMetaFields, keyGeneratorOpt)))
            .or(() -> combinedRecordOpt.map(HoodieRecord::getRecordKey))
            .get();
        // Add secondary index delete records for old records
        oldSecondaryKeyOpt.ifPresent(secKey -> addSecondaryIndexStat(writeStatus, secondaryIndexPartitionPathFieldPair.getKey(), recordKey, secKey, true));
        newSecondaryKeyOpt.ifPresent(secKey ->
            addSecondaryIndexStat(writeStatus, secondaryIndexPartitionPathFieldPair.getKey(), recordKey, secKey, false));
      }
    });
  }

  private static void addSecondaryIndexStat(WriteStatus writeStatus, String secondaryIndexPartitionPath, String recordKey, Object secKey, boolean isDeleted) {
    writeStatus.getIndexStats().addSecondaryIndexStats(secondaryIndexPartitionPath, recordKey, secKey.toString(), isDeleted);
  }
}
