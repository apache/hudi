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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.config.HoodieWriteConfig.COMPACT_MERGE_HANDLE_CLASS_NAME;

/**
 * Factory class for instantiating the appropriate implementation of {@link HoodieMergeHandle}.
 */
public class HoodieMergeHandleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeHandleFactory.class);

  /**
   * Creates a merge handle for normal write path.
   */
  public static <T, I, K, O> HoodieMergeHandle<T, I, K, O> create(
      WriteOperationType operationType,
      HoodieWriteConfig writeConfig,
      String instantTime,
      HoodieTable<T, I, K, O> table,
      Iterator<HoodieRecord<T>> recordItr,
      String partitionPath,
      String fileId,
      TaskContextSupplier taskContextSupplier,
      Option<BaseKeyGenerator> keyGeneratorOpt) {

    boolean isFallbackEnabled = writeConfig.isMergeHandleFallbackEnabled();
    Pair<String, String> mergeHandleClasses = getMergeHandleClassesWrite(operationType, writeConfig, table);
    String logContext = String.format("for fileId %s and partition path %s at commit %s", fileId, partitionPath, instantTime);
    LOG.info("Create HoodieMergeHandle implementation {} {}", mergeHandleClasses.getLeft(), logContext);

    Class<?>[] constructorParamTypes = new Class<?>[] {
        HoodieWriteConfig.class, String.class, HoodieTable.class, Iterator.class,
        String.class, String.class, TaskContextSupplier.class, Option.class
    };

    return instantiateMergeHandle(
        isFallbackEnabled, mergeHandleClasses.getLeft(), mergeHandleClasses.getRight(), logContext, constructorParamTypes,
        writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
  }

  /**
   * Creates a merge handle for compaction path.
   */
  public static <T, I, K, O> HoodieMergeHandle<T, I, K, O> create(
      HoodieWriteConfig writeConfig,
      String instantTime,
      HoodieTable<T, I, K, O> table,
      Map<String, HoodieRecord<T>> keyToNewRecords,
      String partitionPath,
      String fileId,
      HoodieBaseFile dataFileToBeMerged,
      TaskContextSupplier taskContextSupplier,
      Option<BaseKeyGenerator> keyGeneratorOpt) {

    boolean isFallbackEnabled = writeConfig.isMergeHandleFallbackEnabled();
    Pair<String, String> mergeHandleClasses = getMergeHandleClassesCompaction(writeConfig, table);
    String logContext = String.format("for fileId %s and partitionPath %s at commit %s", fileId, partitionPath, instantTime);
    LOG.info("Create HoodieMergeHandle implementation {} {}", mergeHandleClasses.getLeft(), logContext);

    Class<?>[] constructorParamTypes = new Class<?>[] {
        HoodieWriteConfig.class, String.class, HoodieTable.class, Map.class,
        String.class, String.class, HoodieBaseFile.class, TaskContextSupplier.class, Option.class
    };

    return instantiateMergeHandle(
        isFallbackEnabled, mergeHandleClasses.getLeft(), mergeHandleClasses.getRight(), logContext, constructorParamTypes,
        writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
  }

  /**
   * Creates a merge handle for compaction with file group reader.
   */
  public static <T, I, K, O> HoodieMergeHandle<T, I, K, O> create(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      FileSlice fileSlice,
      CompactionOperation operation,
      TaskContextSupplier taskContextSupplier,
      HoodieReaderContext<T> readerContext,
      String maxInstantTime,
      HoodieRecord.HoodieRecordType recordType) {

    boolean isFallbackEnabled = config.isMergeHandleFallbackEnabled();

    String mergeHandleClass = config.getCompactionMergeHandleClassName();
    String logContext = String.format("for fileId %s and partitionPath %s at commit %s", operation.getFileId(), operation.getPartitionPath(), instantTime);
    LOG.info("Create HoodieMergeHandle implementation {} {}", mergeHandleClass, logContext);

    Class<?>[] constructorParamTypes = new Class<?>[] {
        HoodieWriteConfig.class, String.class, HoodieTable.class, FileSlice.class, CompactionOperation.class,
        TaskContextSupplier.class, HoodieReaderContext.class, String.class, HoodieRecord.HoodieRecordType.class
    };

    return instantiateMergeHandle(
        isFallbackEnabled, mergeHandleClass, COMPACT_MERGE_HANDLE_CLASS_NAME.defaultValue(), logContext, constructorParamTypes,
        config, instantTime, hoodieTable, fileSlice, operation, taskContextSupplier, readerContext, maxInstantTime, recordType);
  }

  /**
   * Helper method to instantiate a HoodieMergeHandle via reflection, with an optional fallback.
   */
  private static <T, I, K, O> HoodieMergeHandle<T, I, K, O> instantiateMergeHandle(
      boolean isFallbackEnabled,
      String primaryClass,
      String fallbackClass,
      String logContext,
      Class<?>[] constructorParamTypes,
      Object... initargs) {
    try {
      return (HoodieMergeHandle<T, I, K, O>) ReflectionUtils.loadClass(primaryClass, constructorParamTypes, initargs);
    } catch (Throwable e1) {
      if (isFallbackEnabled && fallbackClass != null && !Objects.equals(primaryClass, fallbackClass)) {
        try {
          LOG.warn("HoodieMergeHandle implementation {} failed, now creating fallback implementation {} {}",
              primaryClass, fallbackClass, logContext);
          return (HoodieMergeHandle<T, I, K, O>) ReflectionUtils.loadClass(fallbackClass, constructorParamTypes, initargs);
        } catch (Throwable e2) {
          throw new HoodieException("Could not instantiate the fallback HoodieMergeHandle implementation: " + fallbackClass, e2);
        }
      }
      throw new HoodieException("Could not instantiate the HoodieMergeHandle implementation: " + primaryClass, e1);
    }
  }

  @VisibleForTesting
  static Pair<String, String> getMergeHandleClassesWrite(WriteOperationType operationType, HoodieWriteConfig writeConfig, HoodieTable table) {
    String mergeHandleClass;
    String fallbackMergeHandleClass = null;
    // Overwrite to a different implementation for {@link HoodieWriteMergeHandle} if sorting or CDC is enabled.
    if (table.requireSortedRecords()) {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        mergeHandleClass = HoodieSortedMergeHandleWithChangeLog.class.getName();
      } else {
        mergeHandleClass = HoodieSortedMergeHandle.class.getName();
      }
    } else if (!WriteOperationType.isChangingRecords(operationType) && writeConfig.allowDuplicateInserts()) {
      mergeHandleClass = writeConfig.getConcatHandleClassName();
      if (!mergeHandleClass.equals(HoodieWriteConfig.CONCAT_HANDLE_CLASS_NAME.defaultValue())) {
        fallbackMergeHandleClass = HoodieWriteConfig.CONCAT_HANDLE_CLASS_NAME.defaultValue();
      }
    } else if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
      mergeHandleClass = HoodieMergeHandleWithChangeLog.class.getName();
    } else {
      mergeHandleClass = writeConfig.getMergeHandleClassName();
      if (!mergeHandleClass.equals(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue())) {
        fallbackMergeHandleClass = HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue();
      }
    }

    return Pair.of(mergeHandleClass, fallbackMergeHandleClass);
  }

  @VisibleForTesting
  static Pair<String, String> getMergeHandleClassesCompaction(HoodieWriteConfig writeConfig, HoodieTable table) {
    String mergeHandleClass;
    String fallbackMergeHandleClass = null;
    // Overwrite to sorted implementation for {@link HoodieWriteMergeHandle} if sorting is required.
    if (table.requireSortedRecords()) {
      mergeHandleClass = HoodieSortedMergeHandle.class.getName();
    } else if (table.getMetaClient().getTableConfig().isCDCEnabled() && writeConfig.isYieldingPureLogForMor()) {
      // IMPORTANT: only index type that yields pure log files need to enable the cdc log files for compaction,
      // index type such as the BLOOM does not need this because it would do delta merge for inserts and generates log for updates,
      // both of these two cases are already handled in HoodieCDCExtractor.
      mergeHandleClass = HoodieMergeHandleWithChangeLog.class.getName();
    } else {
      mergeHandleClass = writeConfig.getMergeHandleClassName();
      if (!mergeHandleClass.equals(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue())) {
        fallbackMergeHandleClass = HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue();
      }
    }

    return Pair.of(mergeHandleClass, fallbackMergeHandleClass);
  }
}