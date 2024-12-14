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
    LOG.info("Create HoodieMergeHandle implementation {} for fileId {} and partition path {} at commit {}", mergeHandleClasses.getLeft(), fileId, partitionPath, instantTime);
    try {
      return ReflectionUtils.loadClass(mergeHandleClasses.getLeft(),
          new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, Iterator.class, String.class, String.class,
              TaskContextSupplier.class, Option.class},
          writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    } catch (Throwable e1) {
      if (isFallbackEnabled && null != mergeHandleClasses.getRight()) {
        try {
          LOG.warn("HoodieMergeHandle implementation {} failed, now creating fallback implementation {} for fileId {} and partitionPath {} at commit {}",
              mergeHandleClasses.getLeft(), mergeHandleClasses.getRight(), fileId, partitionPath, instantTime);
          return ReflectionUtils.loadClass(mergeHandleClasses.getRight(),
              new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, Iterator.class, String.class, String.class,
                  TaskContextSupplier.class, Option.class},
              writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
        } catch (Throwable e2) {
          throw new HoodieException("Could not instantiate the fallback HoodieMergeHandle implementation: " + mergeHandleClasses.getRight(), e2);
        }
      }
      throw new HoodieException("Could not instantiate the HoodieMergeHandle implementation: " + mergeHandleClasses.getLeft(), e1);
    }
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
    LOG.info("Create HoodieMergeHandle implementation {} for fileId {} and partitionPath {} at commit {}", mergeHandleClasses.getLeft(), fileId, partitionPath, instantTime);
    try {
      return (HoodieMergeHandle) ReflectionUtils.loadClass(mergeHandleClasses.getLeft(),
          new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, Map.class, String.class, String.class,
              HoodieBaseFile.class, TaskContextSupplier.class, Option.class},
          writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    } catch (Throwable e1) {
      if (isFallbackEnabled && null != mergeHandleClasses.getRight()) {
        try {
          LOG.warn("HoodieMergeHandle implementation {} failed, now creating fallback implementation {} for fileId {} and partitionPath {} at commit {}",
              mergeHandleClasses.getLeft(), mergeHandleClasses.getRight(), fileId, partitionPath, instantTime);
          return (HoodieMergeHandle) ReflectionUtils.loadClass(mergeHandleClasses.getRight(),
              new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, Map.class, String.class, String.class,
                  HoodieBaseFile.class, TaskContextSupplier.class, Option.class},
              writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
        } catch (Throwable e2) {
          throw new HoodieException("Could not instantiate the fallback HoodieMergeHandle implementation: " + mergeHandleClasses.getRight(), e2);
        }
      }
      throw new HoodieException("Could not instantiate the HoodieMergeHandle implementation: " + mergeHandleClasses.getLeft(), e1);
    }
  }

  @VisibleForTesting
  static Pair<String, String> getMergeHandleClassesWrite(WriteOperationType operationType, HoodieWriteConfig writeConfig, HoodieTable table) {
    String mergeHandleClass;
    String fallbackMergeHandleClass = null;
    // Overwrite to a different implementation for {@link HoodieRowMergeHandle} if sorting or CDC is enabled.
    if (table.requireSortedRecords()) {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        mergeHandleClass = HoodieSortedRowMergeHandleWithChangeLog.class.getName();
      } else {
        mergeHandleClass = HoodieSortedRowMergeHandle.class.getName();
      }
    } else if (!WriteOperationType.isChangingRecords(operationType) && writeConfig.allowDuplicateInserts()) {
      mergeHandleClass = HoodieRowConcatHandle.class.getName();
    } else if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
      mergeHandleClass = HoodieRowMergeHandleWithChangeLog.class.getName();
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

    String defaultMergeHandleClass = HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue();
    // Overwrite to sorted implementation for {@link HoodieRowMergeHandle} if sorting is required.
    if (table.requireSortedRecords()) {
      mergeHandleClass = HoodieSortedRowMergeHandle.class.getName();
    } else {
      mergeHandleClass = writeConfig.getMergeHandleClassName();
      if (!mergeHandleClass.equals(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue())) {
        fallbackMergeHandleClass = HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.defaultValue();
      }
    }

    return Pair.of(mergeHandleClass, fallbackMergeHandleClass);
  }
}