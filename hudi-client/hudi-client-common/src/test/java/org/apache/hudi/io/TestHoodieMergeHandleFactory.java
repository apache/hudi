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

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

import static org.apache.hudi.config.HoodieWriteConfig.MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE;
import static org.mockito.Mockito.when;

public class TestHoodieMergeHandleFactory {

  private static final String CUSTOM_MERGE_HANDLE = "io.custom.CustomMergeHandle.java";
  private static final String BASE_PATH = "base_path";

  @Mock
  private HoodieTable mockHoodieTable;
  @Mock
  private HoodieTableConfig mockHoodieTableConfig;
  @Mock
  private HoodieTableMetaClient mockMetaClient;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockHoodieTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockMetaClient.getTableConfig()).thenReturn(mockHoodieTableConfig);
  }

  @Test
  public void validateWriterPathFactoryImpl() {
    // default case
    Properties properties = new Properties();
    properties.setProperty(MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key(), "false");
    Pair mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    // sorted case
    when(mockHoodieTable.requireSortedRecords()).thenReturn(true);
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(true);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieSortedMergeHandleWithChangeLog.class.getName());
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(false);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieSortedMergeHandle.class.getName());

    // non-sorted: no CDC cases
    when(mockHoodieTable.requireSortedRecords()).thenReturn(false);
    Properties propsWithDups = new Properties();
    propsWithDups.setProperty(MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key(), "true");
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.INSERT, getWriterConfig(propsWithDups), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieConcatHandle.class.getName());

    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(propsWithDups), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.INSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    // non-sorted: CDC enabled
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(true);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(propsWithDups), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.INSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    // custom merge handle
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(false);
    properties.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    properties.setProperty(HoodieWriteConfig.CONCAT_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    propsWithDups.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, CUSTOM_MERGE_HANDLE, FileGroupReaderBasedMergeHandle.class.getName());

    when(mockHoodieTable.requireSortedRecords()).thenReturn(true);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieSortedMergeHandle.class.getName());

    when(mockHoodieTable.requireSortedRecords()).thenReturn(false);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.INSERT, getWriterConfig(propsWithDups), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieConcatHandle.class.getName());

    propsWithDups.setProperty(HoodieWriteConfig.CONCAT_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.INSERT, getWriterConfig(propsWithDups), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, CUSTOM_MERGE_HANDLE, HoodieConcatHandle.class.getName());

    // Filegroup reader based merge handle class
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(false);
    properties.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), FileGroupReaderBasedMergeHandle.class.getName());
    propsWithDups.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName(), null);

    // even if CDC is enabled, its the same FG reader based merge handle class.
    when(mockHoodieTableConfig.isCDCEnabled()).thenReturn(true);
    properties.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), FileGroupReaderBasedMergeHandle.class.getName());
    propsWithDups.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesWrite(WriteOperationType.UPSERT, getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName(), null);
  }

  @Test
  public void validateCompactionPathFactoryImpl() {
    // default case
    Properties properties = new Properties();
    Pair mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesCompaction(getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, FileGroupReaderBasedMergeHandle.class.getName());

    // sorted case
    when(mockHoodieTable.requireSortedRecords()).thenReturn(true);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesCompaction(getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieSortedMergeHandle.class.getName());

    // custom case
    when(mockHoodieTable.requireSortedRecords()).thenReturn(false);
    properties.setProperty(HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key(), CUSTOM_MERGE_HANDLE);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesCompaction(getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, CUSTOM_MERGE_HANDLE, FileGroupReaderBasedMergeHandle.class.getName());

    when(mockHoodieTable.requireSortedRecords()).thenReturn(true);
    mergeHandleClasses = HoodieMergeHandleFactory.getMergeHandleClassesCompaction(getWriterConfig(properties), mockHoodieTable);
    validateMergeClasses(mergeHandleClasses, HoodieSortedMergeHandle.class.getName());

  }

  private void validateMergeClasses(Pair<String, String> mergeHandleClasses, String expectedMergeHandleClasses) {
    validateMergeClasses(mergeHandleClasses, expectedMergeHandleClasses, null);
  }

  private void validateMergeClasses(Pair<String, String> mergeHandleClasses, String expectedMergeHandleClass, String expectedFallbackClass) {
    Assertions.assertEquals(expectedMergeHandleClass, mergeHandleClasses.getLeft());
    Assertions.assertEquals(expectedFallbackClass, mergeHandleClasses.getRight());
  }

  private HoodieWriteConfig getWriterConfig(Properties properties) {
    return HoodieWriteConfig.newBuilder().withPath(BASE_PATH).withProperties(properties).build();
  }
}
