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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.marker.WriteMarkers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieSparkTable extends HoodieCommonTestHarness {

  private static final StorageConfiguration<?> CONF = getDefaultStorageConf();

  @ParameterizedTest
  @EnumSource(DeleteFailureType.class)
  public void testDeleteFailureDuringMarkerReconciliation(DeleteFailureType failureType) throws IOException {
    initPath();
    HoodieStorage localStorage = HoodieStorageUtils.getStorage(basePath, CONF);
    WriteMarkers writeMarkers = mock(WriteMarkers.class);
    String partitionPath = "p1";
    List<String> datafiles = Arrays.asList("file1", "file2", "file3");
    List<org.apache.hudi.common.model.HoodieWriteStat> writeStatList = new ArrayList<>();
    Set<String> markerList = new HashSet<>();
    datafiles.forEach(fileName -> {
      org.apache.hudi.common.model.HoodieWriteStat writeStat = new org.apache.hudi.common.model.HoodieWriteStat();
      writeStat.setPath(partitionPath + "/" + fileName);
      writeStatList.add(writeStat);
      markerList.add(partitionPath + "/" + fileName);
    });

    // add 2 additional entries to markers. and create the resp data file. These two files are expected to be deleted during reconciliation.
    List<String> additionalFiles = Arrays.asList("file4", "file5");
    additionalFiles.forEach(fileName -> {
      markerList.add(partitionPath + "/" + fileName);
    });

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath).withMarkersType(MarkerType.DIRECT.name()).build();
    when(writeMarkers.doesMarkerDirExist()).thenReturn(true);
    when(writeMarkers.createdAndMergedDataPaths(getEngineContext(), writeConfig.getFinalizeWriteParallelism())).thenReturn(markerList);

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath(basePath));
    when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getTableConfig()).thenReturn(new HoodieTableConfig());

    additionalFiles.forEach(fileName -> {
      try {
        StoragePath storagePath = new StoragePath(basePath + "/" + partitionPath + "/" + fileName);
        if (failureType == DeleteFailureType.TRUE_ON_DELETE) {
          when(storage.deleteFile(storagePath)).thenReturn(true);
        } else if (failureType == DeleteFailureType.FALSE_ON_DELETE_IS_EXISTS_FALSE) {
          when(storage.deleteFile(storagePath)).thenReturn(false);
          when(storage.exists(storagePath)).thenReturn(false);
        } else if (failureType == DeleteFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
          when(storage.deleteFile(storagePath)).thenReturn(false);
          when(storage.exists(storagePath)).thenReturn(true);
        } else if (failureType == DeleteFailureType.FILE_NOT_FOUND_EXC_ON_DELETE) {
          when(storage.deleteFile(storagePath)).thenThrow(new FileNotFoundException("throwing file not found exception"));
        } else {
          // run time exception
          when(storage.deleteFile(storagePath)).thenThrow(new RuntimeException("throwing run time exception"));
        }
        // lets create the data file. so that we can validate later.
        localStorage.create(storagePath);
      } catch (IOException e) {
        throw new HoodieException("Failed to check data file existance " + fileName);
      }
    });
    HoodieTable hoodieTable = HoodieSparkTable.create(writeConfig, getEngineContext(), metaClient);
    if (failureType == DeleteFailureType.RUNTIME_EXC_ON_DELETE || failureType == DeleteFailureType.FALSE_ON_DELETE_IS_EXISTS_TRUE) {
      assertThrows(HoodieException.class, () -> {
        hoodieTable.reconcileAgainstMarkers(getEngineContext(), "0001", writeStatList, false, false, writeMarkers);
      });
    } else { // all other cases
      hoodieTable.reconcileAgainstMarkers(getEngineContext(), "0001", writeStatList, false, false, writeMarkers);
      // validate that additional files are deleted from storage
      additionalFiles.forEach(fileName -> {
        try {
          verify(storage, times(1)).deleteFile(new StoragePath(basePath + "/" + partitionPath + "/" + fileName));
        } catch (IOException e) {
          throw new HoodieException("Failed to validate that file exists " + fileName);
        }
      });
    }
  }

  enum DeleteFailureType {
    TRUE_ON_DELETE,
    FALSE_ON_DELETE_IS_EXISTS_FALSE,
    FALSE_ON_DELETE_IS_EXISTS_TRUE,
    FILE_NOT_FOUND_EXC_ON_DELETE,
    RUNTIME_EXC_ON_DELETE
  }
}
