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

package org.apache.hudi.common.serialization;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class TestHoodieFileSliceSerializer {
  private static final String FILE_ID_1 = "b5068208-e1a4-11e6-bf01-fe55135034f3";
  private static final String FILE_ID_2 = "b5068208-e1a4-11e6-bf01-fe55135034f4";
  private static final String LOG_FILE_PATH_FORMAT = "file:///tmp/basePath/partitionPath/.%s_100.log.%s_1-0-1";
  private static final short BLOCK_REPLICATION = 1;
  private static final StoragePath BASE_FILE_STORAGE_PATH_1 =
      new StoragePath("file:///tmp/basePath/partitionPath/" + FSUtils.makeBaseFileName("001", "1-0-1", FILE_ID_1, ".parquet"));
  private static final StoragePath BASE_FILE_STORAGE_PATH_2 =
      new StoragePath("file:///tmp/basePath/partitionPath/" + FSUtils.makeBaseFileName("001", "1-0-1", FILE_ID_2, ".parquet"));
  private static final StoragePath LOG_FILE_STORAGE_PATH_1 = new StoragePath(String.format(LOG_FILE_PATH_FORMAT, FILE_ID_1, "0"));
  private static final StoragePath LOG_FILE_STORAGE_PATH_2 = new StoragePath(String.format(LOG_FILE_PATH_FORMAT, FILE_ID_2, "2"));

  @Test
  void testSerDe() throws IOException {
    HoodieFileSliceSerializer hoodieFileSliceSerializer = new HoodieFileSliceSerializer();
    HoodieBaseFile baseFile1 = new HoodieBaseFile(new StoragePathInfo(BASE_FILE_STORAGE_PATH_1, 100, false, BLOCK_REPLICATION, 1024, 0));
    HoodieLogFile logFile1 = new HoodieLogFile(new StoragePathInfo(LOG_FILE_STORAGE_PATH_1, 100, false, BLOCK_REPLICATION, 1024, 0));
    HoodieLogFile logFile2 = new HoodieLogFile("/dummy/base/" + FSUtils.makeInlineLogFileName(FILE_ID_1, HoodieLogFile.DELTA_EXTENSION, "001", 2, "1-0-1"));

    HoodieBaseFile baseFile2 = new HoodieBaseFile(new StoragePathInfo(BASE_FILE_STORAGE_PATH_2, 100, false, BLOCK_REPLICATION, 1024, 0));
    HoodieLogFile logFile3 = new HoodieLogFile(new StoragePathInfo(LOG_FILE_STORAGE_PATH_2, 100, false, BLOCK_REPLICATION, 1024, 0));
    HoodieLogFile logFile4 = new HoodieLogFile("/dummy/base/" + FSUtils.makeInlineLogFileName(FILE_ID_2, HoodieLogFile.DELTA_EXTENSION, "002", 2, "1-0-1"));

    List<FileSlice> fileSliceList = Arrays.asList(
        new FileSlice(new HoodieFileGroupId("partition1", FILE_ID_1), "001", baseFile1, Arrays.asList(logFile1, logFile2)),
        new FileSlice(new HoodieFileGroupId("partition2", FILE_ID_2), "001", baseFile2, Arrays.asList(logFile3, logFile4)),
        new FileSlice("partition3", "002", "fileId-3")
    );

    byte[] serializedBytes = hoodieFileSliceSerializer.serialize(fileSliceList);
    List<FileSlice> deserialized = hoodieFileSliceSerializer.deserialize(serializedBytes);
    Assertions.assertEquals(fileSliceList, deserialized);
  }
}
