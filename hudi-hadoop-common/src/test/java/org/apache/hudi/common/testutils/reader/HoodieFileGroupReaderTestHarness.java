/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;

public class HoodieFileGroupReaderTestHarness extends HoodieCommonTestHarness {
  protected static final String PARTITION_PATH = "any-partition-path";
  protected static final String FILE_ID = "any-file-1";
  // Set the key range for base file and log files.
  protected static List<HoodieFileSliceTestUtils.KeyRange> keyRanges;
  // Set the ordering field for each record set.
  protected static List<Long> timestamps;
  // Set the record types for each record set.
  protected static List<DataGenerationPlan.OperationType> operationTypes;
  // Set the instantTime for each record set.
  protected static List<String> instantTimes;

  // Environmental variables.
  protected static StorageConfiguration<?> storageConf;
  protected static HoodieTestTable testTable;
  protected static HoodieReaderContext<IndexedRecord> readerContext;
  protected static TypedProperties properties;

  static {
    // Note: Make `timestamp` as ordering field.
    properties = new TypedProperties();
    properties.setProperty(
        "hoodie.datasource.write.precombine.field", "timestamp");
    storageConf = getDefaultStorageConf();
    readerContext = new HoodieTestReaderContext(
        Option.empty(), Option.empty());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    FileSystem.closeAll();
  }

  /**
   * Assume the test is for MOR tables by default.
   */
  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  protected void setUpMockCommits() throws Exception {
    for (String instantTime : instantTimes) {
      testTable.addDeltaCommit(instantTime);
    }
  }

  protected ClosableIterator<IndexedRecord> getFileGroupIterator(int numFiles)
      throws IOException, InterruptedException {
    assert (numFiles >= 1 && numFiles <= keyRanges.size());

    Option<FileSlice> fileSliceOpt =
        HoodieFileSliceTestUtils.getFileSlice(
            readerContext.getStorage(basePath, storageConf),
            keyRanges.subList(0, numFiles),
            timestamps.subList(0, numFiles),
            operationTypes.subList(0, numFiles),
            instantTimes.subList(0, numFiles),
            basePath,
            PARTITION_PATH,
            FILE_ID
        );

    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfig.setValue(POPULATE_META_FIELDS, "false");
    HoodieFileGroupReader<IndexedRecord> fileGroupReader =
        HoodieFileGroupReaderTestUtils.createFileGroupReader(
            fileSliceOpt,
            basePath,
            "1000", // Not used internally.
            AVRO_SCHEMA,
            false,
            0L,
            Long.MAX_VALUE,
            properties,
            new HoodieHadoopStorage(basePath, storageConf),
            tableConfig,
            readerContext
        );

    fileGroupReader.initRecordIterators();
    return fileGroupReader.getClosableIterator();
  }
}
