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

package org.apache.hudi.internal;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.hudi.testutils.SparkDatasetTestUtils.getConfigBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for TestHoodieBulkInsertDataInternalWriter.
 */
public class HoodieBulkInsertInternalWriterTestBase extends HoodieClientTestHarness {

  protected static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  protected HoodieWriteConfig getWriteConfig(boolean populateMetaFields) {
    Properties properties = new Properties();
    if (!populateMetaFields) {
      properties.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), SimpleKeyGenerator.class.getName());
      properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key(), SparkDatasetTestUtils.RECORD_KEY_FIELD_NAME);
      properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), SparkDatasetTestUtils.PARTITION_PATH_FIELD_NAME);
      properties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    }
    return getConfigBuilder(basePath, timelineServicePort).withProperties(properties).build();
  }

  protected void assertWriteStatuses(List<HoodieInternalWriteStatus> writeStatuses, int batches, int size,
                                     Option<List<String>> fileAbsPaths, Option<List<String>> fileNames) {
    assertWriteStatuses(writeStatuses, batches, size, false, fileAbsPaths, fileNames);
  }

  protected void assertWriteStatuses(List<HoodieInternalWriteStatus> writeStatuses, int batches, int size, boolean areRecordsSorted,
                                     Option<List<String>> fileAbsPaths, Option<List<String>> fileNames) {
    if (areRecordsSorted) {
      assertEquals(batches, writeStatuses.size());
    } else {
      assertEquals(Math.min(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS.length, batches), writeStatuses.size());
    }

    Map<String, Long> sizeMap = new HashMap<>();
    if (!areRecordsSorted) {
      // <size> no of records are written per batch. Every 4th batch goes into same writeStatus. So, populating the size expected
      // per write status
      for (int i = 0; i < batches; i++) {
        String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[i % 3];
        if (!sizeMap.containsKey(partitionPath)) {
          sizeMap.put(partitionPath, 0L);
        }
        sizeMap.put(partitionPath, sizeMap.get(partitionPath) + size);
      }
    }

    int counter = 0;
    for (HoodieInternalWriteStatus writeStatus : writeStatuses) {
      // verify write status
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3], writeStatus.getPartitionPath());
      if (areRecordsSorted) {
        assertEquals(writeStatus.getTotalRecords(), size);
      } else {
        assertEquals(writeStatus.getTotalRecords(), sizeMap.get(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3]));
      }
      assertNull(writeStatus.getGlobalError());
      assertEquals(writeStatus.getFailedRowsSize(), 0);
      assertEquals(writeStatus.getTotalErrorRecords(), 0);
      assertFalse(writeStatus.hasErrors());
      assertNotNull(writeStatus.getFileId());
      String fileId = writeStatus.getFileId();
      if (fileAbsPaths.isPresent()) {
        fileAbsPaths.get().add(basePath + "/" + writeStatus.getStat().getPath());
      }
      if (fileNames.isPresent()) {
        fileNames.get().add(writeStatus.getStat().getPath()
            .substring(writeStatus.getStat().getPath().lastIndexOf('/') + 1));
      }
      HoodieWriteStat writeStat = writeStatus.getStat();
      if (areRecordsSorted) {
        assertEquals(size, writeStat.getNumInserts());
        assertEquals(size, writeStat.getNumWrites());
      } else {
        assertEquals(sizeMap.get(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3]), writeStat.getNumInserts());
        assertEquals(sizeMap.get(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3]), writeStat.getNumWrites());
      }
      assertEquals(fileId, writeStat.getFileId());
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter++ % 3], writeStat.getPartitionPath());
      assertEquals(0, writeStat.getNumDeletes());
      assertEquals(0, writeStat.getNumUpdateWrites());
      assertEquals(0, writeStat.getTotalWriteErrors());
    }
  }

  protected void assertOutput(Dataset<Row> expectedRows, Dataset<Row> actualRows, String instantTime, Option<List<String>> fileNames,
                              boolean populateMetaColumns) {
    if (populateMetaColumns) {
      // verify 3 meta fields that are filled in within create handle
      actualRows.collectAsList().forEach(entry -> {
        assertEquals(entry.get(HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.ordinal()).toString(), instantTime);
        assertFalse(entry.isNullAt(HoodieMetadataField.FILENAME_METADATA_FIELD.ordinal()));
        if (fileNames.isPresent()) {
          assertTrue(fileNames.get().contains(entry.get(HoodieMetadataField.FILENAME_METADATA_FIELD.ordinal())));
        }
        assertFalse(entry.isNullAt(HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD.ordinal()));
      });

      // after trimming 2 of the meta fields, rest of the fields should match
      Dataset<Row> trimmedExpected = expectedRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
      Dataset<Row> trimmedActual = actualRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
      assertEquals(0, trimmedActual.except(trimmedExpected).count());
    } else { // operation = BULK_INSERT_APPEND_ONLY
      // all meta columns are untouched
      assertEquals(0, expectedRows.except(actualRows).count());
    }
  }
}
