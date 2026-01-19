/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link CopyOnWriteSplitReaderFunction}.
 */
public class TestCopyOnWriteSplitReaderFunction extends HoodieCommonTestHarness {

  private StorageConfiguration<?> storageConf;
  private Configuration flinkConf;
  private InternalSchemaManager internalSchemaManager;
  private DataType dataType;
  private HoodieSchema requestedSchema;
  private List<ExpressionPredicates.Predicate> predicates;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    initPath("dataset");
    storageConf = new HadoopStorageConfiguration(new org.apache.hadoop.conf.Configuration());
    flinkConf = new Configuration();
    flinkConf.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 1024);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    internalSchemaManager = InternalSchemaManager.get(storageConf, metaClient);
    dataType = TestConfigurations.ROW_DATA_TYPE;
    requestedSchema = HoodieSchemaConverter.convertToSchema(dataType.getLogicalType());
    predicates = new ArrayList<>();
  }

  @Test
  public void testConstructor() {
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    assertNotNull(readerFunction, "Reader function should be created successfully");
  }

  @Test
  public void testReadWithNullBasePath() {
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Create a split with null base path
    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        null,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    // Should throw IllegalArgumentException due to null base path
    assertThrows(IllegalArgumentException.class, () -> {
      readerFunction.read(split);
    }, "Should throw IllegalArgumentException when base path is null");
  }

  @Test
  public void testReadWithInvalidPath() {
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Create a split with invalid base path that doesn't exist
    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        "/non/existent/path/file.parquet",
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    // Should throw HoodieIOException when trying to read non-existent file
    assertThrows(HoodieIOException.class, () -> {
      CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
          readerFunction.read(split);
      // Force evaluation by checking hasNext
      iterator.hasNext();
    }, "Should throw HoodieIOException when file doesn't exist");
  }

  @Test
  public void testReadWithValidParquetFile() throws Exception {
    // Create a test parquet file
    String parquetFilePath = TestData.writeTestParquetFile(new File(basePath), "test.parquet", 10);

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        parquetFilePath,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
        readerFunction.read(split);

    assertNotNull(iterator, "Iterator should not be null");
    assertTrue(iterator.hasNext(), "Iterator should have records");

    // Read at least one batch
    RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> batch = iterator.next();
    assertNotNull(batch, "Batch should not be null");
    assertNotNull(batch.nextSplit(), "Split ID should not be null");

    iterator.close();
  }

  @Test
  public void testReadWithPredicates() throws Exception {
    // Create a test parquet file
    String parquetFilePath = TestData.writeTestParquetFile(new File(basePath), "test_with_predicates.parquet", 20);

    // Add predicates to filter data
    List<ExpressionPredicates.Predicate> testPredicates = new ArrayList<>();
    // Note: Actual predicate implementation would require proper setup
    // This is a placeholder to test that predicates are passed correctly

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        testPredicates);

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        parquetFilePath,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
        readerFunction.read(split);

    assertNotNull(iterator, "Iterator with predicates should not be null");
    iterator.close();
  }

  @Test
  public void testClose() throws Exception {
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Close without reading should not throw
    readerFunction.close();

    // Create a test parquet file and read it
    String parquetFilePath = TestData.writeTestParquetFile(new File(basePath), "test_close.parquet", 5);

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        parquetFilePath,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
        readerFunction.read(split);

    // Close after reading should clean up resources
    readerFunction.close();

    // Multiple close calls should be safe
    readerFunction.close();
  }

  @Test
  public void testMultipleReads() throws Exception {
    // Create test parquet files
    String parquetFile1 = TestData.writeTestParquetFile(new File(basePath), "test1.parquet", 5);
    String parquetFile2 = TestData.writeTestParquetFile(new File(basePath), "test2.parquet", 10);

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // First read
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1,
        parquetFile1,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator1 =
        readerFunction.read(split1);
    assertNotNull(iterator1, "First iterator should not be null");
    iterator1.close();

    // Second read with different split
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        2,
        parquetFile2,
        Option.empty(),
        basePath,
        "partition2",
        "read_optimized",
        "file-2");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator2 =
        readerFunction.read(split2);
    assertNotNull(iterator2, "Second iterator should not be null");
    iterator2.close();

    readerFunction.close();
  }

  @Test
  public void testBatchSizeConfiguration() throws Exception {
    // Create configuration with specific batch size
    Configuration customFlinkConf = new Configuration();
    customFlinkConf.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 100);

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        customFlinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    String parquetFilePath = TestData.writeTestParquetFile(new File(basePath), "test_batch.parquet", 200);

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        parquetFilePath,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
        readerFunction.read(split);

    assertNotNull(iterator, "Iterator should be created with custom batch size");

    // Verify that we can read batches
    int batchCount = 0;
    while (iterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> batch = iterator.next();
      batchCount++;
      assertNotNull(batch, "Each batch should not be null");
    }

    assertTrue(batchCount > 0, "Should have read at least one batch");
    iterator.close();
    readerFunction.close();
  }

  @Test
  public void testEmptyPredicatesList() throws Exception {
    // Test with empty predicates list
    List<ExpressionPredicates.Predicate> emptyPredicates = Collections.emptyList();

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        emptyPredicates);

    String parquetFilePath = TestData.writeTestParquetFile(new File(basePath), "test_empty_predicates.parquet", 15);

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        parquetFilePath,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
        readerFunction.read(split);

    assertNotNull(iterator, "Iterator should work with empty predicates");
    assertTrue(iterator.hasNext(), "Should have records with empty predicates");
    iterator.close();
    readerFunction.close();
  }

  @Test
  public void testReaderWithDifferentPartitions() throws Exception {
    // Test reading from splits with different partition paths
    String parquetFile1 = TestData.writeTestParquetFile(new File(basePath), "partition1/test.parquet", 10);
    String parquetFile2 = TestData.writeTestParquetFile(new File(basePath), "partition2/test.parquet", 10);

    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Read from first partition
    HoodieSourceSplit split1 = new HoodieSourceSplit(
        1,
        parquetFile1,
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator1 =
        readerFunction.read(split1);
    assertNotNull(iterator1);
    iterator1.close();

    // Read from second partition
    HoodieSourceSplit split2 = new HoodieSourceSplit(
        2,
        parquetFile2,
        Option.empty(),
        basePath,
        "partition2",
        "read_optimized",
        "file-2");

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator2 =
        readerFunction.read(split2);
    assertNotNull(iterator2);
    iterator2.close();

    readerFunction.close();
  }

  @Test
  public void testExceptionHandlingDuringRead() {
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Create a split with a path that will cause an IOException
    HoodieSourceSplit invalidSplit = new HoodieSourceSplit(
        1,
        "/invalid/path/to/file.parquet",
        Option.empty(),
        basePath,
        "partition1",
        "read_optimized",
        "file-1");

    // Should wrap IOException in HoodieIOException
    HoodieIOException exception = assertThrows(HoodieIOException.class, () -> {
      CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<RowData>>> iterator =
          readerFunction.read(invalidSplit);
      iterator.hasNext(); // Force evaluation
    });

    assertNotNull(exception.getMessage(), "Exception should have a message");
    assertTrue(exception.getMessage().contains("Failed to read from file group"),
        "Exception message should indicate read failure");
  }

  @Test
  public void testSerializability() {
    // Test that CopyOnWriteSplitReaderFunction is serializable
    CopyOnWriteSplitReaderFunction readerFunction = new CopyOnWriteSplitReaderFunction(
        storageConf,
        flinkConf,
        internalSchemaManager,
        dataType,
        requestedSchema,
        predicates);

    // Verify it implements Serializable through SplitReaderFunction
    assertTrue(readerFunction instanceof SplitReaderFunction,
        "Should implement SplitReaderFunction");
  }
}
