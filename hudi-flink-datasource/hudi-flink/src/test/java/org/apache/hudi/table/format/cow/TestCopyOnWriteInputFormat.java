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

package org.apache.hudi.table.format.cow;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.row.HoodieRowDataLanceWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests split planning and iterator lifecycle in {@link CopyOnWriteInputFormat}.
 */
class TestCopyOnWriteInputFormat {

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  void testCreateInputSplitsFiltersHiddenFilesAndReadsNestedDirectories() throws IOException {
    java.nio.file.Path visible = tempDir.resolve("visible.parquet");
    java.nio.file.Path empty = tempDir.resolve("empty.parquet");
    java.nio.file.Path hidden = tempDir.resolve("_hidden.parquet");
    java.nio.file.Path nested = tempDir.resolve("nested/nested.parquet");
    Files.write(visible, new byte[32]);
    Files.createFile(empty);
    Files.write(hidden, new byte[8]);
    Files.createDirectories(nested.getParent());
    Files.write(nested, new byte[16]);

    CopyOnWriteInputFormat inputFormat = inputFormat(
        new Path[] {new Path(tempDir.toUri())}, Long.MAX_VALUE);
    inputFormat.setNestedFileEnumeration(true);

    assertThrows(IllegalArgumentException.class, () -> inputFormat.createInputSplits(0));
    FileInputSplit[] splits = inputFormat.createInputSplits(2);
    assertTrue(splits.length >= 3);
    assertTrue(inputFormat.supportsMultiPaths());
    assertTrue(java.util.Arrays.stream(splits)
        .anyMatch(split -> split.getPath().getName().equals("visible.parquet")));
    assertTrue(java.util.Arrays.stream(splits)
        .anyMatch(split -> split.getPath().getName().equals("empty.parquet")));
    assertTrue(java.util.Arrays.stream(splits)
        .anyMatch(split -> split.getPath().getName().equals("nested.parquet")));
    assertFalse(java.util.Arrays.stream(splits)
        .anyMatch(split -> split.getPath().getName().equals("_hidden.parquet")));
  }

  @Test
  void testUnsplittableLanceFileUsesWholeFileSplit() throws IOException {
    java.nio.file.Path lance = tempDir.resolve("data.lance");
    Files.write(lance, new byte[32]);
    CopyOnWriteInputFormat inputFormat = inputFormat(
        new Path[] {new Path(lance.toUri())}, Long.MAX_VALUE);

    FileInputSplit[] splits = inputFormat.createInputSplits(8);
    assertEquals(1, splits.length);
    assertEquals(-1L, splits[0].getLength());
  }

  @Test
  void testLanceVectorSchemaMismatch() throws Exception {
    RowType rowType = RowType.of(false,
        new LogicalType[] {new ArrayType(false, new FloatType(false))}, new String[] {"embedding"});
    HoodieSchema fileSchema = HoodieSchemaConverter.convertToSchema(
        rowType, "file_record", "embedding:2");
    HoodieSchema tableSchema = HoodieSchemaConverter.convertToSchema(
        rowType, "table_record", "embedding:3");
    StoragePath storagePath = new StoragePath(tempDir.resolve("vector.lance").toUri());

    try (HoodieRowDataLanceWriter writer = new HoodieRowDataLanceWriter(
        storagePath,
        fileSchema,
        "001",
        mock(TaskContextSupplier.class),
        Option.empty(),
        128 * 1024 * 1024L,
        64 * 1024 * 1024L,
        16 * 1024 * 1024L,
        true,
        false,
        false)) {
      writer.writeRow("key1", GenericRowData.of(
          new GenericArrayData(new Object[] {1.0F, 2.0F})));
    }

    DataType rowDataType = HoodieSchemaConverter.convertToDataType(tableSchema);
    CopyOnWriteInputFormat inputFormat = new CopyOnWriteInputFormat(
        new Path[] {new Path(storagePath.toUri())},
        new String[] {"embedding"},
        rowDataType.getChildren().toArray(new DataType[0]),
        new int[] {0},
        FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue(),
        FlinkOptions.PARTITION_PATH_FIELD.defaultValue(),
        false,
        Collections.emptyList(),
        Long.MAX_VALUE,
        new org.apache.hadoop.conf.Configuration(),
        true,
        InternalSchemaManager.DISABLED,
        tableSchema);

    HoodieException exception = assertThrows(
        HoodieException.class,
        () -> inputFormat.open(new FileInputSplit(
            0, new Path(storagePath.toUri()), 0, -1, new String[0])));
    assertTrue(exception.getCause() instanceof HoodieValidationException);
    assertTrue(exception.getCause().getMessage().contains("requested VECTOR(3)"));
    assertTrue(exception.getCause().getMessage().contains("file contains VECTOR(2)"));
  }

  @Test
  void testAcceptFileUsesBuiltInAndCustomFilters() {
    CopyOnWriteInputFormat inputFormat = inputFormat(
        new Path[] {new Path(tempDir.toUri())}, Long.MAX_VALUE);
    assertFalse(inputFormat.acceptFile(fileStatus("_metadata")));
    assertFalse(inputFormat.acceptFile(fileStatus(".hidden")));
    assertTrue(inputFormat.acceptFile(fileStatus("data.parquet")));

    inputFormat.setFilesFilter(new FilePathFilter() {
      @Override
      public boolean filterPath(Path filePath) {
        return filePath.getName().endsWith(".skip");
      }
    });
    assertFalse(inputFormat.acceptFile(fileStatus("data.skip")));
    assertTrue(inputFormat.acceptFile(fileStatus("data.parquet")));
  }

  @Test
  void testLimitAndIteratorLifecycle() throws Exception {
    CopyOnWriteInputFormat inputFormat = inputFormat(
        new Path[] {new Path(tempDir.toUri())}, 1L);
    ClosableIterator<RowData> iterator = mockIterator();
    GenericRowData row = GenericRowData.of(StringData.fromString("value"));
    when(iterator.hasNext()).thenReturn(true);
    when(iterator.next()).thenReturn(row);
    setIterator(inputFormat, iterator);

    assertFalse(inputFormat.reachedEnd());
    assertSame(row, inputFormat.nextRecord(null));
    assertTrue(inputFormat.reachedEnd());
    verify(iterator).hasNext();

    inputFormat.close();
    verify(iterator).close();
    inputFormat.close();
  }

  @Test
  void testReachedEndDelegatesWhenLimitIsNotReached() throws Exception {
    CopyOnWriteInputFormat inputFormat = inputFormat(
        new Path[] {new Path(tempDir.toUri())}, Long.MAX_VALUE);
    ClosableIterator<RowData> iterator = mockIterator();
    when(iterator.hasNext()).thenReturn(false);
    setIterator(inputFormat, iterator);

    assertTrue(inputFormat.reachedEnd());
    verify(iterator).hasNext();
    verify(iterator, never()).next();
  }

  private static CopyOnWriteInputFormat inputFormat(Path[] paths, long limit) {
    List<String> fieldNames = TestConfigurations.ROW_TYPE.getFieldNames();
    List<DataType> fieldTypes = TestConfigurations.ROW_DATA_TYPE.getChildren();
    return new CopyOnWriteInputFormat(
        paths,
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        IntStream.range(0, fieldNames.size()).toArray(),
        FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue(),
        FlinkOptions.PARTITION_PATH_FIELD.defaultValue(),
        false,
        Collections.emptyList(),
        limit,
        new org.apache.hadoop.conf.Configuration(),
        true,
        InternalSchemaManager.DISABLED,
        HoodieSchemaConverter.convertToSchema(
            TestConfigurations.ROW_TYPE.copy()));
  }

  private static FileStatus fileStatus(String name) {
    return new FileStatus(
        1L,
        false,
        1,
        1L,
        0L,
        new org.apache.hadoop.fs.Path("/table/" + name));
  }

  private static void setIterator(
      CopyOnWriteInputFormat inputFormat,
      ClosableIterator<RowData> iterator) throws ReflectiveOperationException {
    Field field = CopyOnWriteInputFormat.class.getDeclaredField("itr");
    field.setAccessible(true);
    field.set(inputFormat, iterator);
  }

  @SuppressWarnings("unchecked")
  private static ClosableIterator<RowData> mockIterator() {
    return mock(ClosableIterator.class);
  }
}
