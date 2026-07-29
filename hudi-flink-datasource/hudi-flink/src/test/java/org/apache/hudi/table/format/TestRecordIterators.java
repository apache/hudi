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

package org.apache.hudi.table.format;

import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.inline.InLineFSUtils;
import org.apache.hudi.table.format.cow.ParquetSplitReaderUtil;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.util.SerializationUtil;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.hadoop.ParquetInputFormat.FILTER_PREDICATE;
import static org.apache.parquet.hadoop.ParquetInputFormat.UNBOUND_RECORD_FILTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests configuration and path handling used by {@link RecordIterators}.
 */
class TestRecordIterators {

  @Test
  void testGetFileNameHandlesRegularAndInlinePaths() throws Exception {
    assertEquals(
        "file-id_1-0-1_001.parquet",
        invoke(
            "getFileName",
            new Class<?>[] {org.apache.flink.core.fs.Path.class},
            new org.apache.flink.core.fs.Path(
                "file:///table/file-id_1-0-1_001.parquet")));

    StoragePath outerPath = new StoragePath(
        "file:///table/region=us/file-id_1-0-1_001.parquet");
    StoragePath inlinePath = InLineFSUtils.getInlineFilePath(
        outerPath, "file", 10L, 20L);
    assertEquals(
        outerPath.getName(),
        invoke(
            "getFileName",
            new Class<?>[] {org.apache.flink.core.fs.Path.class},
            new org.apache.flink.core.fs.Path(inlinePath.toUri())));
  }

  @Test
  void testFilterPredicateConfiguration() throws Exception {
    Configuration conf = new Configuration();
    assertNull(invoke(
        "getFilterPredicate",
        new Class<?>[] {Configuration.class},
        conf));

    FilterPredicate expected = eq(intColumn("id"), 7);
    SerializationUtil.writeObjectToConfAsBase64(FILTER_PREDICATE, expected, conf);
    FilterPredicate actual = invoke(
        "getFilterPredicate",
        new Class<?>[] {Configuration.class},
        conf);
    assertEquals(expected.toString(), actual.toString());

    conf.set(FILTER_PREDICATE, "not-base64");
    InvocationTargetException exception = assertThrows(
        InvocationTargetException.class,
        () -> invoke(
            "getFilterPredicate",
            new Class<?>[] {Configuration.class},
            conf));
    assertTrue(exception.getCause() instanceof RuntimeException);
  }

  @Test
  void testUnboundRecordFilterConfiguration() throws Exception {
    Configuration conf = new Configuration();
    assertNull(invoke(
        "getUnboundRecordFilterInstance",
        new Class<?>[] {Configuration.class},
        conf));

    conf.setClass(
        UNBOUND_RECORD_FILTER,
        ConfigurableRecordFilter.class,
        UnboundRecordFilter.class);
    ConfigurableRecordFilter filter = invoke(
        "getUnboundRecordFilterInstance",
        new Class<?>[] {Configuration.class},
        conf);
    assertSame(conf, filter.getConf());

    conf.setClass(
        UNBOUND_RECORD_FILTER,
        InaccessibleRecordFilter.class,
        UnboundRecordFilter.class);
    InvocationTargetException exception = assertThrows(
        InvocationTargetException.class,
        () -> invoke(
            "getUnboundRecordFilterInstance",
            new Class<?>[] {Configuration.class},
            conf));
    assertTrue(exception.getCause() instanceof BadConfigurationException);
  }

  @Test
  void testGetPartitionSpecUsesOuterPathForInlineFiles() throws Exception {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(
        FlinkOptions.PARTITION_PATH_FIELD.key(),
        "region");
    hadoopConf.setBoolean(
        FlinkOptions.HIVE_STYLE_PARTITIONING.key(),
        true);
    StorageConfiguration<?> storageConf =
        new HadoopStorageConfiguration(hadoopConf);
    StoragePath outerPath = new StoragePath(
        "file:///table/region=us/file-id_1-0-1_001.parquet");
    StoragePath inlinePath = InLineFSUtils.getInlineFilePath(
        outerPath, "file", 10L, 20L);

    LinkedHashMap<String, Object> partitionSpec = invoke(
        "getPartitionSpec",
        new Class<?>[] {
            StorageConfiguration.class,
            StoragePath.class,
            java.util.List.class,
            java.util.List.class
        },
        storageConf,
        inlinePath,
        Collections.singletonList("region"),
        Collections.singletonList(DataTypes.STRING()));
    assertEquals(Collections.singletonMap("region", "us"), partitionSpec);
  }

  @Test
  void testParquetIteratorBuildsPredicateAndSchemaEvolutionReader() throws Exception {
    InternalSchema mergeSchema = new InternalSchema(Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get())));
    InternalSchemaManager schemaManager = mock(InternalSchemaManager.class);
    CastMap castMap = new CastMap();
    DataType[] fieldTypes = {DataTypes.INT()};
    castMap.setFileFieldTypes(fieldTypes);
    Predicate predicate = mock(Predicate.class);
    when(predicate.filter()).thenReturn(eq(intColumn("id"), 7));
    when(schemaManager.getMergeSchema(anyString())).thenReturn(mergeSchema);
    when(schemaManager.getCastMap(
        org.mockito.ArgumentMatchers.eq(mergeSchema),
        any(String[].class),
        any(DataType[].class),
        any(int[].class))).thenReturn(castMap);
    when(schemaManager.getMergeFieldNames(
        org.mockito.ArgumentMatchers.eq(mergeSchema),
        any(String[].class))).thenReturn(new String[] {"id"});
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenReturn(true);

    try (MockedStatic<ParquetSplitReaderUtil> mocked = mockStatic(ParquetSplitReaderUtil.class)) {
      mocked.when(() -> ParquetSplitReaderUtil.genPartColumnarRowReader(
          anyBoolean(), anyBoolean(), any(Configuration.class), any(String[].class),
          any(DataType[].class), anyMap(), any(int[].class), anyInt(), any(), anyLong(),
          anyLong(), any(), any())).thenReturn(reader);

      ClosableIterator<RowData> iterator = RecordIterators.getParquetRecordIterator(
          schemaManager,
          true,
          true,
          new Configuration(),
          new String[] {"id"},
          fieldTypes,
          Collections.emptyMap(),
          new int[] {0},
          16,
          new org.apache.flink.core.fs.Path("file:///table.parquet"),
          0L,
          1L,
          Collections.singletonList(predicate));
      assertFalse(iterator.hasNext());
      iterator.close();
      verify(reader).close();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T invoke(
      String methodName,
      Class<?>[] parameterTypes,
      Object... arguments) throws Exception {
    Method method = RecordIterators.class.getDeclaredMethod(methodName, parameterTypes);
    method.setAccessible(true);
    return (T) method.invoke(null, arguments);
  }

  public static class ConfigurableRecordFilter
      implements UnboundRecordFilter, Configurable {
    private Configuration conf;

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return () -> true;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }

  public static class InaccessibleRecordFilter implements UnboundRecordFilter {
    private InaccessibleRecordFilter() {
    }

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return () -> true;
    }
  }
}
