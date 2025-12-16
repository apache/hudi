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

package org.apache.hudi.utils;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.utils.BucketStreamWriteFunctionWrapper;
import org.apache.hudi.sink.utils.StreamWriteFunctionWrapper;
import org.apache.hudi.sink.utils.BulkInsertFunctionWrapper;
import org.apache.hudi.sink.utils.ConsistentBucketStreamWriteFunctionWrapper;
import org.apache.hudi.sink.utils.InsertFunctionWrapper;
import org.apache.hudi.sink.utils.TestFunctionWrapper;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataAvroQueryContexts;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertEquals;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_PROPERTIES_FILE;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data set for testing, also some utilities to check the results.
 */
public class TestData {
  public static List<RowData> DATA_SET_INSERT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_INSERT_DECIMAL_ORDERING = Arrays.asList(
      insertRow(TestConfigurations.ROW_TYPE_DECIMAL_ORDERING, StringData.fromString("id1"), StringData.fromString("Danny"),
          23, DecimalData.fromBigDecimal(BigDecimal.TEN, 10, 0), StringData.fromString("par1")),
      insertRow(TestConfigurations.ROW_TYPE_DECIMAL_ORDERING, StringData.fromString("id1"), StringData.fromString("Bob"),
          44, DecimalData.fromBigDecimal(BigDecimal.ONE, 10, 0), StringData.fromString("par2"))
  );

  public static List<RowData> DATA_SET_INSERT_PARTITION_IS_NULL = Arrays.asList(
      insertRow(StringData.fromString("idNull"), StringData.fromString("He"), 30,
          TimestampData.fromEpochMillis(9), null)
  );

  public static List<RowData> DATA_SET_UPDATE_INSERT = Arrays.asList(
      // advance the age by 1
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      // same with before
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      // new data
      insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_INSERT_SEPARATE_PARTITION = Arrays.asList(
      insertRow(StringData.fromString("id12"), StringData.fromString("Monica"), 27,
          TimestampData.fromEpochMillis(9), StringData.fromString("par5")),
      insertRow(StringData.fromString("id13"), StringData.fromString("Phoebe"), 31,
          TimestampData.fromEpochMillis(10), StringData.fromString("par5")),
      insertRow(StringData.fromString("id14"), StringData.fromString("Rachel"), 52,
          TimestampData.fromEpochMillis(11), StringData.fromString("par6")),
      insertRow(StringData.fromString("id15"), StringData.fromString("Ross"), 29,
          TimestampData.fromEpochMillis(12), StringData.fromString("par6"))
  );

  public static List<RowData> DATA_SET_INSERT_DUPLICATES = new ArrayList<>();

  static {
    IntStream.range(0, 5).forEach(i -> DATA_SET_INSERT_DUPLICATES.add(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1"))));
  }

  public static List<RowData> DATA_SET_INSERT_SAME_KEY = new ArrayList<>();

  static {
    IntStream.range(0, 5).forEach(i -> DATA_SET_INSERT_SAME_KEY.add(
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(i), StringData.fromString("par1"))));
  }

  public static List<RowData> DATA_SET_UPDATE_BEFORE = Arrays.asList(
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")));

  // data set of test_source.data
  public static List<RowData> DATA_SET_SOURCE_INSERT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // data set of test_source.data first commit.
  public static List<RowData> DATA_SET_SOURCE_INSERT_FIRST_COMMIT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2"))
  );

  // data set of test_source.data latest commit.
  public static List<RowData> DATA_SET_SOURCE_INSERT_LATEST_COMMIT = Arrays.asList(
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // merged data set of test_source.data and test_source_2.data
  public static List<RowData> DATA_SET_SOURCE_MERGED = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // changelog details of test_source.data and test_source_2.data
  public static List<RowData> DATA_SET_SOURCE_CHANGELOG = Arrays.asList(
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      updateAfterRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      updateBeforeRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      updateAfterRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      updateBeforeRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      updateAfterRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // data set of test_source.data with partition 'par1' overwrite
  public static List<RowData> DATA_SET_SOURCE_INSERT_OVERWRITE = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  // data set of test_source.data with partition 'par1' and 'par2' overwrite
  public static List<RowData> DATA_SET_SOURCE_INSERT_OVERWRITE_DYNAMIC_PARTITION = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1000), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2000), StringData.fromString("par2")),
      insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6000), StringData.fromString("par3")),
      insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7000), StringData.fromString("par4")),
      insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8000), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_UPDATE_DELETE = Arrays.asList(
      // this is update
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      // this is delete
      deleteRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      deleteRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      // delete a record that has no inserts
      deleteRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3"))
  );

  public static List<RowData> DATA_SET_INSERT_UPDATE_DELETE = Arrays.asList(
      // INSERT
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 19,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      // UPDATE
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 19,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 20,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 20,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 21,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 21,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(4), StringData.fromString("par1")),
      // DELETE
      deleteRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(5), StringData.fromString("par1"))
  );

  public static List<RowData> DATA_SET_SINGLE_INSERT = Collections.singletonList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")));

  public static List<RowData> DATA_SET_PART1 = Collections.singletonList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")));

  public static List<RowData> DATA_SET_PART2 = Collections.singletonList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par2")));

  public static List<RowData> DATA_SET_PART3 = Collections.singletonList(
      insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")));

  public static List<RowData> DATA_SET_PART4 = Collections.singletonList(
      insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")));

  public static List<RowData> DATA_SET_SINGLE_DELETE = Collections.singletonList(
      deleteRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(5), StringData.fromString("par1")));

  public static List<RowData> DATA_SET_DISORDER_INSERT = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(4), StringData.fromString("par1")),
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1"))
  );

  public static List<RowData> DATA_SET_DISORDER_INSERT_DELETE = Arrays.asList(
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(4), StringData.fromString("par1")),
      insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      deleteRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1"))
  );

  public static List<RowData> DATA_SET_DISORDER_UPDATE_DELETE = Arrays.asList(
      // DISORDER UPDATE
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 21,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 20,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 20,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      updateAfterRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(4), StringData.fromString("par1")),
      updateBeforeRow(StringData.fromString("id1"), StringData.fromString("Danny"), 21,
          TimestampData.fromEpochMillis(3), StringData.fromString("par1")),
      // DISORDER DELETE
      deleteRow(StringData.fromString("id1"), StringData.fromString("Danny"), 22,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1"))
  );

  // data types handled specifically for Hoodie Key
  public static List<RowData> DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE = new ArrayList<>();

  static {
    IntStream.range(1, 9).forEach(i -> DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE.add(
        insertRow(TestConfigurations.ROW_TYPE_HOODIE_KEY_SPECIAL_DATA_TYPE,
            TimestampData.fromEpochMillis(i), i, DecimalData.fromBigDecimal(new BigDecimal(String.format("%d.%d%d", i, i, i)), 3, 2))));
  }

  public static List<RowData> dataSetInsert(int... ids) {
    List<RowData> inserts = new ArrayList<>();
    Arrays.stream(ids).forEach(i -> inserts.add(
        insertRow(StringData.fromString("id" + i), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(i), StringData.fromString("par1"))));
    return inserts;
  }

  public static List<RowData> dataSetUpsert(int... ids) {
    List<RowData> inserts = new ArrayList<>();
    Arrays.stream(ids).forEach(i -> {
      inserts.add(
          updateBeforeRow(StringData.fromString("id" + i), StringData.fromString("Danny"), 23,
              TimestampData.fromEpochMillis(i), StringData.fromString("par1")));
      inserts.add(
          updateAfterRow(StringData.fromString("id" + i), StringData.fromString("Danny"), 23,
              TimestampData.fromEpochMillis(i), StringData.fromString("par1")));
    });
    return inserts;
  }

  /**
   * Updates the rows with given value {@code val} at field index {@code idx}.
   * All the target rows specified with range {@code targets} would be updated.
   *
   * <p>NOTE: only INT type is supported.
   *
   * @param dataset The rows to update
   * @param idx     The target field index
   * @param val     The new value
   * @param targets The target row numbers to update, the number starts from 0
   *
   * @return Copied rows with new values setup.
   */
  public static List<RowData> update(List<RowData> dataset, int idx, int val, int... targets) {
    List<RowData> copied = dataset.stream().map(TestConfigurations.SERIALIZER::copy).collect(Collectors.toList());
    Arrays.stream(targets).forEach(target -> {
      BinaryRowData rowData = (BinaryRowData) copied.get(target);
      rowData.setInt(idx, val);
    });
    return copied;
  }

  /**
   * Returns a copy of the given rows excluding the rows at indices {@code targets}.
   */
  public static List<RowData> delete(List<RowData> dataset, int... targets) {
    Set<Integer> exclude = Arrays.stream(targets).boxed().collect(Collectors.toSet());
    return IntStream.range(0, dataset.size())
        .filter(i -> !exclude.contains(i))
        .mapToObj(i -> TestConfigurations.SERIALIZER.copy(dataset.get(i))).collect(Collectors.toList());
  }

  public static List<RowData> filterOddRows(List<RowData> rows) {
    return filterRowsByIndexPredicate(rows, i -> i % 2 != 0);
  }

  public static List<RowData> filterEvenRows(List<RowData> rows) {
    return filterRowsByIndexPredicate(rows, i -> i % 2 == 0);
  }

  private static List<RowData> filterRowsByIndexPredicate(List<RowData> rows, Predicate<Integer> predicate) {
    List<RowData> filtered = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      if (predicate.test(i)) {
        filtered.add(rows.get(i));
      }
    }
    return filtered;
  }

  private static Integer toIdSafely(Object id) {
    if (id == null) {
      return -1;
    }
    final String idStr = id.toString();
    if (idStr.startsWith("id")) {
      return Integer.parseInt(idStr.substring(2));
    }
    return -1;
  }

  /**
   * Returns string format of a list of RowData.
   */
  public static String rowDataToString(List<RowData> rows) {
    return rowDataToString(rows, TestConfigurations.ROW_DATA_TYPE);
  }

  /**
   * Returns string format of a list of RowData.
   */
  public static String rowDataToString(List<RowData> rows, DataType rowType) {
    DataStructureConverter<Object, Object> converter =
        DataStructureConverters.getConverter(rowType);
    return rows.stream()
        .sorted(Comparator.comparing(o -> toIdSafely(o.getString(0))))
        .map(row -> converter.toExternal(row).toString())
        .collect(Collectors.toList()).toString();
  }

  /**
   * Write a list of row data with Hoodie format base on the given configuration.
   *
   * @param dataBuffer The data buffer to write
   * @param conf       The flink configuration
   * @throws Exception if error occurs
   */
  public static void writeData(
      List<RowData> dataBuffer,
      Configuration conf) throws Exception {
    final TestFunctionWrapper<RowData> funcWrapper = getWritePipeline(conf.get(FlinkOptions.PATH), conf);
    funcWrapper.openFunction();

    for (RowData rowData : dataBuffer) {
      funcWrapper.invoke(rowData);
    }

    // this triggers the data write and event send
    funcWrapper.checkpointFunction(1);

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);
    funcWrapper.checkpointComplete(1);

    funcWrapper.close();
  }

  /**
   * Write a list of row data with Hoodie format base on the given configuration.
   *
   * <p>The difference with {@link #writeData} is that it flush data using #endInput, and it
   * does not generate inflight instant.
   *
   * @param dataBuffer The data buffer to write
   * @param conf       The flink configuration
   * @throws Exception if error occurs
   */
  public static void writeDataAsBatch(
      List<RowData> dataBuffer,
      Configuration conf) throws Exception {
    TestFunctionWrapper<RowData> funcWrapper = getWritePipeline(conf.get(FlinkOptions.PATH), conf);
    funcWrapper.openFunction();

    for (RowData rowData : dataBuffer) {
      funcWrapper.invoke(rowData);
    }

    // this triggers the data write and event send
    funcWrapper.endInput();

    final OperatorEvent nextEvent = funcWrapper.getNextEvent();
    funcWrapper.getCoordinator().handleEventFromOperator(0, nextEvent);

    funcWrapper.inlineCompaction();

    funcWrapper.close();
  }

  /**
   * Initializes a writing pipeline with given configuration.
   */
  public static TestFunctionWrapper<RowData> getWritePipeline(String basePath, Configuration conf) throws Exception {
    if (OptionsResolver.isBulkInsertOperation(conf)) {
      return new BulkInsertFunctionWrapper<>(basePath, conf);
    } else if (OptionsResolver.isAppendMode(conf)) {
      return new InsertFunctionWrapper<>(basePath, conf);
    } else if (OptionsResolver.isBucketIndexType(conf)) {
      if (OptionsResolver.isConsistentHashingBucketIndexType(conf)) {
        return new ConsistentBucketStreamWriteFunctionWrapper<>(basePath, conf);
      } else {
        return new BucketStreamWriteFunctionWrapper<>(basePath, conf);
      }
    } else {
      return new StreamWriteFunctionWrapper<>(basePath, conf);
    }
  }

  private static String toStringSafely(Object obj) {
    return obj == null ? "null" : obj.toString();
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   */
  public static void assertRowsEquals(List<Row> rows, String expected) {
    assertRowsEquals(rows, expected, false);
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows           Actual result rows
   * @param expected       Expected string of the sorted rows
   * @param withChangeFlag Whether compares with change flags
   */
  public static void assertRowsEquals(List<Row> rows, String expected, boolean withChangeFlag) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toStringSafely(o.getField(0))))
        .map(row -> {
          final String rowStr = row.toString();
          if (withChangeFlag) {
            return row.getKind().shortString() + "(" + rowStr + ")";
          } else {
            return rowStr;
          }
        })
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(expected));
  }

  /**
   * Sort the {@code rows} using field at index {@code orderingPos} and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows        Actual result rows
   * @param expected    Expected string of the sorted rows
   * @param orderingPos Field position for ordering
   */
  public static void assertRowsEquals(List<Row> rows, String expected, int orderingPos) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toStringSafely(o.getField(orderingPos))))
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(expected));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected row data list {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected row data list
   */
  public static void assertRowsEquals(List<Row> rows, List<RowData> expected) {
    String rowsString = rows.stream()
        .sorted(Comparator.comparing(o -> toIdSafely(o.getField(0))))
        .collect(Collectors.toList()).toString();
    assertThat(rowsString, is(rowDataToString(expected)));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected string {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected string of the sorted rows
   */
  public static void assertRowDataEquals(List<RowData> rows, String expected) {
    String rowsString = rowDataToString(rows);
    assertThat(rowsString, is(expected));
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected row data list {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected row data list
   */
  public static void assertRowDataEquals(List<RowData> rows, List<RowData> expected) {
    assertRowDataEquals(rows, expected, TestConfigurations.ROW_DATA_TYPE);
  }

  /**
   * Sort the {@code rows} using field at index 0 and asserts
   * it equals with the expected row data list {@code expected}.
   *
   * @param rows     Actual result rows
   * @param expected Expected row data list
   * @param rowType DataType for record
   */
  public static void assertRowDataEquals(List<RowData> rows, List<RowData> expected, DataType rowType) {
    String rowsString = rowDataToString(rows, rowType);
    assertThat(rowsString, is(rowDataToString(expected, rowType)));
  }

  /**
   * Assert that expected and actual collection of rows are equal regardless of the order.
   *
   * @param expected expected row collection
   * @param actual actual row collection
   */
  public static void assertRowsEqualsUnordered(Collection<Row> expected, Collection<Row> actual) {
    assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  /**
   * Checks the source data set are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile The file base to check, should be a directory
   * @param expected The expected results mapping, the key should be the partition path
   *                 and value should be values list with the key partition
   */
  public static void checkWrittenData(File baseFile, Map<String, String> expected) throws IOException {
    checkWrittenData(baseFile, expected, 4);
  }

  /**
   * Checks the source data set are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   *                   and value should be values list with the key partition
   * @param partitions The expected partition number
   */
  public static void checkWrittenData(
      File baseFile,
      Map<String, String> expected,
      int partitions) throws IOException {
    checkWrittenData(baseFile, expected, partitions, TestData::filterOutVariables);
  }

  /**
   * Checks the source data set are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   *                   and value should be values list with the key partition
   * @param partitions The expected partition number
   * @param extractor  The fields extractor
   */
  public static void checkWrittenData(
      File baseFile,
      Map<String, String> expected,
      int partitions,
      Function<GenericRecord, String> extractor) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);
    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(new Path(latestDataFile.getAbsolutePath())).build();
      List<String> readBuffer = new ArrayList<>();
      GenericRecord nextRecord = reader.read();
      while (nextRecord != null) {
        readBuffer.add(extractor.apply(nextRecord));
        nextRecord = reader.read();
      }
      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  /**
   * Checks the source data set are written as expected.
   * Different with {@link #checkWrittenData}, it reads all the data files.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   *                   and value should be values list with the key partition
   * @param partitions The expected partition number
   */
  public static void checkWrittenAllData(
      File baseFile,
      Map<String, String> expected,
      int partitions) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);

    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));

    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);

      List<String> readBuffer = new ArrayList<>();
      for (File dataFile : dataFiles) {
        ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(new Path(dataFile.getAbsolutePath())).build();
        GenericRecord nextRecord = reader.read();
        while (nextRecord != null) {
          readBuffer.add(filterOutVariables(nextRecord));
          nextRecord = reader.read();
        }
      }

      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  public static List<GenericRecord> readAllData(
      File baseFile, RowType rowType, int partitions) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);

    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));

    List<GenericRecord> result = new ArrayList<>();
    AvroToRowDataConverters.AvroToRowDataConverter converter =
        AvroToRowDataConverters.createConverter(rowType, true);

    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);

      for (File dataFile : dataFiles) {
        ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(new Path(dataFile.getAbsolutePath())).build();

        GenericRecord nextRecord = reader.read();
        while (nextRecord != null) {
          result.add(nextRecord);
          nextRecord = reader.read();
        }
      }
    }

    return result;
  }

  /**
   * Checks the source data are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param basePath The file base to check, should be a directory
   * @param expected The expected results mapping, the key should be the partition path
   */
  public static void checkWrittenDataCOW(
      File basePath,
      Map<String, List<String>> expected) throws IOException {
    checkWrittenDataCOW(basePath, expected, TestData::filterOutVariables);
  }

  /**
   * Checks the source data are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param basePath The file base to check, should be a directory
   * @param expected The expected results mapping, the key should be the partition path
   * @param extractor The extractor to extract the required fields from the avro row
   */
  public static void checkWrittenDataCOW(
      File basePath,
      Map<String, List<String>> expected,
      Function<GenericRecord, String> extractor) throws IOException {

    // 1. init flink table
    HoodieTableMetaClient metaClient = createMetaClient(basePath.toURI().toString());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath.toURI().toString()).build();
    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, HoodieFlinkEngineContext.DEFAULT, metaClient);

    // 2. check each partition data
    expected.forEach((partition, partitionDataSet) -> {

      List<String> readBuffer = new ArrayList<>();

      table.getBaseFileOnlyView().getLatestBaseFiles(partition)
          .forEach(baseFile -> {
            String path = baseFile.getPath();
            try {
              ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path)).build();
              GenericRecord nextRecord = reader.read();
              while (nextRecord != null) {
                readBuffer.add(extractor.apply(nextRecord));
                nextRecord = reader.read();
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      assertThat("Unexpected records number under partition: " + partition,
          readBuffer.size(), is(partitionDataSet.size()));
      for (String record : readBuffer) {
        assertTrue(partitionDataSet.contains(record), "Unexpected record: " + record);
      }
    });

  }

  /**
   * Checks the MERGE_ON_READ source data are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile   The file base to check, should be a directory
   * @param expected   The expected results mapping, the key should be the partition path
   * @param partitions The expected partition number
   */
  public static void checkWrittenDataMOR(
      File baseFile,
      Map<String, String> expected,
      int partitions) throws Exception {
    assert baseFile.isDirectory() : "Base path should be a directory";
    String basePath = baseFile.getAbsolutePath();
    File hoodiePropertiesFile = new File(baseFile + "/" + METAFOLDER_NAME + "/" + HOODIE_PROPERTIES_FILE);
    assert hoodiePropertiesFile.exists();
    // 1. init flink table
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .fromFile(hoodiePropertiesFile)
        .withMemoryConfig(
            HoodieMemoryConfig.newBuilder()
                .withMaxMemoryMaxSize(
                    FlinkOptions.WRITE_MERGE_MAX_MEMORY.defaultValue() * 1024 * 1024L,
                    FlinkOptions.COMPACTION_MAX_MEMORY.defaultValue() * 1024 * 1024L)
                .build())
        .withPath(basePath)
        .build();
    // deal with partial update merger
    if (config.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME).contains(PartialUpdateAvroPayload.class.getSimpleName())
        || (config.getString(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID) != null
        && config.getString(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID).equalsIgnoreCase(HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID))) {
      config.setValue(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), PartialUpdateFlinkRecordMerger.class.getName());
    }

    HoodieTableMetaClient metaClient = createMetaClient(basePath);
    HoodieFlinkTable<?> table = HoodieFlinkTable.create(config, HoodieFlinkEngineContext.DEFAULT, metaClient);
    Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(schema);

    String latestInstant = metaClient.getActiveTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::requestedTime).orElse(null);
    assertNotNull(latestInstant, "No completed commit under table path" + basePath);

    File[] partitionDirs = baseFile.listFiles(file -> !file.getName().startsWith(".") && file.isDirectory());
    assertNotNull(partitionDirs);
    assertThat("The partitions number should be: " + partitions, partitionDirs.length, is(partitions));

    // 2. check each partition data
    for (File partitionDir : partitionDirs) {
      List<String> readBuffer = new ArrayList<>();
      List<FileSlice> fileSlices = table.getSliceView().getLatestMergedFileSlicesBeforeOrOn(partitionDir.getName(), latestInstant).collect(Collectors.toList());
      for (FileSlice fileSlice : fileSlices) {
        try (ClosableIterator<RowData> rowIterator = getRecordIterator(fileSlice, hoodieSchema, metaClient, config)) {
          while (rowIterator.hasNext()) {
            RowData rowData = rowIterator.next();
            readBuffer.add(filterOutVariables(schema, rowData));
          }
        }
      }
      // Ensure that to write and read sequences are consistent.
      readBuffer.sort(String::compareTo);
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  /**
   * Filter out the variables like _hoodie_record_key and _hoodie_partition_path.
   *
   * @param genericRecord data record
   * @return field values joined with comma
   */
  public static String filterOutVariablesWithoutHudiMetadata(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(genericRecord, "uuid"));
    fields.add(getFieldValue(genericRecord, "name"));
    fields.add(getFieldValue(genericRecord, "age"));
    fields.add(TimestampData.fromEpochMillis((long) genericRecord.get("ts")).toTimestamp().toString());
    fields.add(genericRecord.get("partition").toString());
    return String.join(",", fields);
  }

  private static ClosableIterator<RowData> getRecordIterator(
      FileSlice fileSlice,
      HoodieSchema tableSchema,
      HoodieTableMetaClient metaClient,
      HoodieWriteConfig writeConfig) throws IOException {
    HoodieFileGroupReader<RowData> fileGroupReader =
        FormatUtils.createFileGroupReader(metaClient, writeConfig, InternalSchemaManager.DISABLED, fileSlice, tableSchema, tableSchema,
            fileSlice.getLatestInstantTime(), FlinkOptions.REALTIME_PAYLOAD_COMBINE, false, Collections.emptyList(), Option.empty());
    return fileGroupReader.getClosableIterator();
  }

  /**
   * Filter out the variables like file name.
   */
  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(genericRecord, "_hoodie_record_key"));
    fields.add(getFieldValue(genericRecord, "_hoodie_partition_path"));
    fields.add(getFieldValue(genericRecord, "uuid"));
    fields.add(getFieldValue(genericRecord, "name"));
    fields.add(getFieldValue(genericRecord, "age"));
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("partition").toString());
    return String.join(",", fields);
  }

  private static String filterOutVariables(Schema schema, RowData record) {
    RowDataAvroQueryContexts.RowDataQueryContext queryContext = RowDataAvroQueryContexts.fromAvroSchema(schema);
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(queryContext, record, "_hoodie_record_key"));
    fields.add(getFieldValue(queryContext, record, "_hoodie_partition_path"));
    fields.add(getFieldValue(queryContext, record, "uuid"));
    fields.add(getFieldValue(queryContext, record, "name"));
    fields.add(getFieldValue(queryContext, record, "age"));
    fields.add(getFieldValue(queryContext, record, "ts"));
    fields.add(getFieldValue(queryContext, record, "partition"));
    return String.join(",", fields);
  }

  private static String getFieldValue(RowDataAvroQueryContexts.RowDataQueryContext queryContext, RowData rowData, String fieldName) {
    return String.valueOf(queryContext.getFieldQueryContext(fieldName).getValAsJava(rowData, true));
  }

  private static String getFieldValue(GenericRecord genericRecord, String fieldName) {
    if (genericRecord.get(fieldName) != null) {
      return genericRecord.get(fieldName).toString();
    } else {
      return null;
    }
  }

  public static BinaryRowData insertRow(Object... fields) {
    return insertRow(TestConfigurations.ROW_TYPE, fields);
  }

  public static BinaryRowData insertRow(RowType rowType, Object... fields) {
    LogicalType[] types = rowType.getFields().stream().map(RowType.RowField::getType)
        .toArray(LogicalType[]::new);
    assertEquals(
        "Filed count inconsistent with type information",
        fields.length,
        types.length);
    BinaryRowData row = new BinaryRowData(fields.length);
    BinaryRowWriter writer = new BinaryRowWriter(row);
    writer.reset();
    for (int i = 0; i < fields.length; i++) {
      Object field = fields[i];
      if (field == null) {
        writer.setNullAt(i);
      } else {
        BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
      }
    }
    writer.complete();
    return row;
  }

  private static BinaryRowData deleteRow(Object... fields) {
    BinaryRowData rowData = insertRow(fields);
    rowData.setRowKind(RowKind.DELETE);
    return rowData;
  }

  private static BinaryRowData updateBeforeRow(Object... fields) {
    BinaryRowData rowData = insertRow(fields);
    rowData.setRowKind(RowKind.UPDATE_BEFORE);
    return rowData;
  }

  private static BinaryRowData updateAfterRow(Object... fields) {
    BinaryRowData rowData = insertRow(fields);
    rowData.setRowKind(RowKind.UPDATE_AFTER);
    return rowData;
  }

  /**
   * Creates row with specified field values.
   * <p>This method is used to define row in convenient way such as:
   *
   * <pre> {@code
   * row(1, array("abc1", "def1"), map("abc1", 1, "def1", 3), row(1, "abc1"))
   * }</pre>
   */
  public static Row row(Object... values) {
    return Row.of(values);
  }

  /**
   * Creates array with specified values.
   * <p>This method is used to define row in convenient way such as:
   *
   * <pre> {@code
   * row(1, array("abc1", "def1"), map("abc1", 1, "def1", 3), row(1, "abc1"))
   * }</pre>
   */
  @SafeVarargs
  public static <T> T[] array(T... values) {
    return values;
  }

  /**
   * Creates map with specified keys and values.
   * <p>This method is used to define row in convenient way such as:
   *
   * <pre> {@code
   * row(1, array("abc1", "def1"), map("abc1", 1, "def1", 3), row(1, "abc1"))
   * }</pre>
   *
   * <p>NOTE: be careful of the order of keys and values. If you make
   * a mistake, {@link ClassCastException} will occur later than the
   * creation of the map due to type erasure.
   */
  public static <K, V> Map<K, V> map(Object... kwargs) {
    HashMap<Object, Object> map = new HashMap<>();
    for (int i = 0; i < kwargs.length; i += 2) {
      map.put(kwargs[i], kwargs[i + 1]);
    }
    @SuppressWarnings("unchecked")
    Map<K, V> resultMap = (Map<K, V>) map;
    return resultMap;
  }
}
