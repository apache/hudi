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

package org.apache.hudi.operator.utils;

import org.apache.hudi.common.fs.FSUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Data set for testing, also some utilities to check the results. */
public class TestData {
  public static List<RowData> DATA_SET_ONE = Arrays.asList(
      binaryRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      binaryRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      binaryRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      binaryRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      binaryRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      binaryRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      binaryRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      binaryRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  public static List<RowData> DATA_SET_TWO = Arrays.asList(
      // advance the age by 1
      binaryRow(StringData.fromString("id1"), StringData.fromString("Danny"), 24,
          TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
      binaryRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 34,
          TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
      binaryRow(StringData.fromString("id3"), StringData.fromString("Julian"), 54,
          TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
      binaryRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 32,
          TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
      // same with before
      binaryRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
          TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
      // new data
      binaryRow(StringData.fromString("id9"), StringData.fromString("Jane"), 19,
          TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
      binaryRow(StringData.fromString("id10"), StringData.fromString("Ella"), 38,
          TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
      binaryRow(StringData.fromString("id11"), StringData.fromString("Phoebe"), 52,
          TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
  );

  /**
   * Checks the source data TestConfigurations.DATA_SET_ONE are written as expected.
   *
   * <p>Note: Replace it with the Flink reader when it is supported.
   *
   * @param baseFile The file base to check, should be a directly
   * @param expected The expected results mapping, the key should be the partition path
   */
  public static void checkWrittenData(File baseFile, Map<String, String> expected) throws IOException {
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);
    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(4));
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(file -> file.getName().endsWith(".parquet"));
      assertNotNull(dataFiles);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(new Path(latestDataFile.getAbsolutePath())).build();
      List<String> readBuffer = new ArrayList<>();
      GenericRecord nextRecord = reader.read();
      while (nextRecord != null) {
        readBuffer.add(filterOutVariables(nextRecord));
        nextRecord = reader.read();
      }
      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  /**
   * Filter out the variables like file name.
   */
  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(genericRecord.get("_hoodie_record_key").toString());
    fields.add(genericRecord.get("_hoodie_partition_path").toString());
    fields.add(genericRecord.get("uuid").toString());
    fields.add(genericRecord.get("name").toString());
    fields.add(genericRecord.get("age").toString());
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("partition").toString());
    return Strings.join(fields, ",");
  }

  private static BinaryRowData binaryRow(Object... fields) {
    LogicalType[] types = TestConfigurations.ROW_TYPE.getFields().stream().map(RowType.RowField::getType)
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
}
