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

package org.apache.hudi.hadoop.avro;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestHoodieAvroParquetReader {

  @TempDir
  java.nio.file.Path tmpDir;

  @Test
  void testReadsIntegerBackedDecimalAsHiveDecimalWithProjection() throws Exception {
    Path file = writeIntegerDecimalFile();
    JobConf conf = new JobConf();
    // project only the decimal so the row still places it at its table position
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "price");
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "1");

    List<Writable[]> rows = read(file, conf, Option.empty());

    assertEquals(2, rows.size());
    rows.forEach(row -> assertNull(row[0]));
    assertEquals(Arrays.asList(new BigDecimal("17.00"), new BigDecimal("-0.01")), prices(rows));
  }

  @Test
  void testReadsIntegerBackedDecimalAsHiveDecimalWithTableSchema() throws Exception {
    // the file group reader path hands the table schema in and sets no projection
    Path file = writeIntegerDecimalFile();
    HoodieSchema tableSchema = HoodieSchema.createRecord("m", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("price", HoodieSchema.createNullable(HoodieSchema.createDecimal("price", null, null, 15, 2, 7)), null, null)));

    List<Writable[]> rows = read(file, new JobConf(), Option.of(tableSchema));

    assertEquals(Arrays.asList(1L, 2L), rows.stream().map(row -> ((LongWritable) row[0]).get()).collect(Collectors.toList()));
    assertEquals(Arrays.asList(new BigDecimal("17.00"), new BigDecimal("-0.01")), prices(rows));
  }

  private Path writeIntegerDecimalFile() throws IOException {
    MessageType type = MessageTypeParser.parseMessageType("message m {\n"
        + "  required int64 id;\n"
        + "  optional int64 price (DECIMAL(15,2));\n"
        + "}");
    Path file = new Path(tmpDir.resolve("integer_decimal.parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withType(type).withConf(new Configuration()).build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(type);
      writer.write(factory.newGroup().append("id", 1L).append("price", 1700L));
      writer.write(factory.newGroup().append("id", 2L).append("price", -1L));
    }
    return file;
  }

  private static List<Writable[]> read(Path file, JobConf conf, Option<HoodieSchema> tableSchema) throws Exception {
    long length = file.getFileSystem(conf).getFileStatus(file).getLen();
    ParquetInputSplit split = new ParquetInputSplit(file, 0, length, length, new String[0], null);
    List<Writable[]> rows = new ArrayList<>();
    HoodieAvroParquetReader reader = new HoodieAvroParquetReader(split, conf, Option.empty(), tableSchema);
    try {
      reader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()));
      while (reader.nextKeyValue()) {
        rows.add(reader.getCurrentValue().get());
      }
    } finally {
      reader.close();
    }
    return rows;
  }

  private static List<BigDecimal> prices(List<Writable[]> rows) {
    return rows.stream()
        .map(row -> assertInstanceOf(HiveDecimalWritable.class, row[1]).getHiveDecimal().bigDecimalValue().setScale(2))
        .collect(Collectors.toList());
  }
}
