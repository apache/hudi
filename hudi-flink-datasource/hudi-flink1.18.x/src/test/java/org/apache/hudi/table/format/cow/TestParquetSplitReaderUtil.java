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

package org.apache.hudi.table.format.cow;

import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for reading nested ARRAY columns through {@link ParquetSplitReaderUtil} and
 * {@link ParquetColumnarRowSplitReader}, covering the list encodings allowed by the Parquet
 * backward-compatibility rules.
 */
public class TestParquetSplitReaderUtil {

  private static final LogicalType STRING = new VarCharType(VarCharType.MAX_LENGTH);

  private static final int DEFAULT_BATCH_SIZE = 2048;

  @TempDir
  File tempDir;

  /**
   * Physical encodings of a list of single- or multi-field element groups.
   */
  enum ListEncoding {
    THREE_LEVEL {
      @Override
      String schema(String name, String elementFields) {
        return "optional group " + name + " (LIST) { repeated group list { optional group element { "
            + elementFields + " } } }";
      }

      @Override
      Group addElement(Group list, String name) {
        return list.addGroup("list").addGroup("element");
      }
    },
    TWO_LEVEL_ARRAY {
      @Override
      String schema(String name, String elementFields) {
        return "optional group " + name + " (LIST) { repeated group array { " + elementFields + " } }";
      }

      @Override
      Group addElement(Group list, String name) {
        return list.addGroup("array");
      }
    },
    TWO_LEVEL_TUPLE {
      @Override
      String schema(String name, String elementFields) {
        return "optional group " + name + " (LIST) { repeated group " + name + "_tuple { "
            + elementFields + " } }";
      }

      @Override
      Group addElement(Group list, String name) {
        return list.addGroup(name + "_tuple");
      }
    };

    abstract String schema(String name, String elementFields);

    abstract Group addElement(Group list, String name);
  }

  @ParameterizedTest
  @EnumSource(ListEncoding.class)
  void testReadArrayOfRowWithNestedArrayOfRow(ListEncoding encoding) throws IOException {
    MessageType schema = itemsSchema(encoding);
    LogicalType itemsType = new ArrayType(
        rowType(new String[] {"name", "tags"}, STRING, new ArrayType(rowType(new String[] {"value"}, STRING))));

    List<String> values = writeAndRead(schema, itemsWriter(encoding), "items", itemsType,
        row -> formatItems(row, false));

    assertEquals(Arrays.asList("[a:[x, y], b:[z]]", "[c:[]]", "null"), values);
  }

  @ParameterizedTest
  @EnumSource(ListEncoding.class)
  void testReadArrayOfRowWithFieldsMissingFromFile(ListEncoding encoding) throws IOException {
    List<String> values = writeAndRead(itemsSchema(encoding), itemsWriter(encoding), "items",
        evolvedItemsType(), row -> formatItems(row, true), DEFAULT_BATCH_SIZE);

    assertEquals(
        Arrays.asList("[a(null):[x(null), y(null)], b(null):[z(null)]]", "[c(null):[]]", "null"), values);
  }

  @ParameterizedTest
  @EnumSource(ListEncoding.class)
  void testReadArrayOfRowWithFieldsMissingFromFileInBatchSmallerThanElementCount(ListEncoding encoding)
      throws IOException {
    // The first batch holds two rows with three items and three tags, so the placeholders of the
    // absent fields must grow beyond the batch size.
    List<String> values = writeAndRead(itemsSchema(encoding), itemsWriter(encoding), "items",
        evolvedItemsType(), row -> formatItems(row, true), 2);

    assertEquals(
        Arrays.asList("[a(null):[x(null), y(null)], b(null):[z(null)]]", "[c(null):[]]", "null"), values);
  }

  @Test
  void testReadBareRepeatedGroupNamedAfterItsParentAsArrayOfRow() throws IOException {
    // A repeated group with one field named <parent>_tuple is the element itself, not a wrapper.
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional group info { repeated group info_tuple { optional int32 x; } } }");
    Consumer<Group> writer = record -> {
      Group info = record.addGroup("info");
      info.addGroup("info_tuple").append("x", 1);
      info.addGroup("info_tuple").append("x", 2);
    };
    LogicalType infoType = rowType(new String[] {"info_tuple"}, new ArrayType(rowType(new String[] {"x"}, new IntType())));

    List<String> values = writeAndRead(schema, writer, "info", infoType, row -> {
      ArrayData tuples = row.getRow(0, 1).getArray(0);
      List<Integer> xs = new ArrayList<>();
      for (int i = 0; i < tuples.size(); i++) {
        xs.add(tuples.getRow(i, 1).getInt(0));
      }
      return xs.toString();
    });

    assertEquals(Arrays.asList("[1, 2]", "[1, 2]", "[1, 2]"), values);
  }

  @Test
  void testReadRowWithFieldMissingFromFileAfterLongArray() throws IOException {
    // The row's last field in the file is an array with more elements than the batch size, which
    // must not count as more rows when sizing the placeholder of the absent field.
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional group info { optional binary name (UTF8); "
            + "optional group nums (LIST) { repeated group list { optional int32 element; } } } }");
    int[] recordIndex = {0};
    Consumer<Group> writer = record -> {
      Group info = record.addGroup("info");
      info.append("name", "r" + recordIndex[0]++);
      Group nums = info.addGroup("nums");
      for (int i = 0; i < 100; i++) {
        nums.addGroup("list").append("element", i);
      }
    };
    LogicalType infoType = rowType(new String[] {"name", "nums", "added"}, STRING, new ArrayType(new IntType()), STRING);

    List<String> values = writeAndRead(schema, writer, "info", infoType, row -> {
      RowData info = row.getRow(0, 3);
      return info.getString(0) + ":" + info.getArray(1).size() + ":" + (info.isNullAt(2) ? "null" : info.getString(2));
    }, 2);

    assertEquals(Arrays.asList("r0:100:null", "r1:100:null", "r2:100:null"), values);
  }

  @ParameterizedTest
  @CsvSource({"int32, 9", "int64, 18"})
  void testReadArrayOfRowWithIntegerEncodedDecimal(String physicalType, int precision) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional group items (LIST) { repeated group list { optional group element { "
            + "optional " + physicalType + " amount (DECIMAL(" + precision + ",2)); } } } }");
    Consumer<Group> writer = record -> {
      Group items = record.addGroup("items");
      appendUnscaled(items.addGroup("list").addGroup("element"), physicalType, 12345);
      appendUnscaled(items.addGroup("list").addGroup("element"), physicalType, -1);
    };
    LogicalType itemsType = new ArrayType(rowType(new String[] {"amount"}, new DecimalType(precision, 2)));

    List<String> values = writeAndRead(schema, writer, "items", itemsType, row -> {
      ArrayData items = row.getArray(0);
      List<String> amounts = new ArrayList<>();
      for (int i = 0; i < items.size(); i++) {
        amounts.add(items.getRow(i, 1).getDecimal(0, precision, 2).toString());
      }
      return amounts.toString();
    });

    assertEquals(Arrays.asList("[123.45, -0.01]", "[123.45, -0.01]", "[123.45, -0.01]"), values);
  }

  @ParameterizedTest
  @CsvSource({"int32, 9", "int64, 18"})
  void testReadIntegerEncodedDecimal(String physicalType, int precision) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional " + physicalType + " amount (DECIMAL(" + precision + ",2)); }");

    List<String> values = writeAndRead(schema, record -> appendUnscaled(record, physicalType, 12345), "amount",
        new DecimalType(precision, 2), row -> row.getDecimal(0, precision, 2).toString());

    assertEquals(Arrays.asList("123.45", "123.45", "123.45"), values);
  }

  @Test
  void testReadThreeLevelArrayOfPrimitive() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional group nums (LIST) { repeated group list { required int32 element; } } }");
    Consumer<Group> writer = record -> {
      Group list = record.addGroup("nums");
      list.addGroup("list").append("element", 1);
      list.addGroup("list").append("element", 2);
    };

    List<String> values = writeAndRead(schema, writer, "nums", new ArrayType(new IntType()),
        row -> formatInts(row.getArray(0)));

    assertEquals(Arrays.asList("[1, 2]", "[1, 2]", "[1, 2]"), values);
  }

  @Test
  void testReadTwoLevelArrayOfPrimitive() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { optional group nums (LIST) { repeated int32 array; } }");
    Consumer<Group> writer = record -> {
      Group list = record.addGroup("nums");
      list.append("array", 1);
      list.append("array", 2);
    };

    List<String> values = writeAndRead(schema, writer, "nums", new ArrayType(new IntType()),
        row -> formatInts(row.getArray(0)));

    assertEquals(Arrays.asList("[1, 2]", "[1, 2]", "[1, 2]"), values);
  }

  /**
   * Appends the unscaled value of a decimal to the {@code amount} field, as the given physical type.
   */
  private static void appendUnscaled(Group group, String physicalType, int unscaled) {
    if (physicalType.equals("int32")) {
      group.append("amount", unscaled);
    } else {
      group.append("amount", (long) unscaled);
    }
  }

  private static MessageType itemsSchema(ListEncoding encoding) {
    String tags = encoding.schema("tags", "optional binary value (UTF8);");
    String items = encoding.schema("items", "optional binary name (UTF8); " + tags);
    return MessageTypeParser.parseMessageType("message m { " + items + " }");
  }

  /**
   * Writes three records: two non-empty item lists (one with an empty nested list) and a null list.
   */
  private static Consumer<Group> itemsWriter(ListEncoding encoding) {
    int[] recordIndex = {0};
    return record -> {
      int index = recordIndex[0]++;
      if (index == 0) {
        Group items = record.addGroup("items");
        addItem(encoding, items, "a", "x", "y");
        addItem(encoding, items, "b", "z");
      } else if (index == 1) {
        addItem(encoding, record.addGroup("items"), "c");
      }
    };
  }

  private static void addItem(ListEncoding encoding, Group items, String name, String... tags) {
    Group item = encoding.addElement(items, "items");
    item.append("name", name);
    Group tagList = item.addGroup("tags");
    for (String tag : tags) {
      encoding.addElement(tagList, "tags").append("value", tag);
    }
  }

  /**
   * Returns the items type with fields that the written files don't have: {@code added_scores} on
   * the item and {@code added_tag_field} on the nested tag.
   */
  private static LogicalType evolvedItemsType() {
    RowType tagType = rowType(new String[] {"value", "added_tag_field"}, STRING, STRING);
    return new ArrayType(
        rowType(new String[] {"name", "added_scores", "tags"}, STRING, new ArrayType(new IntType()),
            new ArrayType(tagType)));
  }

  private static RowType rowType(String[] names, LogicalType... types) {
    return RowType.of(types, names);
  }

  private List<String> writeAndRead(
      MessageType schema,
      Consumer<Group> recordWriter,
      String columnName,
      LogicalType columnType,
      Function<RowData, String> formatter) throws IOException {
    return writeAndRead(schema, recordWriter, columnName, columnType, formatter, DEFAULT_BATCH_SIZE);
  }

  private List<String> writeAndRead(
      MessageType schema,
      Consumer<Group> recordWriter,
      String columnName,
      LogicalType columnType,
      Function<RowData, String> formatter,
      int batchSize) throws IOException {
    Path path = new Path(new File(tempDir, "data.parquet").getAbsolutePath());
    Configuration conf = new Configuration();
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(schema).withConf(conf).build()) {
      for (int i = 0; i < 3; i++) {
        Group record = factory.newGroup();
        recordWriter.accept(record);
        writer.write(record);
      }
    }

    List<String> values = new ArrayList<>();
    try (ParquetColumnarRowSplitReader reader = new ParquetColumnarRowSplitReader(
        true,
        true,
        conf,
        new LogicalType[] {columnType},
        new String[] {columnName},
        VectorizedColumnBatch::new,
        batchSize,
        path,
        0,
        new File(tempDir, "data.parquet").length(),
        null,
        null)) {
      while (!reader.reachedEnd()) {
        values.add(formatter.apply(reader.nextRecord()));
      }
    }
    return values;
  }

  /**
   * Formats {@code array<row<name, [added_scores,] array<row<value[, added_tag_field]>>>>}. With
   * {@code evolved}, the added fields are printed in parentheses after the existing ones.
   */
  private static String formatItems(RowData row, boolean evolved) {
    if (row.isNullAt(0)) {
      return "null";
    }
    ArrayData items = row.getArray(0);
    int tagsPos = evolved ? 2 : 1;
    List<String> formatted = new ArrayList<>();
    for (int i = 0; i < items.size(); i++) {
      RowData item = items.getRow(i, tagsPos + 1);
      String name = item.getString(0).toString();
      if (evolved) {
        name += "(" + (item.isNullAt(1) ? "null" : formatInts(item.getArray(1))) + ")";
      }
      ArrayData tags = item.getArray(tagsPos);
      List<String> tagValues = new ArrayList<>();
      for (int j = 0; j < tags.size(); j++) {
        RowData tag = tags.getRow(j, evolved ? 2 : 1);
        String value = tag.getString(0).toString();
        if (evolved) {
          value += "(" + (tag.isNullAt(1) ? "null" : tag.getString(1).toString()) + ")";
        }
        tagValues.add(value);
      }
      formatted.add(name + ":" + tagValues);
    }
    return formatted.toString();
  }

  private static String formatInts(ArrayData array) {
    List<Integer> ints = new ArrayList<>();
    for (int i = 0; i < array.size(); i++) {
      ints.add(array.getInt(i));
    }
    return ints.toString();
  }
}
