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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.CommittedInstants;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests how splits carry a {@link HiveReaderTableState}.
 */
class TestHiveReaderTableStateEncoding {

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveReaderTableStateEncoding.class);
  private static final String BASE_PATH = "file:/tmp/hudi_table";
  private static final String LATEST_COMMIT = "20260101000500000";

  /**
   * The state of a wide table read by many splits is encoded once and every split carries at most the size bound,
   * dropping the create schema; a table too wide for the bound ships the state without the schema.
   */
  @Test
  void testSplitSizeIsBoundedForWideTables() throws IOException {
    for (int columns : new int[] {50, 1000, 10000}) {
      String schema = wideSchema(columns);
      Option<HiveReaderTableState> state = HiveReaderTableState.create(BASE_PATH, LATEST_COMMIT, tableProperties(schema),
          schema, Option.of(committedInstants(500)), HiveReaderTableState.MAX_ENCODED_BYTES);
      assertTrue(state.isPresent());
      assertEquals(columns < 10000, state.get().getTableSchema().isPresent(), "schema shipped for " + columns + " columns");
      assertFalse(state.get().getTableState().getTableConfig().contains(HoodieTableConfig.CREATE_SCHEMA));

      int maxSplitSize = 0;
      for (int i = 0; i < 10000; i++) {
        FileSplitWithReaderTableState split = new FileSplitWithReaderTableState(
            new Path(BASE_PATH + "/2026/01/01/file-" + i + "_0-1-1_" + LATEST_COMMIT + ".parquet"), 0, 1024, new String[0], state.get());
        maxSplitSize = Math.max(maxSplitSize, serialize(split).length);
      }
      LOG.info("{} columns: schema {} bytes, encoded state {} bytes, largest of 10000 splits {} bytes", columns,
          schema.length(), state.get().getEncoded().length, maxSplitSize);
      assertTrue(maxSplitSize <= HiveReaderTableState.MAX_ENCODED_BYTES + 512, "split of " + maxSplitSize + " bytes");
    }
  }

  @Test
  void testStateSurvivesSplitRoundTrip() throws IOException {
    String schema = wideSchema(10);
    Map<String, String> properties = tableProperties(schema);
    HiveReaderTableState state = HiveReaderTableState.create(BASE_PATH, LATEST_COMMIT, properties, schema,
        Option.of(committedInstants(3)), HiveReaderTableState.MAX_ENCODED_BYTES).get();
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(BASE_PATH + "/file-1_0-1-1_001.parquet"), 0, 1024, new String[0]), BASE_PATH,
        Collections.singletonList(new HoodieLogFile(new StoragePath(BASE_PATH + "/.file-1_001.log.1_0-1-1"), 10L)),
        LATEST_COMMIT, false, Option.empty());
    split.setReaderTableState(Option.of(state));

    HoodieRealtimeFileSplit read = new HoodieRealtimeFileSplit();
    deserialize(serialize(split), read);
    HiveReaderTableState readState = read.getReaderTableState().get();
    assertEquals(LATEST_COMMIT, readState.getLatestCommitTime());
    assertEquals(schema, readState.getTableSchema().get());
    FileGroupReaderTableState tableState = readState.getTableState();
    assertEquals(BASE_PATH, tableState.getBasePath().toString());
    assertEquals("v", tableState.getTableConfig().getProps().getProperty("hoodie.table.some.property"));
    assertTrue(tableState.isCommitted("20260101000000000"));
    assertFalse(tableState.isCommitted("20260101000001000"));
    assertEquals(split.toString(), read.toString());
  }

  /**
   * Splits written without the trailing state, or with a format this reader does not know, read as splits without it.
   */
  @Test
  void testSplitsWithoutReadableStateReadWithout() throws IOException {
    HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
        new FileSplit(new Path(BASE_PATH + "/file-1_0-1-1_001.parquet"), 0, 1024, new String[0]), BASE_PATH,
        new ArrayList<>(), LATEST_COMMIT, false, Option.empty());
    byte[] bytes = serialize(split);
    // the state flag is the last byte of a split without the state
    byte[] withoutTrailingField = new byte[bytes.length - 1];
    System.arraycopy(bytes, 0, withoutTrailingField, 0, withoutTrailingField.length);
    HoodieRealtimeFileSplit read = new HoodieRealtimeFileSplit();
    deserialize(withoutTrailingField, read);
    assertFalse(read.getReaderTableState().isPresent());
    assertEquals(split.toString(), read.toString());

    DataOutputBuffer out = new DataOutputBuffer();
    out.writeByte(7);
    out.writeInt(3);
    out.write(new byte[] {1, 2, 3});
    out.writeInt(42);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    assertFalse(HiveReaderTableState.read(in).isPresent());
    assertEquals(42, in.readInt());
  }

  private static String wideSchema(int columns) {
    List<HoodieSchemaField> fields = IntStream.range(0, columns)
        .mapToObj(i -> HoodieSchemaField.of("column_" + Integer.toHexString(i * 7919) + "_" + i,
            HoodieSchema.createNullable(i % 2 == 0 ? HoodieSchemaType.LONG : HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE))
        .collect(Collectors.toList());
    return HoodieSchema.createRecord("wide_record", "org.apache.hudi.test", null, fields).toString();
  }

  private static Map<String, String> tableProperties(String schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put(HoodieTableConfig.NAME.key(), "wide_table");
    properties.put(HoodieTableConfig.CREATE_SCHEMA.key(), schema);
    properties.put("hoodie.table.some.property", "v");
    return properties;
  }

  private static CommittedInstants committedInstants(int count) {
    List<String> completed = IntStream.range(0, count)
        .mapToObj(i -> String.format("202601010000%02d%03d", i / 1000 % 60, i % 1000)).collect(Collectors.toList());
    return CommittedInstants.of(completed, Collections.singletonList("20260101000001000"), Option.of(completed.get(0)));
  }

  private static byte[] serialize(Writable split) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    split.write(out);
    byte[] bytes = new byte[out.getLength()];
    System.arraycopy(out.getData(), 0, bytes, 0, out.getLength());
    return bytes;
  }

  private static void deserialize(byte[] bytes, Writable split) throws IOException {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    split.readFields(in);
    assertNotNull(split);
  }
}
