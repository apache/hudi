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

package org.apache.hudi.source.stats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link ColumnStatsIndex}.
 */
public class TestColumnStatsIndex {
  @TempDir
  File tempFile;

  @Test
  void testReadPartitionStatsIndex() throws Exception {
    final String path = tempFile.getAbsolutePath();
    Configuration conf = TestConfigurations.getDefaultConf(path);
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.setString("hoodie.metadata.index.partition.stats.enable", "true");
    conf.setString("hoodie.metadata.index.column.stats.enable", "true");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String[] queryColumns = {"uuid", "age"};
    PartitionStatsIndex indexSupport = new PartitionStatsIndex(path, TestConfigurations.ROW_TYPE, conf, StreamerUtil.createMetaClient(conf));
    List<RowData> indexRows = indexSupport.readColumnStatsIndexByColumns(queryColumns);
    List<String> results = indexRows.stream().map(Object::toString).sorted(String::compareTo).collect(Collectors.toList());
    List<String> expected = Arrays.asList(
        "+I(par1,+I(23),+I(33),0,2,age)",
        "+I(par1,+I(id1),+I(id2),0,2,uuid)",
        "+I(par2,+I(31),+I(53),0,2,age)",
        "+I(par2,+I(id3),+I(id4),0,2,uuid)",
        "+I(par3,+I(18),+I(20),0,2,age)",
        "+I(par3,+I(id5),+I(id6),0,2,uuid)",
        "+I(par4,+I(44),+I(56),0,2,age)",
        "+I(par4,+I(id7),+I(id8),0,2,uuid)");
    assertEquals(expected, results);

    Pair<List<RowData>, String[]> transposedIndexTable = indexSupport.transposeColumnStatsIndex(indexRows, queryColumns);
    List<String> transposed = transposedIndexTable.getLeft().stream().map(Object::toString).sorted(String::compareTo).collect(Collectors.toList());
    assertThat(transposed.size(), is(4));
    assertArrayEquals(new String[] {"age", "uuid"}, transposedIndexTable.getRight());
    List<String> expected1 = Arrays.asList(
        "+I(par1,2,23,33,0,id1,id2,0)",
        "+I(par2,2,31,53,0,id3,id4,0)",
        "+I(par3,2,18,20,0,id5,id6,0)",
        "+I(par4,2,44,56,0,id7,id8,0)");
    assertEquals(expected1, transposed);
  }

  @Test
  void testTransposeColumnStatsIndex() throws Exception {
    final String path = tempFile.getAbsolutePath();
    Configuration conf = TestConfigurations.getDefaultConf(path);
    conf.setBoolean(HoodieMetadataConfig.ENABLE.key(), true);
    conf.setBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setBoolean(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), true);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // explicit query columns
    String[] queryColumns1 = {"uuid", "age"};
    FileStatsIndex indexSupport = new FileStatsIndex(path, TestConfigurations.ROW_TYPE, conf, StreamerUtil.createMetaClient(conf));
    List<RowData> indexRows1 = indexSupport.readColumnStatsIndexByColumns(queryColumns1);
    Pair<List<RowData>, String[]> transposedIndexTable1 = indexSupport.transposeColumnStatsIndex(indexRows1, queryColumns1);
    assertThat("The schema columns should sort by natural order",
        Arrays.toString(transposedIndexTable1.getRight()), is("[age, uuid]"));
    List<RowData> transposed1 = filterOutFileNames(transposedIndexTable1.getLeft());
    assertThat(transposed1.size(), is(4));
    final String expected = "["
        + "+I(2,18,20,0,id5,id6,0), "
        + "+I(2,23,33,0,id1,id2,0), "
        + "+I(2,31,53,0,id3,id4,0), "
        + "+I(2,44,56,0,id7,id8,0)]";
    assertThat(transposed1.toString(), is(expected));

    // no query columns, only for tests
    assertThrows(IllegalArgumentException.class,
        () -> indexSupport.readColumnStatsIndexByColumns(new String[0]));
  }

  private static List<RowData> filterOutFileNames(List<RowData> indexRows) {
    return indexRows.stream().map(row -> {
      GenericRowData gr = (GenericRowData) row;
      GenericRowData converted = new GenericRowData(gr.getArity() - 1);
      for (int i = 1; i < gr.getArity(); i++) {
        converted.setField(i - 1, gr.getField(i));
      }
      return converted;
    })
        // sort by age min values
        .sorted(Comparator.comparingInt(r -> r.getInt(1)))
        .collect(Collectors.toList());
  }
}
