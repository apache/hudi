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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link ColumnStatsIndices}.
 */
public class TestColumnStatsIndices {
  @TempDir
  File tempFile;

  @Test
  void testTransposeColumnStatsIndex() throws Exception {
    final String path = tempFile.getAbsolutePath();
    Configuration conf = TestConfigurations.getDefaultConf(path);
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    conf.setBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setString("hoodie.metadata.index.column.stats.enable", "true");

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(true)
        .build();
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // explicit query columns
    String[] queryColumns1 = {"uuid", "age"};
    List<RowData> indexRows1 = ColumnStatsIndices.readColumnStatsIndex(path, metadataConfig, queryColumns1);
    Pair<List<RowData>, String[]> transposedIndexTable1 = ColumnStatsIndices
        .transposeColumnStatsIndex(indexRows1, queryColumns1, TestConfigurations.ROW_TYPE);
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
        () -> ColumnStatsIndices.readColumnStatsIndex(path, metadataConfig, new String[0]));
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
