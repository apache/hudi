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

package org.apache.hudi.util;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests that map conversions materialize the input arrays only once.
 */
public class TestMapDataConversion {

  @ParameterizedTest
  @CsvSource({"true, 0", "true, 1", "true, 100", "false, 0", "false, 1", "false, 100"})
  public void testMapArraysMaterializedOnce(boolean projection, int size) {
    Map<Integer, Integer> input = new HashMap<>();
    Map<Integer, Long> expected = new HashMap<>();
    for (int i = 0; i < size; i++) {
      input.put(i, i % 2 == 0 ? i : null);
      expected.put(i, i % 2 == 0 ? (long) i : null);
    }
    GenericMapData map = spy(new GenericMapData(input));
    MapType from = new MapType(new IntType(), new IntType());
    MapType to = new MapType(new IntType(), new BigIntType());
    MapData result;
    if (projection) {
      SchemaEvolvingRowDataProjection converter = SchemaEvolvingRowDataProjection.instance(
          RowType.of(from), RowType.of(to), Collections.emptyMap());
      result = converter.project(GenericRowData.of(map)).getMap(0);
    } else {
      result = (MapData) TypeConverters.getInstance(from, to).convert(map);
    }

    assertEquals(new GenericMapData(expected), result);
    verify(map).keyArray();
    verify(map).valueArray();
  }
}
