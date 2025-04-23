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

import org.apache.hudi.util.SchemaEvolvingRowDataProjection;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests from {@link SchemaEvolvingRowDataProjection}.
 */
public class TestSchemaEvolvingRowDataProjection {
  @Test
  public void testSchemaEvolvedProjection() {
    // nested composite type: Array<Row>
    RowType innerRow = new RowType(
        false,
        Arrays.asList(
            new RowType.RowField("inner_f1", DataTypes.INT().getLogicalType()),
            new RowType.RowField("inner_f2", DataTypes.STRING().getLogicalType())));
    ArrayType rowArray = new ArrayType(innerRow);
    // nested composite type: Map<String, Row>
    MapType rowMap = new MapType(DataTypes.STRING().getLogicalType(), innerRow);

    // schema evolution of nested composite type
    RowType changedInnerRow = new RowType(
        false,
        Arrays.asList(
            // change field type of the inner RowType field
            new RowType.RowField("inner_f1", DataTypes.STRING().getLogicalType()),
            // rename field of the inner RowType field
            new RowType.RowField("inner_f2_change", DataTypes.STRING().getLogicalType()),
            // add a field for the inner RowType field
            new RowType.RowField("inner_f3", DataTypes.DOUBLE().getLogicalType())));
    ArrayType changedRowArray = new ArrayType(changedInnerRow);
    MapType changedRowMap = new MapType(DataTypes.STRING().getLogicalType(), changedInnerRow);

    RowType from = new RowType(
        false,
        Arrays.asList(
            new RowType.RowField("uuid", DataTypes.STRING().getLogicalType()),
            new RowType.RowField("partition", DataTypes.STRING().getLogicalType()),
            new RowType.RowField("f1", DataTypes.INT().getLogicalType()),
            new RowType.RowField("f2", DataTypes.STRING().getLogicalType()),
            new RowType.RowField("row_f", innerRow),
            new RowType.RowField("map_f", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).getLogicalType()),
            new RowType.RowField("array_f", DataTypes.ARRAY(DataTypes.INT()).getLogicalType()),
            new RowType.RowField("row_array", rowArray),
            new RowType.RowField("row_map", rowMap),
            new RowType.RowField("ts", DataTypes.BIGINT().getLogicalType())));

    RowType to = new RowType(
        false,
        Arrays.asList(
            // change field order
            new RowType.RowField("row_array", changedRowArray),
            new RowType.RowField("partition", DataTypes.STRING().getLogicalType()),
            // change field name
            new RowType.RowField("f1_change", DataTypes.INT().getLogicalType()),
            new RowType.RowField("f2", DataTypes.STRING().getLogicalType()),
            new RowType.RowField("row_f", changedInnerRow),
            // change value field type for map
            new RowType.RowField("map_f", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()).getLogicalType()),
            // change value field type for array
            new RowType.RowField("array_f", DataTypes.ARRAY(DataTypes.DOUBLE()).getLogicalType()),
            new RowType.RowField("row_map", changedRowMap),
            // add a field
            new RowType.RowField("f3", DataTypes.STRING().getLogicalType()),
            // change field type
            new RowType.RowField("ts", DataTypes.DOUBLE().getLogicalType())));

    // column renaming mapping: new -> old
    Map<String, String> renamedCols = new HashMap<String, String>() {{
        put("f1_change", "f1");
        put("row_map.value.inner_f2_change", "inner_f2");
        put("row_f.inner_f2_change", "inner_f2");
        put("row_array.element.inner_f2_change", "inner_f2");
      }
    };

    GenericRowData innerRowData = new GenericRowData(innerRow.getFieldCount());
    innerRowData.setField(0, 1);
    innerRowData.setField(1, StringData.fromString("f2_str"));
    ArrayData rowArrayData = new GenericArrayData(new GenericRowData[] {innerRowData});
    MapData mapData = new GenericMapData(new HashMap<StringData, Integer>() {{
        put(StringData.fromString("k1"), 1);
        put(StringData.fromString("k2"), 2);
      }
    });
    MapData rowMapData = new GenericMapData(new HashMap<StringData, GenericRowData>() {{
        put(StringData.fromString("key"), innerRowData);
      }
    });
    ArrayData arrayData = new GenericArrayData(new Integer[] {1, 2, 3});

    GenericRowData rowData = new GenericRowData(from.getFieldCount());
    rowData.setField(0, StringData.fromString("uuid"));
    rowData.setField(1, StringData.fromString("par1"));
    rowData.setField(2, 1);
    rowData.setField(3, StringData.fromString("asdf"));
    rowData.setField(4, innerRowData);
    rowData.setField(5, mapData);
    rowData.setField(6, arrayData);
    rowData.setField(7, rowArrayData);
    rowData.setField(8, rowMapData);
    rowData.setField(9, 1000L);

    MapData changedMapData = new GenericMapData(new HashMap<StringData, StringData>() {{
        put(StringData.fromString("k1"), StringData.fromString("1"));
        put(StringData.fromString("k2"), StringData.fromString("2"));
      }
    });

    ArrayData changedArrayData = new GenericArrayData(new Double[] {1.0, 2.0, 3.0});

    GenericRowData expected = new GenericRowData(to.getFieldCount());
    GenericRowData innerExpected = new GenericRowData(changedInnerRow.getFieldCount());
    innerExpected.setField(0, StringData.fromString("1"));
    innerExpected.setField(1, StringData.fromString("f2_str"));
    innerExpected.setField(2, null);
    ArrayData changedRowArrayData = new GenericArrayData(new GenericRowData[] {innerExpected});
    MapData changedRowMapData = new GenericMapData(new HashMap<StringData, GenericRowData>() {{
        put(StringData.fromString("key"), innerExpected);
      }
    });

    expected.setField(0, changedRowArrayData);
    expected.setField(1, StringData.fromString("par1"));
    expected.setField(2, 1);
    expected.setField(3, StringData.fromString("asdf"));
    expected.setField(4, innerExpected);
    expected.setField(5, changedMapData);
    expected.setField(6, changedArrayData);
    expected.setField(7, changedRowMapData);
    expected.setField(8, null);
    expected.setField(9, 1000.0);

    SchemaEvolvingRowDataProjection projection = SchemaEvolvingRowDataProjection.instance(from, to, renamedCols);
    GenericRowData projRow = (GenericRowData) projection.project(rowData);
    assertEquals(expected, projRow);
  }
}
