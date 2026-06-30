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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the MySQL-specific behavior of {@link MysqlDebeziumTransformer}: the surfaced binlog
 * metadata columns and the derived {@code _event_seq} ordering column, in both flat and nested
 * layouts.
 */
class TestMysqlDebeziumTransformer extends DebeziumTransformerTestBase {

  // A MySQL source struct carries binlog file/pos/row in addition to name/ts_ms. before is populated
  // (same shape as after) so Spark infers it as a struct; these tests use inserts (op=c) which take
  // `after` regardless of before's contents.
  private static String mysqlEvent(String op, long id, String file, long pos) {
    String row = "{\"id\":" + id + ",\"title\":\"t" + id + "\"}";
    return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000500,"
        + "\"before\":" + row + ","
        + "\"after\":" + row + ","
        + "\"source\":{\"name\":\"mysqldb\",\"ts_ms\":1700000000000,"
        + "\"file\":\"" + file + "\",\"pos\":" + pos + ",\"row\":0}}";
  }

  @Test
  void testMysqlMetadataColumnsAndSeqSurfacedFlat() {
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");

    Dataset<Row> result = new MysqlDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(mysqlEvent("c", 1, "mysql-bin.000001", 100)), props);

    List<String> columns = Arrays.asList(result.columns());
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_FILE_COL_NAME), "_event_bin_file present");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_POS_COL_NAME), "_event_pos present");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_ROW_COL_NAME), "_event_row present");
    assertTrue(columns.contains(DebeziumConstants.ADDED_SEQ_COL_NAME), "_event_seq present");

    // _event_seq = "<binlog-suffix>.<pos>" -> "000001.100"
    String seq = result.first().getAs(DebeziumConstants.ADDED_SEQ_COL_NAME);
    assertEquals("000001.100", seq);
  }

  @Test
  void testFlatByDefault() {
    // MySQL does not opt into nested metadata, so with no property set the layout is flat.
    Dataset<Row> result = new MysqlDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(mysqlEvent("c", 1, "mysql-bin.000001", 100)), new TypedProperties());

    List<String> columns = Arrays.asList(result.columns());
    assertFalse(columns.contains(AbstractDebeziumTransformer.DEBEZIUM_METADATA_FIELD),
        "MySQL should be flat by default");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_FILE_COL_NAME), "_event_bin_file at root");
  }

  @Test
  void testNestedModeGroupsMetadataButSeqStaysCorrect() {
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "true");

    Dataset<Row> result = new MysqlDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(mysqlEvent("c", 1, "mysql-bin.000001", 100)), props);

    List<String> columns = Arrays.asList(result.columns());
    assertTrue(columns.contains(AbstractDebeziumTransformer.DEBEZIUM_METADATA_FIELD), "metadata nested");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_OP_COL_NAME), "op at root");
    assertTrue(columns.contains(DebeziumConstants.ADDED_SEQ_COL_NAME), "_event_seq at root");
    assertFalse(columns.contains(DebeziumConstants.FLATTENED_FILE_COL_NAME), "binlog file is nested, not root");

    Row metadata = result.first().getAs(AbstractDebeziumTransformer.DEBEZIUM_METADATA_FIELD);
    List<String> nested = Arrays.asList(metadata.schema().fieldNames());
    assertTrue(nested.contains(DebeziumConstants.FLATTENED_FILE_COL_NAME));
    assertTrue(nested.contains(DebeziumConstants.FLATTENED_POS_COL_NAME));
    assertTrue(nested.contains(DebeziumConstants.FLATTENED_ROW_COL_NAME));

    // _event_seq must still be computed correctly from the nested file/pos
    String seq = result.first().getAs(DebeziumConstants.ADDED_SEQ_COL_NAME);
    assertEquals("000001.100", seq);
  }
}
