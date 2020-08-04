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

package org.apache.hudi.utilities.functional;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDeltaStreamerWithOverwriteLatestAvroPayload extends UtilitiesTestBase {
  private static String PARQUET_SOURCE_ROOT;
  private static final String PROPS_FILENAME_TEST_PARQUET = "test-parquet-dfs-source.properties";

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass(true);
    PARQUET_SOURCE_ROOT = dfsBasePath + "/parquetFiles";

    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/sql-transformer.properties", dfs,
        dfsBasePath + "/sql-transformer.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source.avsc", dfs, dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source-flattened.avsc", dfs, dfsBasePath + "/source-flattened.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target.avsc", dfs, dfsBasePath + "/target.avsc");
  }

  @Test
  public void testOverwriteLatestAvroPayload() throws Exception {
    // test defaultDeleteMarkerField
    this.testOverwriteLatestAvroPayload(null);

    // test userDefinedDeleteMarkerField
    this.testOverwriteLatestAvroPayload("user_defined_delete_marker_field");
  }

  private void testOverwriteLatestAvroPayload(String deleteMarkerField) throws Exception {
    String path = PARQUET_SOURCE_ROOT + "/1.parquet";
    List<GenericRecord> records = HoodieTestDataGenerator.generateGenericRecords(5, false, 0);
    Helpers.saveParquetToDFS(records, new Path(path));

    TypedProperties parquetProps = new TypedProperties();
    parquetProps.setProperty("include", "base.properties");
    parquetProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    parquetProps.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    parquetProps.setProperty("hoodie.deltastreamer.source.dfs.root", PARQUET_SOURCE_ROOT);
    if (deleteMarkerField != null) {
      parquetProps.setProperty(HoodieWriteConfig.DELETE_MARKER_FIELD_PROP, deleteMarkerField);
    }
    Helpers.savePropsToDFS(parquetProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_PARQUET);

    String tableBasePath = dfsBasePath + "/test_overwrite_lastest_avro_payload_table";

    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, HoodieDeltaStreamer.Operation.INSERT, ParquetDFSSource.class.getName(),
            null, PROPS_FILENAME_TEST_PARQUET, false,
            false, 100000, false, null, null, "timestamp"), jsc);
    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, tableBasePath + "/*/*.parquet", sqlContext);

    String path2 = PARQUET_SOURCE_ROOT + "/2.parquet";
    List<GenericRecord> records2 = HoodieTestDataGenerator.generateGenericRecords(4, true, 1);
    Helpers.saveParquetToDFS(records2, new Path(path2));
    deltaStreamer.sync();

    List<Row> rows = sqlContext.read().format("org.apache.hudi").load(tableBasePath + "/*/*.parquet").collectAsList();
    assertEquals(1, rows.size());
    assertEquals(records.get(4).get("_row_key"), rows.get(0).getString(2));
  }
}
