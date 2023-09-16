/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Add test cases for out of the box schema evolution for deltastreamer:
 * https://hudi.apache.org/docs/schema_evolution#out-of-the-box-schema-evolution
 */
public class TestHoodieDeltaStreamerSchemaEvolution extends TestHoodieDeltaStreamer {

  private void testBase(String tableType, String updateFile, String updateColumn, String condition, int count) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    String datapath = String.class.getResource("/data/schema-evolution/start.json").getPath();
    sparkSession.read().json(datapath).write().format("parquet").save(PARQUET_SOURCE_ROOT);

    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps);
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config deltaCfg =  TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, "COPY_ON_WRITE", "timestamp", null);

    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(deltaCfg, jsc);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    datapath = String.class.getResource("/data/schema-evolution/" + updateFile).getPath();
    sparkSession.read().json(datapath).write().format("parquet").mode(SaveMode.Append).save(PARQUET_SOURCE_ROOT);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(10, tableBasePath, sqlContext);
    sparkSession.read().format("hudi").load(tableBasePath).select(updateColumn).show(10);
    assertEquals(count, sparkSession.read().format("hudi").load(tableBasePath).filter(condition).count());

  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testAddNullableColRoot(String tableType) throws Exception {
    testBase(tableType, "testAddNullableColRoot.json", "zextra_nullable_col", "zextra_nullable_col = 'yes'", 2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testAddNullableMetaCol(String tableType) throws Exception {
    testBase(tableType, "testAddNullableMetaCol.json", "_extra_nullable_col", "_extra_nullable_col = 'yes'", 2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testAddNullableColStruct(String tableType) throws Exception {
    testBase(tableType, "testAddNullableColStruct.json", "tip_history.zextra_nullable_col", "tip_history[0].zextra_nullable_col = 'yes'", 2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testAddComplexField(String tableType) throws Exception {
    testBase(tableType, "testAddComplexField.json", "zcomplex_array", "size(zcomplex_array) > 0", 2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testAddNullableColChangeOrder(String tableType) throws Exception {
    testBase(tableType, "testAddNullableColChangeOrderPass.json", "extra_nullable_col", "extra_nullable_col = 'yes'", 2);
    testBase(tableType, "testAddNullableColChangeOrderFail.json", "extra_nullable_col", "extra_nullable_col = 'yes'", 1);
  }
}
