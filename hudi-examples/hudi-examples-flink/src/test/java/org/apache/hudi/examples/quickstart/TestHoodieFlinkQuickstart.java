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

package org.apache.hudi.examples.quickstart;

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.HoodieTableType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.List;

import static org.apache.hudi.examples.quickstart.TestQuickstartData.assertRowsEquals;

/**
 * IT cases for Hoodie table source and sink.
 */

@Disabled
public class TestHoodieFlinkQuickstart extends AbstractTestBase {
  private final HoodieFlinkQuickstart flinkQuickstart = HoodieFlinkQuickstart.instance();

  @BeforeEach
  void beforeEach() {
    flinkQuickstart.initEnv();
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testHoodieFlinkQuickstart(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    flinkQuickstart.createFileSource();

    // create hudi table
    flinkQuickstart.createHudiTable(tempFile.getAbsolutePath(), "t1", tableType);

    // insert data
    List<Row> rows = flinkQuickstart.insertData();
    assertRowsEquals(rows, TestQuickstartData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);

    // query data
    List<Row> rows1 = flinkQuickstart.queryData();
    assertRowsEquals(rows1, TestQuickstartData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);

    // update data
    List<Row> rows2 = flinkQuickstart.updateData();
    assertRowsEquals(rows2, TestQuickstartData.DATA_SET_SOURCE_INSERT_LATEST_COMMIT);
  }
}
