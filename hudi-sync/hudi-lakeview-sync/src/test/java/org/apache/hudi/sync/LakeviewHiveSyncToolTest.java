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

package org.apache.hudi.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LakeviewHiveSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() {
    FileSystem fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  void testLakeViewHiveSyncTool() throws Exception {
    HiveTestUtil.setUp(Option.empty(), true);
    LocalDateTime localDateTime = LocalDateTime.now();
    String instantTime = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddhhmmssSSS"));
    HiveTestUtil.createCOWTable(instantTime, 2, false);

    try (MockedConstruction<LakeviewSyncTool> lakeviewSyncToolMockedConstruction = mockConstruction(LakeviewSyncTool.class,
        (lakeviewSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(hiveSyncProps, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        }); LakeviewHiveSyncTool lakeviewHiveSyncTool = new LakeviewHiveSyncTool(hiveSyncProps, hadoopConf)) {
      lakeviewHiveSyncTool.syncHoodieTable();

      List<LakeviewSyncTool> lakeviewSyncToolsConstructed = lakeviewSyncToolMockedConstruction.constructed();
      assertEquals(1, lakeviewSyncToolsConstructed.size());
      LakeviewSyncTool lakeviewSyncTool = lakeviewSyncToolsConstructed.get(0);
      verify(lakeviewSyncTool, times(1)).syncHoodieTable();
    }
  }
}