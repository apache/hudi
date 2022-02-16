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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie delta write stat {@link HoodieDeltaWriteStat}.
 */
public class TestHoodieDeltaWriteStat {

  @Test
  public void testBaseFileAndLogFiles() {
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    String baseFile = "file1" + HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
    String logFile1 = ".log1.log";
    String logFile2 = ".log2.log";

    writeStat.setBaseFile(baseFile);
    writeStat.addLogFiles(logFile1);
    writeStat.addLogFiles(logFile2);
    assertTrue(writeStat.getLogFiles().contains(logFile1));
    assertTrue(writeStat.getLogFiles().contains(logFile2));
    assertEquals(baseFile, writeStat.getBaseFile());

    writeStat.setLogFiles(new ArrayList<>());
    assertTrue(writeStat.getLogFiles().isEmpty());
  }
}
