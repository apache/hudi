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
import java.util.Collections;
import java.util.List;

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

  @Test
  void testGetHoodieDeltaWriteStatFromPreviousStat() {
    HoodieDeltaWriteStat prevStat = createDeltaWriteStat("part", "fileId", "888",
        "base", Collections.singletonList("log1"));
    HoodieDeltaWriteStat stat = prevStat.copy();
    assertEquals(prevStat.getPartitionPath(), stat.getPartitionPath());
    assertEquals(prevStat.getFileId(), stat.getFileId());
    assertEquals(prevStat.getPrevCommit(), stat.getPrevCommit());
    assertEquals(prevStat.getBaseFile(), stat.getBaseFile());
    assertEquals(1, stat.getLogFiles().size());
    assertEquals(prevStat.getLogFiles().get(0), stat.getLogFiles().get(0));
  }

  private HoodieDeltaWriteStat createDeltaWriteStat(String partition, String fileId, String prevCommit, String baseFile, List<String> logFiles) {
    HoodieDeltaWriteStat writeStat1 = new HoodieDeltaWriteStat();
    writeStat1.setPartitionPath(partition);
    writeStat1.setFileId(fileId);
    writeStat1.setPrevCommit(prevCommit);
    writeStat1.setBaseFile(baseFile);
    writeStat1.setLogFiles(logFiles);
    return writeStat1;
  }
}
