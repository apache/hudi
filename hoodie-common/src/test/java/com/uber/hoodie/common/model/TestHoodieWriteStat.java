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

package com.uber.hoodie.common.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestHoodieWriteStat {

  @Test
  public void testSetPaths() {
    String commitTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String basePathString = "/data/tables/some-hoodie-table";
    String partitionPathString = "2017/12/31";
    String fileName = UUID.randomUUID().toString();
    String writeToken = "1-0-1";

    Path basePath = new Path(basePathString);
    Path partitionPath = new Path(basePath, partitionPathString);
    Path tempPath = new Path(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);

    Path finalizeFilePath = new Path(partitionPath, FSUtils.makeDataFileName(commitTime, writeToken, fileName));
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new Path(basePath, writeStat.getPath()));

    // test for null tempFilePath
    writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new Path(basePath, writeStat.getPath()));
    assertNull(writeStat.getTempPath());
  }
}
