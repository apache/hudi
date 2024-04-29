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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHoodieRealtimeInputFormatUtils {

  private Configuration hadoopConf;

  @TempDir
  public java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() {
    hadoopConf = HoodieTestUtils.getDefaultStorageConf().unwrap();
    hadoopConf.set("fs.defaultFS", "file:///");
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
  }

  @Test
  public void testAddProjectionField() {
    hadoopConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    HoodieRealtimeInputFormatUtils.addProjectionField(hadoopConf, hadoopConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "").split("/"));
  }
}
