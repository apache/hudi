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

package org.apache.hudi.common.table;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieReaderConfig {

  @Test
  public void testIsFileGroupReaderEnabled() {
    HoodieConfig config = new HoodieConfig();
    // tbl version 8 by default allows file group reader
    assertTrue(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, config));
    // tbl version 6 does not allow file group reader
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.SIX, config));
    config.setValue(FILE_GROUP_READER_ENABLED, "false");
    // tbl version 8 does not allow file group reader when config is set to false
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, config));

    Map<String, String> map = new HashMap<>();
    // tbl version 8 by default allows file group reader
    assertTrue(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, map));
    // tbl version 6 does not allow file group reader
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.SIX, map));
    map.put(FILE_GROUP_READER_ENABLED.key(), "false");
    // tbl version 8 does not allow file group reader when config is set to false
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, map));

    Configuration conf = new Configuration();
    // tbl version 8 by default allows file group reader
    assertTrue(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, conf));
    // tbl version 6 does not allow file group reader
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.SIX, conf));
    conf.set(FILE_GROUP_READER_ENABLED.key(), "false");
    // tbl version 8 does not allow file group reader when config is set to false
    assertFalse(HoodieReaderConfig.isFileGroupReaderEnabled(HoodieTableVersion.EIGHT, conf));
  }
}
