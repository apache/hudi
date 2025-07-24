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

package org.apache.hudi.configuration;

import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link OptionsResolver}
 */
public class TestOptionsResolver {
  @TempDir
  File tempFile;
  
  @Test
  void testGetIndexType() {
    Configuration conf = getConf();
    // set uppercase index
    conf.set(FlinkOptions.INDEX_TYPE, "BLOOM");
    assertEquals(HoodieIndex.IndexType.BLOOM, OptionsResolver.getIndexType(conf));
    // set lowercase index
    conf.set(FlinkOptions.INDEX_TYPE, "bloom");
    assertEquals(HoodieIndex.IndexType.BLOOM, OptionsResolver.getIndexType(conf));
  }

  @Test
  void testIsLazyFailedWritesCleanPolicy() {
    Configuration conf = new Configuration();
    // add any parameter
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    // add value for FAILED_WRITES_CLEANER_POLICY using default key
    conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.NEVER.name());
    assertFalse(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));

    if (!HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.getAlternatives().isEmpty()) {
      conf = new Configuration();
      // add any parameter
      conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
      // add value for FAILED_WRITES_CLEANER_POLICY using alternative key
      conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.getAlternatives().get(0), HoodieFailedWritesCleaningPolicy.LAZY.name());
      assertTrue(OptionsResolver.isLazyFailedWritesCleanPolicy(conf));
    }
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    return conf;
  }
}
