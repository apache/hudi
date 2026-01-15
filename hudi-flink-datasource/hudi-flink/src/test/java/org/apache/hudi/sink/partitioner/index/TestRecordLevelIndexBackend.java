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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link RecordLevelIndexBackend}.
 */
public class TestRecordLevelIndexBackend {

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, COPY_ON_WRITE.name());
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testRecordLevelIndexBackend() throws Exception {
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    String firstCommitTime = TestUtils.getLastCompleteInstant(tempFile.toURI().toString());

    try (RecordLevelIndexBackend recordLevelIndexBackend = new RecordLevelIndexBackend(conf, -1)) {
      // get record location
      HoodieRecordGlobalLocation location = recordLevelIndexBackend.get("id1");
      assertNotNull(location);
      assertEquals("par1", location.getPartitionPath());
      assertEquals(firstCommitTime, location.getInstantTime());

      // get record location with non existed key
      location = recordLevelIndexBackend.get("new_key");
      assertNull(location);

      // get records locations for multiple record keys
      Map<String, HoodieRecordGlobalLocation> locations = recordLevelIndexBackend.get(Arrays.asList("id1", "id2", "id3"));
      assertEquals(3, locations.size());
      locations.values().forEach(Assertions::assertNotNull);

      // get records locations for multiple record keys with unexisted key
      locations = recordLevelIndexBackend.get(Arrays.asList("id1", "id2", "new_key"));
      assertEquals(3, locations.size());
      assertNull(locations.get("new_key"));

      // new checkpoint
      recordLevelIndexBackend.onCheckpoint(1);

      // update record location
      HoodieRecordGlobalLocation newLocation = new HoodieRecordGlobalLocation("par5", "1003", "file_id_4");
      recordLevelIndexBackend.update("new_key", newLocation);
      location = recordLevelIndexBackend.get("new_key");
      assertEquals(newLocation, location);

      // previous instant commit success, clean
      Correspondent correspondent = mock(Correspondent.class);
      Map<Long, String> inFlightInstants = new HashMap<>();
      inFlightInstants.put(1L, "0001");
      when(correspondent.requestInFlightInstants()).thenReturn(inFlightInstants);
      recordLevelIndexBackend.onCheckpointComplete(correspondent);
      assertEquals(1, recordLevelIndexBackend.getRecordIndexCache().getCaches().size());
      // the cache will only contain 'new_key', others are cleaned.
      location = recordLevelIndexBackend.getRecordIndexCache().get("new_key");
      assertEquals(newLocation, location);
      location = recordLevelIndexBackend.getRecordIndexCache().get("id1");
      assertNull(location);
    }
  }
}
