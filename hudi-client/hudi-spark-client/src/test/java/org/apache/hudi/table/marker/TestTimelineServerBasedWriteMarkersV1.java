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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TimelineServerBasedWriteMarkersV1}
 */
public class TestTimelineServerBasedWriteMarkersV1 extends TestTimelineServerBasedWriteMarkers {
  @BeforeEach
  @Override
  public void setup() throws IOException {
    super.setup();
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(timelineService.getServerPort())
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS);
    HoodieTable table = mock(HoodieTable.class);
    when(table.getStorage()).thenReturn(storage);
    when(table.getMetaClient()).thenReturn(metaClient);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(table.getConfig()).thenReturn(writeConfig);
    when(writeConfig.getViewStorageConfig()).thenReturn(builder.build());
    this.writeMarkers = new TimelineServerBasedWriteMarkersV1(table, "000");
  }
}
