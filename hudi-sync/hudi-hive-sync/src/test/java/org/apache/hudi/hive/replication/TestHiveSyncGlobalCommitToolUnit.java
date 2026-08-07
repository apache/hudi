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

package org.apache.hudi.hive.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHiveSyncGlobalCommitToolUnit {

  @TempDir
  Path tempDir;

  @Test
  void commitDetectsDivergentReplicationStateAndClosesBothSides() throws Exception {
    ReplicationStateSync local = mock(ReplicationStateSync.class);
    ReplicationStateSync remote = mock(ReplicationStateSync.class);
    when(local.getClusterId()).thenReturn("LOCALSYNC");
    when(remote.getClusterId()).thenReturn("REMOTESYNC");
    when(local.replicationStateIsInSync(remote)).thenReturn(false);

    HiveSyncGlobalCommitTool tool = mock(HiveSyncGlobalCommitTool.class, CALLS_REAL_METHODS);
    setField(tool, "replicationStateSyncList", Arrays.asList(local, remote));

    assertFalse(tool.commit());
    tool.close();

    verify(local).sync();
    verify(remote).sync();
    verify(local).close();
    verify(remote).close();
  }

  @Test
  void loadParamsReadsXmlConfiguration() throws Exception {
    Path configFile = tempDir.resolve("global-sync.xml");
    Properties properties = new Properties();
    properties.setProperty("marker", "loaded");
    try (OutputStream output = Files.newOutputStream(configFile)) {
      properties.storeToXML(output, "test");
    }

    Method loadParams = HiveSyncGlobalCommitTool.class.getDeclaredMethod("loadParams", String[].class);
    loadParams.setAccessible(true);
    HiveSyncGlobalCommitParams params = (HiveSyncGlobalCommitParams) loadParams.invoke(null, (Object) new String[] {
        "--config-xml-file", configFile.toString(), "--replicated-timestamp", "100",
        "--base-path", tempDir.resolve("table").toString()
    });

    assertEquals("loaded", params.loadedProps.getProperty("marker"));
    assertEquals("100", params.globalHiveSyncConfigParams.globallyReplicatedTimeStamp);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = HiveSyncGlobalCommitTool.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
