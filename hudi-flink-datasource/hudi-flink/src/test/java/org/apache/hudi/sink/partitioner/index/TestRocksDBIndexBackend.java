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

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test cases for {@link RocksDBIndexBackend}.
 */
public class TestRocksDBIndexBackend {

  @TempDir
  File tempFile;

  @Test
  void testGetAndUpdate() throws Exception {
    try (RocksDBIndexBackend rocksDBIndexBackend = new RocksDBIndexBackend(tempFile.getAbsolutePath())) {
      assertNull(rocksDBIndexBackend.get("id1"));

      HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("par1", "001", "file1");
      rocksDBIndexBackend.update("id1", location1);
      assertEquals(location1, rocksDBIndexBackend.get("id1"));

      HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("par2", "002", "file2");
      rocksDBIndexBackend.update("id2", location2);
      assertEquals(location2, rocksDBIndexBackend.get("id2"));
    }
  }
}
