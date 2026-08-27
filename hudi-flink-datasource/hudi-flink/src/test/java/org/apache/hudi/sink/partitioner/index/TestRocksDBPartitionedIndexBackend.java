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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RocksDBPartitionedIndexBackend}.
 */
public class TestRocksDBPartitionedIndexBackend {

  @TempDir
  File tempFile;

  @Test
  void testGetOnUnknownPartitionReturnsNull() throws Exception {
    try (RocksDBPartitionedIndexBackend backend = new RocksDBPartitionedIndexBackend(tempFile.getAbsolutePath())) {
      assertNull(backend.get("par1", "key1"));
      assertFalse(backend.isPartitionRegistered("par1"));
      assertTrue(backend.listRegisteredPartitions().isEmpty());
    }
  }

  @Test
  void testUpdateAndGetRoundTripAcrossPartitions() throws Exception {
    try (RocksDBPartitionedIndexBackend backend = new RocksDBPartitionedIndexBackend(tempFile.getAbsolutePath())) {
      backend.update("par1", "key1", "file1");
      backend.update("par2", "key1", "file2");

      assertEquals("file1", backend.get("par1", "key1"));
      assertEquals("file2", backend.get("par2", "key1"));
      assertNull(backend.get("par1", "key2"));
      assertNull(backend.get("par3", "key1"));
    }
  }

  @Test
  void testCompletenessInvariantRegistersPartitionAfterUpdate() throws Exception {
    try (RocksDBPartitionedIndexBackend backend = new RocksDBPartitionedIndexBackend(tempFile.getAbsolutePath())) {
      assertFalse(backend.isPartitionRegistered("par1"));

      backend.update("par1", "key1", "file1");
      assertTrue(backend.isPartitionRegistered("par1"));
      assertEquals(1, backend.listRegisteredPartitions().size());
      assertTrue(backend.listRegisteredPartitions().contains("par1"));

      // Repeated updates to an already-registered partition should not create duplicate registry entries.
      backend.update("par1", "key2", "file1");
      assertEquals(1, backend.listRegisteredPartitions().size());
    }
  }

  @Test
  void testDeletePartitionRemovesDataAndRegistration() throws Exception {
    try (RocksDBPartitionedIndexBackend backend = new RocksDBPartitionedIndexBackend(tempFile.getAbsolutePath())) {
      backend.update("par1", "key1", "file1");
      assertTrue(backend.isPartitionRegistered("par1"));

      backend.deletePartition("par1");

      assertFalse(backend.isPartitionRegistered("par1"));
      assertTrue(backend.listRegisteredPartitions().isEmpty());
      assertNull(backend.get("par1", "key1"));

      // Deleting an already-absent partition should be a no-op, not an error.
      backend.deletePartition("par1");
    }
  }
}
