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

package org.apache.hudi.io.storage;

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link StoragePathInfo}
 */
@Slf4j
public class TestStoragePathInfo {

  private static final long LENGTH = 100;
  private static final short BLOCK_REPLICATION = 1;
  private static final long BLOCK_SIZE = 1000000L;
  private static final long MODIFICATION_TIME = System.currentTimeMillis();
  private static final String PATH1 = "/abc/xyz1";
  private static final String PATH2 = "/abc/xyz2";
  private static final StoragePath STORAGE_PATH1 = new StoragePath(PATH1);
  private static final StoragePath STORAGE_PATH2 = new StoragePath(PATH2);

  @Test
  public void testConstructor() {
    StoragePathInfo pathInfo = new StoragePathInfo(STORAGE_PATH1, LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);
    validateAccessors(pathInfo, PATH1, LENGTH, false, MODIFICATION_TIME);
    pathInfo = new StoragePathInfo(STORAGE_PATH2, -1, true, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME + 2L);
    validateAccessors(pathInfo, PATH2, -1, true, MODIFICATION_TIME + 2L);
  }

  @Test
  public void testSerializability() throws IOException, ClassNotFoundException {
    StoragePathInfo pathInfo = new StoragePathInfo(STORAGE_PATH1, LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(pathInfo);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           ObjectInputStream ois = new ObjectInputStream(bais)) {
        StoragePathInfo deserialized = (StoragePathInfo) ois.readObject();
        validateAccessors(deserialized, PATH1, LENGTH, false, MODIFICATION_TIME);
      }
    }
  }

  @Test
  public void testCompareTo() {
    StoragePathInfo pathInfo1 = new StoragePathInfo(
        new StoragePath(PATH1), LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);
    StoragePathInfo pathInfo2 = new StoragePathInfo(
        new StoragePath(PATH1), LENGTH + 2, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME + 2L);
    StoragePathInfo pathInfo3 = new StoragePathInfo(
        new StoragePath(PATH2), LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);

    assertEquals(0, pathInfo1.compareTo(pathInfo2));
    assertEquals(-1, pathInfo1.compareTo(pathInfo3));
  }

  @Test
  public void testEquals() {
    StoragePathInfo pathInfo1 = new StoragePathInfo(
        new StoragePath(PATH1), LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);
    StoragePathInfo pathInfo2 = new StoragePathInfo(
        new StoragePath(PATH1), LENGTH + 2, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME + 2L);
    assertEquals(pathInfo1, pathInfo2);
  }

  @Test
  public void testNotEquals() {
    StoragePathInfo pathInfo1 = new StoragePathInfo(
        STORAGE_PATH1, LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME);
    StoragePathInfo pathInfo2 = new StoragePathInfo(
        STORAGE_PATH2, LENGTH, false, BLOCK_REPLICATION, BLOCK_SIZE, MODIFICATION_TIME + 2L);
    assertFalse(pathInfo1.equals(pathInfo2));
    assertFalse(pathInfo2.equals(pathInfo1));
  }

  private void validateAccessors(StoragePathInfo pathInfo,
                                 String path,
                                 long length,
                                 boolean isDirectory,
                                 long modificationTime) {
    assertEquals(new StoragePath(path), pathInfo.getPath());
    assertEquals(length, pathInfo.getLength());
    assertEquals(isDirectory, pathInfo.isDirectory());
    assertEquals(!isDirectory, pathInfo.isFile());
    assertEquals(modificationTime, pathInfo.getModificationTime());
  }
}
