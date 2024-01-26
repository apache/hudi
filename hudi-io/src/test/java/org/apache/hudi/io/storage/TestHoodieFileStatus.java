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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link HoodieFileStatus}
 */
public class TestHoodieFileStatus {
  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieFileStatus.class);
  private static final long LENGTH = 100;
  private static final long MODIFICATION_TIME = System.currentTimeMillis();
  private static final String PATH1 = "/abc/xyz1";
  private static final String PATH2 = "/abc/xyz2";
  private static final HoodieLocation LOCATION1 = new HoodieLocation(PATH1);
  private static final HoodieLocation LOCATION2 = new HoodieLocation(PATH2);

  @Test
  public void testConstructor() {
    HoodieFileStatus fileStatus = new HoodieFileStatus(LOCATION1, LENGTH, false, MODIFICATION_TIME);
    validateAccessors(fileStatus, PATH1, LENGTH, false, MODIFICATION_TIME);
    fileStatus = new HoodieFileStatus(LOCATION2, -1, true, MODIFICATION_TIME + 2L);
    validateAccessors(fileStatus, PATH2, -1, true, MODIFICATION_TIME + 2L);
  }

  @Test
  public void testSerializability() throws IOException, ClassNotFoundException {
    HoodieFileStatus fileStatus = new HoodieFileStatus(LOCATION1, LENGTH, false, MODIFICATION_TIME);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(fileStatus);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           ObjectInputStream ois = new ObjectInputStream(bais)) {
        HoodieFileStatus deserialized = (HoodieFileStatus) ois.readObject();
        validateAccessors(deserialized, PATH1, LENGTH, false, MODIFICATION_TIME);
      }
    }
  }

  @Test
  public void testEquals() {
    HoodieFileStatus fileStatus1 = new HoodieFileStatus(
        new HoodieLocation(PATH1), LENGTH, false, MODIFICATION_TIME);
    HoodieFileStatus fileStatus2 = new HoodieFileStatus(
        new HoodieLocation(PATH1), LENGTH + 2, false, MODIFICATION_TIME + 2L);
    assertEquals(fileStatus1, fileStatus2);
  }

  @Test
  public void testNotEquals() {
    HoodieFileStatus fileStatus1 = new HoodieFileStatus(
        LOCATION1, LENGTH, false, MODIFICATION_TIME);
    HoodieFileStatus fileStatus2 = new HoodieFileStatus(
        LOCATION2, LENGTH, false, MODIFICATION_TIME + 2L);
    assertFalse(fileStatus1.equals(fileStatus2));
    assertFalse(fileStatus2.equals(fileStatus1));
  }

  private void validateAccessors(HoodieFileStatus fileStatus,
                                 String location,
                                 long length,
                                 boolean isDirectory,
                                 long modificationTime) {
    assertEquals(new HoodieLocation(location), fileStatus.getLocation());
    assertEquals(length, fileStatus.getLength());
    assertEquals(isDirectory, fileStatus.isDirectory());
    assertEquals(!isDirectory, fileStatus.isFile());
    assertEquals(modificationTime, fileStatus.getModificationTime());
  }
}
