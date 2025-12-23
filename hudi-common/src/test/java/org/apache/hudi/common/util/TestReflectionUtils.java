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

package org.apache.hudi.common.util;

import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.conflict.detection.EarlyConflictDetectionStrategy;
import org.apache.hudi.common.conflict.detection.TimelineServerBasedDetectionStrategy;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link ReflectionUtils}
 */
public class TestReflectionUtils {
  @Test
  public void testIsSubClass() {
    String subClassName1 = DirectMarkerBasedDetectionStrategy.class.getName();
    String subClassName2 = TimelineServerBasedDetectionStrategy.class.getName();
    assertTrue(isSubClass(subClassName1, EarlyConflictDetectionStrategy.class));
    assertTrue(isSubClass(subClassName2, EarlyConflictDetectionStrategy.class));
    assertTrue(isSubClass(subClassName2, TimelineServerBasedDetectionStrategy.class));
    assertFalse(isSubClass(subClassName2, DirectMarkerBasedDetectionStrategy.class));
  }

  @Test
  void testGetMethod() {
    assertTrue(getMethod(HoodieStorage.class, "getScheme").isPresent());
    assertTrue(getMethod(HoodieStorage.class, "listFiles", StoragePath.class).isPresent());
    assertTrue(getMethod(HoodieStorage.class,
        "listDirectEntries", StoragePath.class, StoragePathFilter.class).isPresent());
    assertFalse(getMethod(HoodieStorage.class,
        "listDirectEntries", StoragePathFilter.class).isPresent());
    assertFalse(getMethod(HoodieStorage.class, "nonExistentMethod").isPresent());
  }

  @Test
  void testGetTopLevelClassesInClasspath() {
    long classCount = ReflectionUtils.getTopLevelClassesInClasspath(ReflectionUtils.class).count();
    assertTrue(classCount > 0, "Should find at least one class in the package");
  }

  @Test
  void testGetTopLevelClassesInClasspathContainsExpectedClasses() {
    java.util.List<String> classes = ReflectionUtils.getTopLevelClassesInClasspath(ReflectionUtils.class)
        .collect(java.util.stream.Collectors.toList());

    assertTrue(classes.stream().anyMatch(c -> c.contains("ReflectionUtils")),
        "Should contain ReflectionUtils class");
  }

  @Test
  void testGetTopLevelClassesInClasspathWithArrayClass() {
    java.util.stream.Stream<String> stream = ReflectionUtils.getTopLevelClassesInClasspath(String[].class);
    assertNotNull(stream, "Should return a non-null stream");
    assertEquals(0, stream.count(), "Should return an empty stream for array classes");
  }

  @Test
  void testGetTopLevelClassesInClasspathWithPrimitiveArrayClass() {
    java.util.stream.Stream<String> stream = ReflectionUtils.getTopLevelClassesInClasspath(int[].class);
    assertNotNull(stream, "Should return a non-null stream");
    assertEquals(0, stream.count(), "Should return an empty stream for primitive array classes");
  }
}
