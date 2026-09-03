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

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    List<String> classes = ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class)
        .collect(Collectors.toList());
    assertTrue(classes.contains(TestReflectionUtils.class.getName()));
    assertTrue(classes.contains(ReflectionUtils.class.getName()));
  }

  @Test
  void testGetTopLevelClassesInClasspathHandlesIOException() {
    // Context class loader whose getResources throws, exercising the recovery path
    // where the method must return an empty stream instead of propagating.
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    ClassLoader throwingLoader = new ClassLoader(parent) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        throw new IOException("Simulated failure enumerating resources");
      }
    };
    Thread.currentThread().setContextClassLoader(throwingLoader);
    try {
      List<String> classes = ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class)
          .collect(Collectors.toList());
      assertTrue(classes.isEmpty());
    } finally {
      Thread.currentThread().setContextClassLoader(parent);
    }
  }
}
