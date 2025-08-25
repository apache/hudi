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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ReflectionUtils}
 */
public class TestReflectionUtils {

  private ClassLoader originalClassLoader;
  private URLClassLoader customClassLoader;

  @BeforeEach
  public void setUp() {
    originalClassLoader = Thread.currentThread().getContextClassLoader();
    // Create a custom class loader for testing
    customClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
  }

  @AfterEach
  public void tearDown() {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
    if (customClassLoader != null) {
      try {
        customClassLoader.close();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }

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
  public void testThreadContextClassLoaderWithNonSystemClass() throws NoSuchFieldException, IllegalAccessException {
    // Use a class that exists in the current context but simulate it's not in system loader
    String testClassName = "org.apache.hudi.test.CustomTestClass";

    // Create a custom class loader that can load our test class
    ClassLoader isolatedLoader = new ClassLoader(getClass().getClassLoader()) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name.equals(testClassName)) {
          // Create a simple class that just extends Object
          byte[] classBytes = createSimpleClassBytes(name);
          return defineClass(name, classBytes, 0, classBytes.length);
        }
        // For all other classes, delegate to parent
        return super.loadClass(name);
      }
    };

    // Reset the cached value before each test part
    Field useThreadContextField = ReflectionUtils.class.getDeclaredField("useThreadContextClassLoader");
    useThreadContextField.setAccessible(true);

    // Test with hoodie.reflection.usethreadcontext=true
    // Since DFSPropertiesConfiguration is not available in this module,
    // ReflectionUtils will default to false, but we can test thread context behavior
    // by directly setting the field for this test
    useThreadContextField.set(null, null);
    
    // Directly set the field to true to test thread context behavior
    useThreadContextField.set(null, true);

    ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(isolatedLoader);
    clearReflectionUtilsCache();

    try {
      // This should succeed because thread context class loader is used
      Class<?> clazz = ReflectionUtils.getClass(testClassName);
      assertNotNull(clazz);
      assertEquals(testClassName, clazz.getName());
    } finally {
      Thread.currentThread().setContextClassLoader(originalTCCL);
    }

    // Test with hoodie.reflection.usethreadcontext=false
    // Since DFSPropertiesConfiguration is not available in this module,
    // ReflectionUtils will default to false. We can directly set the field
    // to test the false behavior
    useThreadContextField.set(null, null);
    
    // Directly set the field to false to test non-thread context behavior
    useThreadContextField.set(null, false);

    clearReflectionUtilsCache();

    // This should always fail when thread context is not used
    assertThrows(HoodieException.class, () -> ReflectionUtils.getClass("org.apache.hudi.test.CustomTestClass"));
  }

  @Test
  public void testGetClassWithSystemClassLoader() {
    // Test loading a standard JDK class
    Class<?> clazz = ReflectionUtils.getClass("java.lang.String");
    assertNotNull(clazz);
    assertEquals(String.class, clazz);
  }

  @Test
  public void testGetClassWithInvalidClassName() {
    assertThrows(HoodieException.class, () -> ReflectionUtils.getClass("com.nonexistent.InvalidClass"));
  }

  private byte[] createSimpleClassBytes(String className) {
    // Convert class name to internal format (replace . with /)
    String internalName = className.replace('.', '/');

    // This is a minimal valid class file structure
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    try {
      // Magic number
      dos.writeInt(0xCAFEBABE);

      // Version (Java 8: minor=0, major=52)
      dos.writeShort(0); // minor version
      dos.writeShort(52); // major version

      // Constant pool count (we need at least 5 entries)
      dos.writeShort(5);

      // Constant pool:
      // #1 = Class reference to this class
      dos.writeByte(7); // CONSTANT_Class
      dos.writeShort(2); // name_index pointing to #2

      // #2 = UTF8 string with class name
      dos.writeByte(1); // CONSTANT_Utf8
      dos.writeUTF(internalName);

      // #3 = Class reference to Object
      dos.writeByte(7); // CONSTANT_Class
      dos.writeShort(4); // name_index pointing to #4

      // #4 = UTF8 string "java/lang/Object"
      dos.writeByte(1); // CONSTANT_Utf8
      dos.writeUTF("java/lang/Object");

      // Access flags (ACC_PUBLIC | ACC_SUPER)
      dos.writeShort(0x0021);

      // This class (index to constant pool entry #1)
      dos.writeShort(1);

      // Super class (index to constant pool entry #3)
      dos.writeShort(3);

      // Interfaces count
      dos.writeShort(0);

      // Fields count
      dos.writeShort(0);

      // Methods count
      dos.writeShort(0);

      // Attributes count
      dos.writeShort(0);

      dos.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to generate class bytes", e);
    }
  }

  private void clearReflectionUtilsCache() {
    try {
      java.lang.reflect.Field cacheField = ReflectionUtils.class.getDeclaredField("CLAZZ_CACHE");
      cacheField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.Map<String, Class<?>> cache = (java.util.Map<String, Class<?>>) cacheField.get(null);
      cache.clear();
    } catch (NoSuchFieldException e) {
      // If the field doesn't exist, the implementation might have changed
      // Log a warning or decide how to handle this
      System.err.println("Warning: Unable to find CLAZZ_CACHE field in ReflectionUtils");
    } catch (IllegalAccessException e) {
      // If we can't access the field, log but don't fail the test
      System.err.println("Warning: Unable to access CLAZZ_CACHE field in ReflectionUtils");
    }
  }
}