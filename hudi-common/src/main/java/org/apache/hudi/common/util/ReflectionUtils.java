/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieException;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A utility class for reflection.
 */
public class ReflectionUtils {

  private static Map<String, Class<?>> clazzCache = new HashMap<>();

  private static Class<?> getClass(String clazzName) {
    if (!clazzCache.containsKey(clazzName)) {
      try {
        Class<?> clazz = Class.forName(clazzName);
        clazzCache.put(clazzName, clazz);
      } catch (ClassNotFoundException e) {
        throw new HoodieException("Unable to load class", e);
      }
    }
    return clazzCache.get(clazzName);
  }

  public static <T> T loadClass(String fqcn) {
    try {
      return (T) getClass(fqcn).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new HoodieException("Could not load class " + fqcn, e);
    }
  }

  /**
   * Instantiate a given class with a generic record payload.
   */
  public static <T extends HoodieRecordPayload> T loadPayload(String recordPayloadClass, Object[] payloadArgs,
      Class<?>... constructorArgTypes) {
    try {
      return (T) getClass(recordPayloadClass).getConstructor(constructorArgTypes).newInstance(payloadArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate payload class ", e);
    }
  }

  /**
   * Creates an instnace of the given class. Use this version when dealing with interface types as constructor args.
   */
  public static Object loadClass(String clazz, Class<?>[] constructorArgTypes, Object... constructorArgs) {
    try {
      return getClass(clazz).getConstructor(constructorArgTypes).newInstance(constructorArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate class ", e);
    }
  }

  /**
   * Creates an instance of the given class. Constructor arg types are inferred.
   */
  public static Object loadClass(String clazz, Object... constructorArgs) {
    Class<?>[] constructorArgTypes = Arrays.stream(constructorArgs).map(Object::getClass).toArray(Class<?>[]::new);
    return loadClass(clazz, constructorArgTypes, constructorArgs);
  }

  /**
   * Return stream of top level class names in the same class path as passed-in class.
   * 
   * @param clazz
   */
  public static Stream<String> getTopLevelClassesInClasspath(Class clazz) {
    try {
      ClassPath classPath = ClassPath.from(clazz.getClassLoader());
      return classPath.getTopLevelClasses().stream().map(ClassInfo::getName);
    } catch (IOException e) {
      throw new RuntimeException("Got exception while dumping top level classes", e);
    }
  }
}
