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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.hudi.TypeUtils.unsafeCast;

/**
 * A utility class for reflection.
 */
public class ReflectionUtils {

  private static final Logger LOG = LogManager.getLogger(ReflectionUtils.class);

  private static final Map<String, Class<?>> CLAZZ_CACHE = new HashMap<>();

  public static Class<?> getClass(String clazzName) {
    synchronized (CLAZZ_CACHE) {
      if (!CLAZZ_CACHE.containsKey(clazzName)) {
        try {
          Class<?> clazz = Class.forName(clazzName);
          CLAZZ_CACHE.put(clazzName, clazz);
        } catch (ClassNotFoundException e) {
          throw new HoodieException("Unable to load class", e);
        }
      }
    }
    return CLAZZ_CACHE.get(clazzName);
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
   * Creates an instance of the given class. Use this version when dealing with interface types as constructor args.
   */
  public static Object loadClass(String clazz, Class<?>[] constructorArgTypes, Object... constructorArgs) {
    return newInstanceUnchecked(getClass(clazz), constructorArgTypes, constructorArgs);
  }

  /**
   * Creates an instance of the given class. Constructor arg types are inferred.
   */
  public static Object loadClass(String clazz, Object... constructorArgs) {
    return newInstanceUnchecked(getClass(clazz), constructorArgs);
  }

  /**
   * Check if the clazz has the target constructor or not.
   *
   * When catch {@link HoodieException} from {@link #loadClass}, it's inconvenient to say if the exception was thrown
   * due to the instantiation's own logic or missing constructor.
   *
   * TODO: ReflectionUtils should throw a specific exception to indicate Reflection problem.
   */
  public static boolean hasConstructor(String clazz, Class<?>[] constructorArgTypes) {
    try {
      getClass(clazz).getConstructor(constructorArgTypes);
      return true;
    } catch (NoSuchMethodException e) {
      LOG.warn("Unable to instantiate class " + clazz, e);
      return false;
    }
  }

  /**
   * Creates a new instance of provided {@link Class} by invoking ctor identified by the
   * provided specific arguments
   *
   * @param klass target class to instantiate
   * @param ctorArgs specific constructor arguments
   * @return new instance of the class
   */
  public static <T> T newInstanceUnchecked(Class<T> klass, Object... ctorArgs) {
    Class<?>[] ctorArgTypes = Arrays.stream(ctorArgs)
        .map(arg -> Objects.requireNonNull(arg).getClass())
        .toArray(Class<?>[]::new);
    return newInstanceUnchecked(klass, ctorArgTypes, ctorArgs);
  }

  /**
   * Creates a new instance of provided {@link Class} by invoking ctor identified by the
   * provided specific arguments
   *
   * @param klass target class to instantiate
   * @param ctorArgs specific constructor arguments
   * @return new instance of the class
   */
  public static <T> T newInstanceUnchecked(Class<T> klass, Class<?>[] ctorArgTypes, Object... ctorArgs) {
    try {
      return unsafeCast(klass.getConstructor(ctorArgTypes).newInstance(ctorArgs));
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException(String.format("Unable to instantiate class %s", klass.getSimpleName()), e);
    }
  }

  /**
   * Scans all classes accessible from the context class loader
   * which belong to the given package and subpackages.
   *
   * @param clazz class
   * @return Stream of Class names in package
   */
  public static Stream<String> getTopLevelClassesInClasspath(Class<?> clazz) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String packageName = clazz.getPackage().getName();
    String path = packageName.replace('.', '/');
    Enumeration<URL> resources = null;
    try {
      resources = classLoader.getResources(path);
    } catch (IOException e) {
      LOG.error("Unable to fetch Resources in package " + e.getMessage());
    }
    List<File> directories = new ArrayList<>();
    while (Objects.requireNonNull(resources).hasMoreElements()) {
      URL resource = resources.nextElement();
      try {
        directories.add(new File(resource.toURI()));
      } catch (URISyntaxException e) {
        LOG.error("Unable to get " + e.getMessage());
      }
    }
    List<String> classes = new ArrayList<>();
    for (File directory : directories) {
      classes.addAll(findClasses(directory, packageName));
    }
    return classes.stream();
  }

  /**
   * Recursive method used to find all classes in a given directory and subdirs.
   *
   * @param directory   The base directory
   * @param packageName The package name for classes found inside the base directory
   * @return classes in the package
   */
  private static List<String> findClasses(File directory, String packageName) {
    List<String> classes = new ArrayList<>();
    if (!directory.exists()) {
      return classes;
    }
    File[] files = directory.listFiles();
    for (File file : Objects.requireNonNull(files)) {
      if (file.isDirectory()) {
        classes.addAll(findClasses(file, packageName + "." + file.getName()));
      } else if (file.getName().endsWith(".class")) {
        classes.add(packageName + '.' + file.getName().substring(0, file.getName().length() - 6));
      }
    }
    return classes;
  }

  /**
   * Returns whether the given two comparable values come from the same runtime class.
   */
  public static boolean isSameClass(Comparable<?> v, Comparable<?> o) {
    return v.getClass() == o.getClass();
  }
}
