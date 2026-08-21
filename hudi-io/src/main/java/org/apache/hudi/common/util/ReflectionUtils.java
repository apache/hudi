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

import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class for reflection.
 */
@Slf4j
public class ReflectionUtils {

  private static final Map<String, Class<?>> CLAZZ_CACHE = new ConcurrentHashMap<>();
  private static final String CLASS_FILE_SUFFIX = ".class";

  public static Class<?> getClass(String clazzName) {
    return CLAZZ_CACHE.computeIfAbsent(clazzName, c -> {
      try {
        return Class.forName(c);
      } catch (ClassNotFoundException e) {
        throw new HoodieException("Unable to load class " + c, e);
      }
    });
  }

  public static <T> T loadClass(String className) {
    try {
      return (T) getClass(className).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new HoodieException("Could not load class " + className, e);
    }
  }

  /**
   * Creates an instance of the given class. Use this version when dealing with interface types as constructor args.
   */
  public static Object loadClass(String clazz, Class<?>[] constructorArgTypes, Object... constructorArgs) {
    try {
      return getClass(clazz).getConstructor(constructorArgTypes).newInstance(constructorArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate class " + clazz, e);
    }
  }

  /**
   * Check if the clazz has the target constructor or not, without throwing warn-level log.
   *
   * @param clazz               Class name.
   * @param constructorArgTypes Argument types of the constructor.
   * @return {@code true} if the clazz has the target constructor, {@code false} otherwise.
   */
  public static boolean hasConstructor(String clazz, Class<?>[] constructorArgTypes) {
    return hasConstructor(clazz, constructorArgTypes, true);
  }

  /**
   * Check if the clazz has the target constructor or not.
   * <p>
   * When catch {@link HoodieException} from {@link #loadClass}, it's inconvenient to say if the exception was thrown
   * due to the instantiation's own logic or missing constructor.
   * <p>
   * TODO: ReflectionUtils should throw a specific exception to indicate Reflection problem.
   *
   * @param clazz               Class name.
   * @param constructorArgTypes Argument types of the constructor.
   * @param silenceWarning      {@code true} to use debug-level logging; otherwise, use warn-level logging.
   * @return {@code true} if the constructor exists; {@code false} otherwise.
   */
  public static boolean hasConstructor(String clazz, Class<?>[] constructorArgTypes, boolean silenceWarning) {
    try {
      getClass(clazz).getConstructor(constructorArgTypes);
      return true;
    } catch (NoSuchMethodException e) {
      String message = "Unable to instantiate class " + clazz;
      if (silenceWarning) {
        log.debug(message, e);
      } else {
        log.warn(message, e);
      }
      return false;
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
   * Scans all classes accessible from the context class loader
   * which belong to the given package and subpackages.
   *
   * @param clazz class
   * @return Stream of Class names in package
   */
  public static Stream<String> getTopLevelClassesInClasspath(Class<?> clazz) {
    // Arrays and primitives have no package, and Class#getPackage is also null when the class was
    // loaded by a loader that defines no package for it.
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return Stream.empty();
    }
    String packageName = pkg.getName();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      return Collections.list(classLoader.getResources(packageName.replace('.', '/'))).stream()
          .flatMap(resource -> classNamesIn(resource, packageName));
    } catch (IOException e) {
      log.error("Unable to fetch Resources in package {}", packageName, e);
      return Stream.empty();
    }
  }

  /**
   * Class names under a single classpath entry for the package, whether that entry is an exploded
   * directory or a jar.
   */
  private static Stream<String> classNamesIn(URL resource, String packageName) {
    String protocol = resource.getProtocol();
    if ("file".equals(protocol)) {
      return classNamesInDirectory(resource, packageName);
    } else if ("jar".equals(protocol)) {
      return classNamesInJar(resource, packageName);
    }
    log.warn("Skipping classpath entry {}, protocol {} is not supported", resource, protocol);
    return Stream.empty();
  }

  private static Stream<String> classNamesInDirectory(URL resource, String packageName) {
    Path directory;
    try {
      directory = Paths.get(resource.toURI());
    } catch (URISyntaxException e) {
      log.error("Unable to get URI for {}", resource, e);
      return Stream.empty();
    }
    if (!Files.isDirectory(directory)) {
      return Stream.empty();
    }
    try (Stream<Path> entries = Files.walk(directory)) {
      // Collected before the walk is closed, since the returned stream outlives this method.
      return entries
          .filter(entry -> entry.toString().endsWith(CLASS_FILE_SUFFIX))
          .map(entry -> toClassName(directory.relativize(entry).toString(), File.separatorChar, packageName + '.'))
          .collect(Collectors.toList())
          .stream();
    } catch (IOException e) {
      log.error("Unable to walk directory {}", directory, e);
      return Stream.empty();
    }
  }

  private static Stream<String> classNamesInJar(URL resource, String packageName) {
    // Entry names are already fully qualified paths from the jar root, so no prefix is prepended.
    String entryPrefix = packageName.replace('.', '/') + '/';
    try {
      JarURLConnection connection = (JarURLConnection) resource.openConnection();
      // Without this the jar is cached and shared, and closing it below would break other readers.
      connection.setUseCaches(false);
      try (JarFile jar = connection.getJarFile()) {
        return jar.stream()
            .map(JarEntry::getName)
            .filter(name -> name.startsWith(entryPrefix) && name.endsWith(CLASS_FILE_SUFFIX))
            .map(name -> toClassName(name, '/', ""))
            .collect(Collectors.toList())
            .stream();
      }
    } catch (IOException e) {
      log.error("Unable to read jar for {}", resource, e);
      return Stream.empty();
    }
  }

  private static String toClassName(String entry, char separator, String prefix) {
    return prefix + entry.substring(0, entry.length() - CLASS_FILE_SUFFIX.length()).replace(separator, '.');
  }

  /**
   * Invoke a static method of a class.
   * @param clazz
   * @param methodName
   * @param args
   * @param parametersType
   * @return the return value of the method
   */
  public static Object invokeStaticMethod(String clazz, String methodName, Object[] args, Class<?>... parametersType) {
    try {
      Method method = Class.forName(clazz).getMethod(methodName, parametersType);
      return method.invoke(null, args);
    } catch (ClassNotFoundException e) {
      throw new HoodieException("Unable to find the class " + clazz, e);
    } catch (NoSuchMethodException e) {
      throw new HoodieException(String.format("Unable to find the method %s of the class %s ",  methodName, clazz), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new HoodieException(String.format("Unable to invoke the method %s of the class %s ", methodName, clazz), e);
    }
  }

  /**
   * Gets a method based on the method name and type of parameters through reflection.
   *
   * @param clazz          {@link Class} object
   * @param methodName     method name
   * @param parametersType type of parameters
   * @return {@link Option} of the method if found; {@code Option.empty()} if not found or error out
   */
  public static Option<Method> getMethod(Class<?> clazz, String methodName, Class<?>... parametersType) {
    try {
      return Option.of(clazz.getMethod(methodName, parametersType));
    } catch (Throwable e) {
      return Option.empty();
    }
  }

  /**
   * Checks if the given class with the name is a subclass of another class.
   *
   * @param aClazzName Class name.
   * @param superClazz Super class to check.
   * @return {@code true} if {@code aClazzName} is a subclass of {@code superClazz};
   * {@code false} otherwise.
   */
  public static boolean isSubClass(String aClazzName, Class<?> superClazz) {
    return superClazz.isAssignableFrom(getClass(aClazzName));
  }
}
