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
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    // Arrays and primitives have no package, and Class#getPackage is also null when the class was
    // loaded by a loader that defines no package for it.
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return Stream.empty();
    }
    String packageName = pkg.getName();
    String path = packageName.replace('.', '/');
    try {
      return Collections.list(classLoader.getResources(path)).stream()
          .flatMap(resource -> classNamesIn(resource, packageName));
    } catch (IOException e) {
      log.error("Unable to fetch Resources in package {}", packageName, e);
      return Stream.empty();
    }
  }

  /**
   * Class names under a single classpath entry for the package, whether that entry is an exploded
   * directory or a jar.
   *
   * <p>A jar entry cannot go through {@link #toDirectory}: a {@code jar:} URL is non-hierarchical,
   * so {@code new File(uri)} throws and the entry would be dropped. Every bundle {@code Main} class
   * runs from inside a shaded jar, so that path has to be read through the jar connection instead.
   *
   * @param resource    a classpath entry holding the package
   * @param packageName the package being scanned
   * @return class names found under that entry, empty if it cannot be read
   */
  private static Stream<String> classNamesIn(URL resource, String packageName) {
    if ("jar".equals(resource.getProtocol())) {
      return classNamesInJar(resource, packageName);
    }
    File directory = toDirectory(resource);
    return directory == null ? Stream.empty() : findClasses(directory, packageName).stream();
  }

  /**
   * Class names under the package inside a jar, read through the jar connection.
   *
   * @param resource    a {@code jar:} classpath entry holding the package
   * @param packageName the package being scanned
   * @return class names found in that jar, empty if the jar cannot be read
   */
  private static Stream<String> classNamesInJar(URL resource, String packageName) {
    try {
      URLConnection connection = resource.openConnection();
      if (!(connection instanceof JarURLConnection)) {
        // A jar: URL served by a non-JDK stream handler. Skip it rather than let the cast throw,
        // since this method exists to stop such an entry from failing the whole scan.
        log.warn("Skipping classpath entry {}, {} is not a JarURLConnection", resource, connection.getClass());
        return Stream.empty();
      }
      JarURLConnection jarConnection = (JarURLConnection) connection;
      // Without this the JarFile is cached and shared JVM-wide, and closing it below would leave
      // any reader that opened the same jar first with "IllegalStateException: zip file closed".
      jarConnection.setUseCaches(false);
      // Derived from the package rather than from JarURLConnection#getEntryName. On a multi-release
      // jar the loader resolves the package to META-INF/versions/N/<pkg>/ on JDK 9-23 but to <pkg>/
      // on 8 and 24+, so anchoring to the entry name would make the result JDK dependent and would
      // drop classes that exist only in the base directory.
      String entryPrefix = packageName.replace('.', '/') + '/';
      try (JarFile jar = jarConnection.getJarFile()) {
        // Collected before the jar is closed, since the returned stream outlives this method.
        return jar.stream()
            .map(JarEntry::getName)
            .filter(name -> name.startsWith(entryPrefix) && name.endsWith(CLASS_FILE_SUFFIX))
            .map(name -> name.substring(0, name.length() - CLASS_FILE_SUFFIX.length()).replace('/', '.'))
            .collect(Collectors.toList())
            .stream();
      }
    } catch (IOException e) {
      log.error("Unable to read jar for {}", resource, e);
      return Stream.empty();
    }
  }

  /**
   * Converts a package resource {@link URL} to a {@link File} directory, or {@code null} if the URI is malformed or does not represent a file.
   *
   * @param resource the package resource URL
   * @return the corresponding directory, or {@code null} if conversion fails
   */
  private static File toDirectory(URL resource) {
    try {
      return new File(resource.toURI());
    } catch (URISyntaxException | IllegalArgumentException e) {
      log.error("Unable to get URI for {}", resource, e);
      return null;
    }
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
    if (files == null) {
      // Null for an unreadable directory, or for a package path that is a regular file. Skipping it
      // keeps one bad classpath entry from failing the whole scan, as the jar branch above does.
      log.warn("Unable to list {}, skipping it", directory);
      return classes;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        classes.addAll(findClasses(file, packageName + "." + file.getName()));
      } else if (file.getName().endsWith(CLASS_FILE_SUFFIX)) {
        classes.add(packageName + '.'
            + file.getName().substring(0, file.getName().length() - CLASS_FILE_SUFFIX.length()));
      }
    }
    return classes;
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
