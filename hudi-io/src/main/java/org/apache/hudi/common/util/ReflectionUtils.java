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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * A utility class for reflection.
 */
public class ReflectionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

  private static final Map<String, Class<?>> CLAZZ_CACHE = new ConcurrentHashMap<>();

  public static Class<?> getClass(String clazzName) {
    return CLAZZ_CACHE.computeIfAbsent(clazzName, c -> {
      try {
        return Class.forName(c);
      } catch (ClassNotFoundException e) {
        throw new HoodieException("Unable to load class", e);
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
        LOG.debug(message, e);
      } else {
        LOG.warn(message, e);
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
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return Stream.empty();
    }
    String packageName = pkg.getName();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String path = packageName.replace('.', '/');

    try {
      return Collections.list(classLoader.getResources(path)).stream()
              .flatMap(url -> getClassesFromResource(url, packageName).stream());
    } catch (IOException e) {
      LOG.error("Unable to fetch resources for package {}", packageName, e);
      return Stream.empty();
    }
  }

  /**
   * Gets classes from a resource URL, handling both file system and JAR resources.
   *
   * @param url         The resource URL
   * @param packageName The package name
   * @return List of class names
   */
  private static List<String> getClassesFromResource(URL url, String packageName) {
    String protocol = url.getProtocol();
    if ("file".equals(protocol)) {
      try {
        File directory = new File(url.toURI());
        return findClasses(directory, packageName);
      } catch (URISyntaxException e) {
        LOG.error("Unable to resolve URI: {}", url, e);
        return Collections.emptyList();
      }
    } else if ("jar".equals(protocol)) {
      return findClassesInJar(url, packageName);
    } else {
      LOG.warn("Unsupported protocol: {}", protocol);
      return Collections.emptyList();
    }
  }

  /**
   * Finds all classes in a JAR file for a given package.
   *
   * @param url         The JAR URL
   * @param packageName The package name
   * @return List of class names
   */
  private static List<String> findClassesInJar(URL url, String packageName) {
    List<String> classes = new ArrayList<>();
    String path = packageName.replace('.', '/');

    try {
      String jarPath = url.getPath();
      if (jarPath.startsWith("jar:")) {
        jarPath = jarPath.substring(4);
      }
      int separatorIndex = jarPath.indexOf("!");
      if (separatorIndex != -1) {
        jarPath = jarPath.substring(0, separatorIndex);
      }

      jarPath = URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name());

      try (JarFile jarFile = new JarFile(jarPath)) {
        jarFile.stream()
            .map(JarEntry::getName)
            .filter(name -> name.startsWith(path) && name.endsWith(".class"))
            .filter(name -> {
              String relativePath = name.substring(path.length());
              if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
              }
              return !relativePath.contains("/");
            })
            .forEach(name -> {
              String className = name.replace('/', '.').substring(0, name.length() - 6);
              classes.add(className);
            });
      }
    } catch (IOException e) {
      LOG.error("Unable to read JAR file from URL: {}", url, e);
    }

    return classes;
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
