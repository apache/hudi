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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
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
   * @return {@code true} if the clazz has the target constructor; {@code false} otherwise.
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
   * <p>Behaviour:
   * <ul>
   *   <li>Resources under {@code jar:} URLs (shaded bundle classpaths) are scanned
   *       through {@link JarURLConnection}. The connection's cache is disabled
   *       before the {@link JarFile} is read, since cached connections share a
   *       single backing file and would close it for every other reader in the
   *       process.</li>
   *   <li>Resources under {@code file:} URLs (exploded class directories) are
   *       walked with {@link Files#walk}.</li>
   *   <li>Inner classes (entries whose name contains {@code $}) are filtered
   *       out, matching the original behaviour.</li>
   *   <li>An {@link IOException} from {@link ClassLoader#getResources} yields an
   *       empty stream instead of propagating; the original code logged the
   *       exception and then triggered {@link NullPointerException} from the
   *       following {@code Objects.requireNonNull}.</li>
   *   <li>A class with no package (arrays, primitives) yields an empty stream
   *       instead of propagating; the original code dereferenced
   *       {@code Class#getPackage()} and threw {@link NullPointerException}.</li>
   * </ul>
   *
   * @param clazz class whose package is scanned; the class itself need not be reachable
   * @return Stream of fully-qualified class names in the package and subpackages
   */
  public static Stream<String> getTopLevelClassesInClasspath(Class<?> clazz) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      return Stream.empty();
    }
    Package pkg = clazz.getPackage();
    if (pkg == null) {
      return Stream.empty();
    }
    String packageName = pkg.getName();
    String path = packageName.replace('.', '/');
    Enumeration<URL> resources;
    try {
      resources = classLoader.getResources(path);
    } catch (IOException e) {
      log.error("Unable to fetch Resources in package {}", packageName, e);
      return Stream.empty();
    }
    return Collections.list(resources).stream()
        .flatMap(resource -> findClasses(resource, packageName));
  }

  /**
   * Finds all top-level classes in {@code packageName} reachable from a package resource URL.
   *
   * <p>Routes {@code jar:} URLs through {@link #findClassesInJar} and everything else through
   * {@link #findClassesInDirectory}.
   *
   * @param resource    the package resource URL
   * @param packageName the package whose classes should be discovered
   * @return the classes found in {@code packageName} and its subpackages, or an empty stream
   */
  private static Stream<String> findClasses(URL resource, String packageName) {
    if ("jar".equals(resource.getProtocol())) {
      return findClassesInJar(resource, packageName);
    }
    Path directory;
    try {
      directory = Paths.get(resource.toURI());
    } catch (URISyntaxException | IllegalArgumentException e) {
      log.error("Unable to get URI for {}", resource, e);
      return Stream.empty();
    }
    return findClassesInDirectory(directory, packageName);
  }

  /**
   * Scans a JAR for class entries under {@code packageName} and its subpackages.
   *
   * <p>{@code JarURLConnection} instances are cached by default; closing the
   * {@link JarFile} returned by a cached connection closes it for every other
   * reader in the same JVM. Disabling the cache here keeps the JAR open for
   * concurrent readers and leaves the lifecycle to the caller.
   *
   * @param resource    a {@code jar:} URL whose {@link JarURLConnection} identifies a JAR entry path
   * @param packageName the package whose classes should be discovered
   * @return the classes found in {@code packageName} and its subpackages, or an empty stream
   */
  private static Stream<String> findClassesInJar(URL resource, String packageName) {
    String prefix = packageName.replace('.', '/') + "/";
    JarURLConnection connection;
    try {
      connection = (JarURLConnection) resource.openConnection();
      connection.setUseCaches(false);
    } catch (IOException e) {
      log.error("Unable to open JAR resource {} for package {}", resource, packageName, e);
      return Stream.empty();
    }
    JarFile jarFile;
    try {
      jarFile = connection.getJarFile();
    } catch (IOException e) {
      log.error("Unable to read JAR resource {} for package {}", resource, packageName, e);
      return Stream.empty();
    }
    List<String> classes = new ArrayList<>();
    Enumeration<JarEntry> entries = jarFile.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String name = entry.getName();
      if (name.startsWith(prefix) && name.endsWith(".class") && !name.contains("$")) {
        String className = name.substring(0, name.length() - ".class".length()).replace('/', '.');
        classes.add(className);
      }
    }
    return classes.stream();
  }

  /**
   * Walks an exploded class directory for class entries under {@code packageName} and its subpackages.
   *
   * @param directory   the base directory corresponding to the package root
   * @param packageName the package whose classes should be discovered
   * @return the classes found in {@code packageName} and its subpackages, or an empty stream
   */
  private static Stream<String> findClassesInDirectory(Path directory, String packageName) {
    if (!Files.isDirectory(directory)) {
      return Stream.empty();
    }
    String prefix = packageName + ".";
    try (Stream<Path> paths = Files.walk(directory)) {
      return paths
          .filter(Files::isRegularFile)
          .map(p -> p.getFileName().toString())
          .filter(name -> name.endsWith(".class"))
          .filter(name -> !name.contains("$"))
          .map(name -> prefix + name.substring(0, name.length() - ".class".length()))
          .collect(Collectors.toList())
          .stream();
    } catch (IOException e) {
      log.error("Unable to walk {} for package {}", directory, packageName, e);
      return Stream.empty();
    }
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
