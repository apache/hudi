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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A utility class for reflection.
 */
public class ReflectionUtils {

  private static final Logger LOG = LogManager.getLogger(ReflectionUtils.class);

  private static final Cache<String, Class<?>> LOADED_CLASSES_CACHE =
      Caffeine.newBuilder()
          .maximumSize(2000)
          .weakValues()
          .expireAfterAccess(Duration.of(5, ChronoUnit.MINUTES))
          .build();

  /**
   * Tries to load class identified by provide {@code qualifiedName}.
   * Throws {@link HoodieException} in case class couldn't be loaded
   *
   * @param qualifiedName target class's qualified name
   * @return instance of {@link Class} corresponding to the class identified by the {@code qualifiedName}
   */
  public static Class<?> getClass(String qualifiedName) {
    Class<?> klass = getClassNoThrow(qualifiedName);
    if (klass != null) {
      return klass;
    }

    throw new HoodieException(String.format("Unable to load class '%s'", qualifiedName));
  }

  /**
   * Tries to load class identified by provide {@code qualifiedName}.
   * Returns null in case class couldn't be loaded
   *
   * @param qualifiedName target class's qualified name
   * @return instance of {@link Class} corresponding to the class identified by the {@code qualifiedName}
   */
  public static Class<?> getClassNoThrow(String qualifiedName) {
    return LOADED_CLASSES_CACHE.get(qualifiedName, name -> {
      try {
        return Class.forName(qualifiedName);
      } catch (ClassNotFoundException e) {
        LOG.info(String.format("Failed to load class '%s'", qualifiedName));
        return null;
      }
    });
  }

  /**
   * Creates new instance of the class identified by {@code classQualifiedName}.
   * Will throw in case class couldn't be loaded or instantiated
   */
  public static <T> T newInstance(String classQualifiedName) {
    try {
      return (T) getClass(classQualifiedName).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new HoodieException(String.format("Failed to create new instance for class '%s'", classQualifiedName), e);
    }
  }

  /**
   * Creates new instance of the class identified by {@code classQualifiedName}.
   * Returns null in case class couldn't be loaded or instantiated
   */
  public static <T> T newInstanceNoThrow(String classQualifiedName) {
    try {
      Class<?> klass = getClassNoThrow(classQualifiedName);
      return klass != null ? (T) klass.newInstance() : null;
    } catch (InstantiationException | IllegalAccessException e) {
      LOG.info(String.format("Failed to create new instance for class '%s'", classQualifiedName));
      return null;
    }
  }

  /**
   * Creates new instance of the class identified by {@code classQualifiedName}.
   * Will throw in case class couldn't be loaded or instantiated
   *
   * @param classQualifiedName qualified name of the target class
   * @param constructorArgTypes corresponding types of the arguments of class' ctor that should be invoked
   * @param constructorArgs argument values to be passed into the target ctor
   * @return new instance of the target class
   */
  public static Object newInstance(String classQualifiedName, Class<?>[] constructorArgTypes, Object... constructorArgs) {
    try {
      return getClass(classQualifiedName).getConstructor(constructorArgTypes).newInstance(constructorArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate class " + classQualifiedName, e);
    }
  }

  /**
   * Creates new instance of the class identified by {@code classQualifiedName}.
   * Will throw in case class couldn't be loaded or instantiated
   *
   * @param classQualifiedName qualified name of the target class
   * @param constructorArgs argument values to be passed into the target ctor
   * @return new instance of the target class
   */
  public static Object newInstance(String classQualifiedName, Object... constructorArgs) {
    Class<?>[] constructorArgTypes = Arrays.stream(constructorArgs).map(Object::getClass).toArray(Class<?>[]::new);
    return newInstance(classQualifiedName, constructorArgTypes, constructorArgs);
  }

  /**
   * @deprecated use {@link #newInstance(String)} instead
   */
  @Deprecated
  public static <T> T loadClass(String className) {
    return newInstance(className);
  }

  /**
   * Creates an instance of the given class. Use this version when dealing with interface types as constructor args.
   * @deprecated
   */
  @Deprecated
  public static Object loadClass(String clazz, Class<?>[] constructorArgTypes, Object... constructorArgs) {
    try {
      return getClass(clazz).getConstructor(constructorArgTypes).newInstance(constructorArgs);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new HoodieException("Unable to instantiate class " + clazz, e);
    }
  }

  /**
   * Creates an instance of the given class. Constructor arg types are inferred.
   * @deprecated
   */
  @Deprecated
  public static Object loadClass(String clazz, Object... constructorArgs) {
    Class<?>[] constructorArgTypes = Arrays.stream(constructorArgs).map(Object::getClass).toArray(Class<?>[]::new);
    return loadClass(clazz, constructorArgTypes, constructorArgs);
  }

  /**
   * Check if the class has the target constructor or not.
   *
   * When catch {@link HoodieException} from {@link #loadClass}, it's inconvenient to say if the exception was thrown
   * due to the instantiation's own logic or missing constructor.
   *
   * TODO: ReflectionUtils should throw a specific exception to indicate Reflection problem.
   */
  public static boolean hasConstructor(String className, Class<?>[] constructorArgTypes) {
    try {
      getClass(className).getConstructor(constructorArgTypes);
      return true;
    } catch (NoSuchMethodException e) {
      LOG.info(String.format("No ctor found in class '%s'", className));
      return false;
    }
  }

  /**
   * Checks whether target class has a method w/ {@code methodName} and list of arguments
   * designated by {@code paramTypes}
   */
  public static boolean hasMethod(Class<?> klass, String methodName, Class<?>... paramTypes) {
    try {
      klass.getMethod(methodName, paramTypes);
      return true;
    } catch (NoSuchMethodException e) {
      LOG.info(String.format("No method %s found in class '%s'", methodName, klass.getName()));
      return false;
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

  /**
   * Invokes a non-static method of a class on provided object
   */
  public static <O, R> R invokeMethod(O obj, String methodName, Object[] args, Class<?>... paramTypes) {
    Class<?> klass = obj.getClass();
    try {
      Method method = klass.getMethod(methodName, paramTypes);
      return (R) method.invoke(obj, args);
    } catch (NoSuchMethodException e) {
      throw new HoodieException(String.format("Unable to find the method '%s' of the class '%s'",  methodName, klass.getName()), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new HoodieException(String.format("Unable to invoke the method '%s' of the class '%s'",  methodName, klass.getName()), e);
    }
  }

  /**
   * Invokes a static method of a class
   */
  public static Object invokeStaticMethod(String clazz, String methodName, Object[] args, Class<?>... parametersType) {
    try {
      Method method = getClass(clazz).getMethod(methodName, parametersType);
      return method.invoke(null, args);
    } catch (NoSuchMethodException e) {
      throw new HoodieException(String.format("Unable to find the method '%s' of the class '%s'",  methodName, clazz), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new HoodieException(String.format("Unable to invoke the method '%s' of the class '%s'",  methodName, clazz), e);
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
