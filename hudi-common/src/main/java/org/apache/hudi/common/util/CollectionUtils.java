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

import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectionUtils {

  public static final Properties EMPTY_PROPERTIES = new Properties();

  /**
   * Combines provided {@link List}s into one
   */
  public static <E> List<E> combine(List<E> one, List<E> another) {
    ArrayList<E> combined = new ArrayList<>(one);
    combined.addAll(another);
    return combined;
  }

  /**
   * Returns difference b/w {@code one} {@link Set} of elements and {@code another}
   */
  public static <E> Set<E> diff(Set<E> one, Set<E> another) {
    Set<E> diff = new HashSet<>(one);
    diff.removeAll(another);
    return diff;
  }

  /**
   * Returns difference b/w {@code one} {@link List} of elements and {@code another}
   *
   * NOTE: This is less optimal counterpart to {@link #diff(Set, Set)}, accepting {@link List}
   *       as a holding collection to support duplicate elements use-cases
   */
  public static <E> List<E> diff(List<E> one, List<E> another) {
    List<E> diff = new ArrayList<>(one);
    diff.removeAll(another);
    return diff;
  }

  /**
   * Determines whether two iterators contain equal elements in the same order. More specifically,
   * this method returns {@code true} if {@code iterator1} and {@code iterator2} contain the same
   * number of elements and every element of {@code iterator1} is equal to the corresponding element
   * of {@code iterator2}.
   *
   * <p>Note that this will modify the supplied iterators, since they will have been advanced some
   * number of elements forward.
   */
  public static boolean elementsEqual(Iterator<?> iterator1, Iterator<?> iterator2) {
    while (iterator1.hasNext()) {
      if (!iterator2.hasNext()) {
        return false;
      }
      Object o1 = iterator1.next();
      Object o2 = iterator2.next();
      if (!Objects.equals(o1, o2)) {
        return false;
      }
    }
    return !iterator2.hasNext();
  }

  @SafeVarargs
  public static <T> Set<T> createSet(final T... elements) {
    return Stream.of(elements).collect(Collectors.toSet());
  }

  public static <K,V> Map<K, V> createImmutableMap(final K key, final V value) {
    return Collections.unmodifiableMap(Collections.singletonMap(key, value));
  }

  @SafeVarargs
  public static <T> List<T> createImmutableList(final T... elements) {
    return Collections.unmodifiableList(Stream.of(elements).collect(Collectors.toList()));
  }

  public static <K,V> Map<K,V> createImmutableMap(final Map<K,V> map) {
    return Collections.unmodifiableMap(map);
  }

  @SafeVarargs
  public static <K,V> Map<K,V> createImmutableMap(final Pair<K,V>... elements) {
    Map<K,V> map = new HashMap<>();
    for (Pair<K,V> pair: elements) {
      map.put(pair.getLeft(), pair.getRight());
    }
    return Collections.unmodifiableMap(map);
  }

  @SafeVarargs
  public static <T> Set<T> createImmutableSet(final T... elements) {
    return Collections.unmodifiableSet(createSet(elements));
  }

  public static <T> Set<T> createImmutableSet(final Set<T> set) {
    return Collections.unmodifiableSet(set);
  }

  public static <T> List<T> createImmutableList(final List<T> list) {
    return Collections.unmodifiableList(list);
  }

  private static Object[] checkElementsNotNull(Object... array) {
    return checkElementsNotNull(array, array.length);
  }

  private static Object[] checkElementsNotNull(Object[] array, int length) {
    for (int i = 0; i < length; i++) {
      checkElementNotNull(array[i], i);
    }
    return array;
  }

  private static Object checkElementNotNull(Object element, int index) {
    return Objects.requireNonNull(element, "Element is null at index " + index);
  }
}
