/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.collection;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Space-efficient, comparable, immutable lists, copied from calcite core.
 */
public class FlatLists {
  private FlatLists() {
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list from an
   * existing list. The list is always copied.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T> List<T> of(List<T> t) {
    return of_(t);
  }

  public static <T extends Comparable> ComparableList<T> ofComparable(List<T> t) {
    return of_(t);
  }

  public static <T extends Comparable> ComparableList<T> ofComparableArray(Object[] t) {
    return ofComparable(Arrays.stream(t).map(v -> (T)v).collect(Collectors.toList()));
  }

  private static <T> ComparableList<T> of_(List<T> t) {
    return new ComparableListImpl(new ArrayList<>(t));
  }


  /** List that is also comparable.
   *
   * <p>You can create an instance whose type
   * parameter {@code T} does not extend {@link Comparable}, but you will get a
   * {@link ClassCastException} at runtime when you call
   * {@link #compareTo(Object)} if the elements of the list do not implement
   * {@code Comparable}.
   */
  public interface ComparableList<T> extends List<T>, Comparable<List> {
  }

  /** Wrapper around a list that makes it implement the {@link Comparable}
   * interface using lexical ordering. The elements must be comparable. */
  static class ComparableListImpl<T extends Comparable<T>>
      extends AbstractList<T>
      implements ComparableList<T>, KryoSerializable {
    private List<T> list;

    protected ComparableListImpl(List<T> list) {
      this.list = list;
    }

    public T get(int index) {
      return list.get(index);
    }

    public int size() {
      return list.size();
    }

    public int compareTo(List o) {
      return compare(list, o);
    }

    static <T extends Comparable<T>> int compare(List<T> list0, List<T> list1) {
      final int size0 = list0.size();
      final int size1 = list1.size();
      if (size1 == size0) {
        return compare(list0, list1, size0);
      }
      final int c = compare(list0, list1, Math.min(size0, size1));
      if (c != 0) {
        return c;
      }
      return size0 - size1;
    }

    static <T extends Comparable<T>> int compare(List<T> list0, List<T> list1, int size) {
      for (int i = 0; i < size; i++) {
        Comparable o0 = list0.get(i);
        Comparable o1 = list1.get(i);
        int c = compare(o0, o1);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }

    static <T extends Comparable<T>> int compare(T a, T b) {
      if (a == b) {
        return 0;
      }
      if (a == null) {
        return -1;
      }
      if (b == null) {
        return 1;
      }
      return a.compareTo(b);
    }

    @Override
    public void write(Kryo kryo, Output output) {
      kryo.writeClassAndObject(output, list);
    }

    @Override
    public void read(Kryo kryo, Input input) {
      list = (List<T>) kryo.readClassAndObject(input);
    }
  }

}


