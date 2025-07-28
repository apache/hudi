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

import org.apache.hudi.common.util.collection.ArrayComparable;

import java.util.List;
import java.util.function.Function;

/**
 * Factory class for ordering values.
 */
public class OrderingValues {
  // Please do not make this variable public and always use `OrderingValues.getDefault` instead.
  private static final Comparable<?> DEFAULT_VALUE = org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;

  /**
   * Creates an {@code OrderingValue} with given values.
   *
   * <p>CAUTION: if the values are fetched through ordering fields,
   * please use {@link #create(List, Function)} or {@link #create(String[], Function)}
   * as much as possible to avoid streaming on the ordering fields in row level.</p>
   */
  public static Comparable create(Comparable[] orderingValues) {
    if (orderingValues.length == 1) {
      return orderingValues[0];
    } else {
      return new ArrayComparable(orderingValues);
    }
  }

  /**
   * Applies the input mapping function on the input ordering fields and returns a new {@code OrderingValue} with
   * the mapped ordering field values.
   */
  public static Comparable create(List<String> orderingFields, Function<String, Comparable> fieldMappingFunction) {
    if (orderingFields.isEmpty()) {
      return DEFAULT_VALUE;
    } else if (orderingFields.size() == 1) {
      return fieldMappingFunction.apply(orderingFields.get(0));
    } else {
      Comparable[] orderingValues = new Comparable[orderingFields.size()];
      for (int i = 0; i < orderingFields.size(); i++) {
        orderingValues[i] = fieldMappingFunction.apply(orderingFields.get(i));
      }
      return new ArrayComparable(orderingValues);
    }
  }

  /**
   * Applies the input mapping function on the input ordering fields and returns a new {@code OrderingValue} with
   * the mapped ordering field values.
   */
  public static Comparable create(String[] orderingFields, Function<String, Comparable> fieldMappingFunction) {
    if (orderingFields.length == 0) {
      return DEFAULT_VALUE;
    } else if (orderingFields.length == 1) {
      return fieldMappingFunction.apply(orderingFields[0]);
    } else {
      Comparable[] orderingValues = new Comparable[orderingFields.length];
      for (int i = 0; i < orderingFields.length; i++) {
        orderingValues[i] = fieldMappingFunction.apply(orderingFields[i]);
      }
      return new ArrayComparable(orderingValues);
    }
  }

  /**
   * Returns the default {@code OrderingValue} value.
   */
  public static Comparable getDefault() {
    return DEFAULT_VALUE;
  }

  /**
   * Returns whether the given {@code orderingValue} is default.
   */
  public static boolean isDefault(Comparable orderingValue) {
    return DEFAULT_VALUE.equals(orderingValue);
  }

  /**
   * Returns whether the given two ordering values belong to the same class.
   */
  public static boolean isSameClass(Comparable val1, Comparable val2) {
    if (val1.getClass() != val2.getClass()) {
      return false;
    } else if (val1 instanceof ArrayComparable) {
      return ((ArrayComparable) val1).isValueSameClass((ArrayComparable) val2);
    }
    return true;
  }

  /**
   * Returns the ordering values as a list.
   */
  public static List<Comparable> getValues(ArrayComparable orderingValue) {
    return orderingValue.getValues();
  }
}
