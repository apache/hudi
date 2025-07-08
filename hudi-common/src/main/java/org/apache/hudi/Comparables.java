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

package org.apache.hudi;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;

public class Comparables implements Comparable, Serializable {
  protected static final long serialVersionUID = 1L;

  private final List<Comparable> comparables;

  public Comparables(List<Comparable> comparables) {
    this.comparables = comparables;
  }

  public Comparables(Comparable comparable) {
    this.comparables = Collections.singletonList(comparable);
  }

  public static boolean isDefault(Comparable orderingVal) {
    if (orderingVal instanceof Comparables) {
      return ((Comparables) orderingVal).comparables.size() == 1
          && ((Comparables) orderingVal).comparables.get(0).equals(DEFAULT_ORDERING_VALUE);
    } else {
      return orderingVal.equals(DEFAULT_ORDERING_VALUE);
    }
  }

  @Override
  public int compareTo(Object o) {
    ValidationUtils.checkArgument(o instanceof Comparables, "Comparables can only be compared with another Comparables");
    Comparables otherComparables = (Comparables) o;
    ValidationUtils.checkArgument(comparables.size() == otherComparables.comparables.size(), "Comparables should be of same size");
    for (int i = 0; i < comparables.size(); i++) {
      int comparingValue = comparables.get(i).compareTo(otherComparables.comparables.get(i));
      if (comparingValue != 0) {
        return comparingValue;
      }
    }
    return 0;
  }

  public List<Comparable> getComparables() {
    return comparables;
  }

  public Comparables apply(Function<Comparable, Comparable> comparableMapper) {
    return new Comparables(comparables.stream().map(comparable -> comparableMapper.apply(comparable)).collect(Collectors.toList()));
  }

  public boolean isEmpty() {
    return comparables.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Comparables that = (Comparables) o;
    if (comparables.size() != that.comparables.size()) {
      return false;
    }
    for (int i = 0; i < comparables.size(); i++) {
      if (!comparables.get(i).equals(that.comparables.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(comparables);
  }
}
