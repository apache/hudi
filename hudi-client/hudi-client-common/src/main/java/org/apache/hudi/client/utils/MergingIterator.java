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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.Iterator;
import java.util.function.BiFunction;

public class MergingIterator<T> implements Iterator<T> {

  private final Iterator<T> leftIterator;
  private final Iterator<T> rightIterator;
  private final BiFunction<T, T, T> mergeFunction;

  public MergingIterator(Iterator<T> leftIterator, Iterator<T> rightIterator, BiFunction<T, T, T> mergeFunction) {
    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    this.mergeFunction = mergeFunction;
  }

  @Override
  public boolean hasNext() {
    boolean leftHasNext = leftIterator.hasNext();
    boolean rightHasNext = rightIterator.hasNext();
    ValidationUtils.checkArgument(leftHasNext == rightHasNext);
    return leftHasNext;
  }

  @Override
  public T next() {
    return mergeFunction.apply(leftIterator.next(), rightIterator.next());
  }
}
