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

import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.function.BiFunction;

/**
 * Closeable counterpart of {@link MergingIterator}
 */
public class ClosableMergingIterator<T1, T2, R> extends MergingIterator<T1, T2, R> implements ClosableIterator<R> {

  public ClosableMergingIterator(ClosableIterator<T1> leftIterator,
                                 ClosableIterator<T2> rightIterator,
                                 BiFunction<T1, T2, R> mergeFunction) {
    super(leftIterator, rightIterator, mergeFunction);
  }

  @Override
  public void close() {
    ((ClosableIterator<T1>) leftIterator).close();
    ((ClosableIterator<T2>) rightIterator).close();
  }
}
