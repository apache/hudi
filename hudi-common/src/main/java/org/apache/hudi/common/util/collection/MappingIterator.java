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

package org.apache.hudi.common.util.collection;

import java.util.Iterator;
import java.util.function.Function;

/**
 * TODO java-doc
 */
public class MappingIterator<T, R> implements Iterator<R> {
  private final Iterator<T> sourceIterator;
  private final Function<T, R> mapper;

  public MappingIterator(Iterator<T> sourceIterator, Function<T, R> mapper) {
    this.sourceIterator = sourceIterator;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return sourceIterator.hasNext();
  }

  @Override
  public R next() {
    return mapper.apply(sourceIterator.next());
  }
}
