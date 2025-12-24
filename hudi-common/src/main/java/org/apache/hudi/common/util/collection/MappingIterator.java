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
 * Iterator mapping elements of the provided source {@link Iterator} from {@code I} to {@code O}
 * TODO zhangyue143 这里能够基于Disruptor 异步化来提升IO性能
 */
public class MappingIterator<I, O> implements Iterator<O> {

  public static <I, O> MappingIterator<I, O> transform(Iterator<I> source, Function<I, O> mapper) {
    return new MappingIterator(source, mapper);
  }

  protected final Iterator<I> source;
  private final Function<I, O> mapper;

  public MappingIterator(Iterator<I> source, Function<I, O> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public O next() {
    return mapper.apply(source.next());
  }
}
