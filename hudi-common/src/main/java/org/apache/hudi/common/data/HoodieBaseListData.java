/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.data;

import org.apache.hudi.common.util.Either;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data representation of either a stream or a list of objects with Type T.
 *
 * @param <T> Object value type.
 */
public abstract class HoodieBaseListData<T> {

  protected final Either<Stream<T>, List<T>> data;
  protected final boolean lazy;

  protected HoodieBaseListData(List<T> data, boolean lazy) {
    this.data = lazy ? Either.left(data.stream().parallel()) : Either.right(data);
    this.lazy = lazy;
  }

  protected HoodieBaseListData(Stream<T> dataStream, boolean lazy) {
    // NOTE: In case this container is being instantiated by an eager parent, we have to
    //       pre-materialize the stream
    if (lazy) {
      this.data = Either.left(dataStream);
    } else {
      this.data = Either.right(dataStream.collect(Collectors.toList()));
      dataStream.close();
    }
    this.lazy = lazy;
  }

  protected Stream<T> asStream() {
    return lazy ? data.asLeft() : data.asRight().parallelStream();
  }

  protected boolean isEmpty() {
    if (lazy) {
      return !data.asLeft().findAny().isPresent();
    } else {
      return data.asRight().isEmpty();
    }
  }

  protected long count() {
    if (lazy) {
      return data.asLeft().count();
    } else {
      return data.asRight().size();
    }
  }

  protected List<T> collectAsList() {
    if (lazy) {
      try (Stream<T> stream = data.asLeft()) {
        return stream.collect(Collectors.toList());
      }
    } else {
      return data.asRight();
    }
  }

  static class IteratorCloser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IteratorCloser.class);
    private final Iterator<?> iterator;

    IteratorCloser(Iterator<?> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void run() {
      if (iterator instanceof AutoCloseable) {
        try {
          ((AutoCloseable) iterator).close();
        } catch (Exception ex) {
          LOG.warn("Failed to properly close iterator", ex);
        }
      }
    }
  }
}
