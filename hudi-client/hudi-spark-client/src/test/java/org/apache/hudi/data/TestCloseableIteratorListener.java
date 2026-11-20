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

package org.apache.hudi.data;

import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A task that opens many iterators one after another (one per file of a scan) must not keep every exhausted one reachable
 * from its completion listeners until the task ends: {@link CloseableIteratorListener#addListenerReleasedOnExhaustion} closes
 * an iterator once it is read to its end and drops the listener's reference, while {@link CloseableIteratorListener#addListener}
 * keeps it until task completion.
 */
@Tag("functional")
public class TestCloseableIteratorListener extends HoodieClientTestBase {

  private static final int ITERATORS = 3;
  private static final Map<String, AtomicInteger> CLOSE_COUNTS = new ConcurrentHashMap<>();

  /** Runs in one task: drains {@link #ITERATORS} iterators, then reports per iterator "closed,reachable" after a full GC. */
  private static List<String> drainInATask(boolean releasedOnExhaustion, String prefix) {
    List<String> states = new ArrayList<>();
    List<WeakReference<Object>> refs = new ArrayList<>();
    List<scala.collection.Iterator<String>> readers = new ArrayList<>();
    for (int i = 0; i < ITERATORS; i++) {
      scala.collection.Iterator<String> iterator = new ClosingIterator(
          new TrackingCloseableIterator<>(prefix + i, Arrays.asList("a", "b").iterator()));
      refs.add(new WeakReference<>(iterator));
      scala.collection.Iterator<String> read = releasedOnExhaustion
          ? CloseableIteratorListener.addListenerReleasedOnExhaustion(iterator) : CloseableIteratorListener.addListener(iterator);
      if (releasedOnExhaustion) {
        readers.add(read);
      }
      List<String> values = new ArrayList<>();
      while (read.hasNext()) {
        values.add(read.next());
      }
      assertEquals(Arrays.asList("a", "b"), values);
    }
    for (int attempt = 0; attempt < 10 && refs.stream().anyMatch(ref -> ref.get() != null); attempt++) {
      System.gc();
    }
    assertEquals(releasedOnExhaustion ? ITERATORS : 0, readers.size());
    readers.forEach(read -> assertFalse(read.hasNext()));
    for (int i = 0; i < ITERATORS; i++) {
      states.add(TrackingCloseableIterator.isClosed(prefix + i) + "," + (refs.get(i).get() != null));
    }
    return states;
  }

  /** A scala iterator over a {@link TrackingCloseableIterator} that forwards {@link #close}. */
  private static final class ClosingIterator extends scala.collection.AbstractIterator<String> implements AutoCloseable {
    private final TrackingCloseableIterator<String> delegate;

    private ClosingIterator(TrackingCloseableIterator<String> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public String next() {
      return delegate.next();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  @Test
  void exhaustedIteratorIsClosedAndReleasedBeforeTheTaskEnds() {
    List<String> states = jsc.parallelize(Collections.singletonList(1), 1)
        .flatMap(x -> drainInATask(true, "released-").iterator()).collect();
    assertEquals(Collections.nCopies(ITERATORS, "true,false"), states);
  }

  @Test
  void plainListenerKeepsEveryIteratorUntilTheTaskEnds() {
    List<String> states = jsc.parallelize(Collections.singletonList(1), 1)
        .flatMap(x -> drainInATask(false, "kept-").iterator()).collect();
    // not closed before the task ends, and each iterator still reachable from its listener
    assertEquals(Collections.nCopies(ITERATORS, "false,true"), states);
  }

  @Test
  void partialConsumptionIsClosedOnceAtTaskCompletion() {
    String id = "partial";
    List<String> values = jsc.parallelize(Collections.singletonList(id), 1).map(key -> {
      scala.collection.Iterator<String> read = CloseableIteratorListener.addListenerReleasedOnExhaustion(
          new CountingIterator(key, Arrays.asList("a", "b")));
      assertTrue(read.hasNext());
      String value = read.next();
      assertEquals(0, CLOSE_COUNTS.get(key).get());
      return value;
    }).collect();
    assertEquals(Collections.singletonList("a"), values);
    assertEquals(1, CLOSE_COUNTS.remove(id).get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exhaustionThenTaskCompletionClosesOnce(boolean empty) {
    String id = "exhausted-" + empty;
    List<List<String>> values = jsc.parallelize(Collections.singletonList(id), 1).map(key -> {
      scala.collection.Iterator<String> read = CloseableIteratorListener.addListenerReleasedOnExhaustion(
          new CountingIterator(key, empty ? Collections.emptyList() : Arrays.asList("a", "b")));
      List<String> actual = new ArrayList<>();
      while (read.hasNext()) {
        actual.add(read.next());
      }
      assertFalse(read.hasNext());
      assertThrows(NoSuchElementException.class, read::next);
      assertEquals(1, CLOSE_COUNTS.get(key).get());
      return actual;
    }).collect();
    assertEquals(Collections.singletonList(empty ? Collections.emptyList() : Arrays.asList("a", "b")), values);
    assertEquals(1, CLOSE_COUNTS.remove(id).get());
  }

  @Test
  void explicitCloseThenTaskCompletionClosesOnce() {
    String id = "explicit";
    List<String> values = jsc.parallelize(Collections.singletonList(id), 1).map(key -> {
      scala.collection.Iterator<String> read = CloseableIteratorListener.addListenerReleasedOnExhaustion(
          new CountingIterator(key, Arrays.asList("a", "b")));
      assertTrue(read.hasNext());
      String value = read.next();
      ((AutoCloseable) read).close();
      ((AutoCloseable) read).close();
      assertFalse(read.hasNext());
      assertThrows(NoSuchElementException.class, read::next);
      assertEquals(1, CLOSE_COUNTS.get(key).get());
      return value;
    }).collect();
    assertEquals(Collections.singletonList("a"), values);
    assertEquals(1, CLOSE_COUNTS.remove(id).get());
  }

  private static final class CountingIterator extends scala.collection.AbstractIterator<String> implements AutoCloseable {
    private final String id;
    private final java.util.Iterator<String> values;

    private CountingIterator(String id, List<String> values) {
      this.id = id;
      this.values = values.iterator();
      CLOSE_COUNTS.put(id, new AtomicInteger());
    }

    @Override
    public boolean hasNext() {
      return values.hasNext();
    }

    @Override
    public String next() {
      return values.next();
    }

    @Override
    public void close() {
      CLOSE_COUNTS.get(id).incrementAndGet();
    }
  }
}
