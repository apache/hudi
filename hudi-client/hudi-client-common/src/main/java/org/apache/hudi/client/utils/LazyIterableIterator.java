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

import java.util.Iterator;

/**
 * (NOTE: Adapted from Apache SystemML) This class is a generic base class for lazy, single pass inputItr classes in
 * order to simplify the implementation of lazy iterators for mapPartitions use cases. Note [SPARK-3369], which gives
 * the reasons for backwards compatibility with regard to the iterable API despite Spark's single pass nature.
 * <p>
 * Provide a way to obtain a inputItr of type O (output), out of an inputItr of type I (input)
 * <p>
 * Things to remember: - Assumes Spark calls hasNext() to check for elements, before calling next() to obtain them -
 * Assumes hasNext() gets called atleast once. - Concrete Implementation is responsible for calling inputIterator.next()
 * and doing the processing in computeNext()
 */
public abstract class LazyIterableIterator<I, O> implements Iterable<O>, Iterator<O> {

  protected Iterator<I> inputItr;
  private boolean consumed = false;
  private boolean startCalled = false;
  private boolean endCalled = false;

  public LazyIterableIterator(Iterator<I> in) {
    inputItr = in;
  }

  /**
   * Called once, before any elements are processed.
   */
  protected abstract void start();

  /**
   * Block computation to be overwritten by sub classes.
   */
  protected abstract O computeNext();

  /**
   * Called once, after all elements are processed.
   */
  protected abstract void end();

  //////////////////
  // iterable implementation

  private void invokeStartIfNeeded() {
    if (!startCalled) {
      startCalled = true;
      try {
        start();
      } catch (Exception e) {
        throw new RuntimeException("Error in start()");
      }
    }
  }

  private void invokeEndIfNeeded() {
    // make the calls out to begin() & end()
    if (!endCalled) {
      endCalled = true;
      // if we are out of elements, and end has not been called yet
      try {
        end();
      } catch (Exception e) {
        throw new RuntimeException("Error in end()");
      }
    }
  }

  @Override
  public Iterator<O> iterator() {
    // check for consumed inputItr
    if (consumed) {
      throw new RuntimeException("Invalid repeated inputItr consumption.");
    }

    // hand out self as inputItr exactly once (note: do not hand out the input
    // inputItr since it is consumed by the self inputItr implementation)
    consumed = true;
    return this;
  }

  //////////////////
  // inputItr implementation

  @Override
  public boolean hasNext() {
    boolean ret = inputItr.hasNext();
    // make sure, there is exactly one call to start()
    invokeStartIfNeeded();
    if (!ret) {
      // if we are out of elements, and end has not been called yet
      invokeEndIfNeeded();
    }

    return ret;
  }

  @Override
  public O next() {
    try {
      return computeNext();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void remove() {
    throw new RuntimeException("Unsupported remove operation.");
  }
}
