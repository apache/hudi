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

import java.util.Iterator;

/**
 * An iterator that give a chance to release resources.
 *
 * @param <R> The return type
 */
public class CachedIterator<R> implements Iterator<R> {
  private final Iterator<R> iterator;
  private R currentRow = null;
  private boolean advanced = false;

  public CachedIterator(Iterator<R> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    advanceIfNeeded();
    return currentRow != null;
  }

  @Override
  public R next() {
    return null;
  }

  private void advanceIfNeeded() {
    if (!advanced) {
      advanced = true;
      if (iterator.hasNext()) {
        currentRow = iterator.next();
      }
    }
  }

}
