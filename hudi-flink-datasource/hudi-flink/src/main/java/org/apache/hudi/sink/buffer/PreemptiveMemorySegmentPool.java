/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A {@link MemorySegmentPool} wrapper that can reclaim pages from an inactive owner when the
 * delegate pool is exhausted.
 *
 * <p>The owner represents the bucket whose buffer is currently requesting pages. The reclaimer
 * must never reclaim that owner because the request may occur in the middle of serializing a row.
 * If no other owner can release pages, this pool returns {@code null} and lets the caller handle
 * the allocation failure.
 */
public class PreemptiveMemorySegmentPool implements MemorySegmentPool, Closeable {

  /** Callback that reclaims memory from an owner other than the excluded in-flight owner. */
  @FunctionalInterface
  public interface MemoryReclaimer {
    /**
     * Reclaims memory from an inactive owner.
     *
     * @param excludedOwnerId the owner currently requesting a page
     * @return {@code true} if memory was reclaimed and allocation should be retried
     */
    boolean reclaim(String excludedOwnerId);
  }

  private final MemorySegmentPool delegate;
  private final MemoryReclaimer memoryReclaimer;

  /**
   * The owner whose buffer is currently serializing a row. This is {@code null} outside
   * {@code writeRow}, including while a new buffer is being created, so allocation failures in
   * those contexts are handled by the caller's existing fallback path.
   */
  @Nullable
  private String currentOwnerId;

  /** Prevents an allocation made from the reclamation callback from recursively reclaiming. */
  private boolean preempting;

  public PreemptiveMemorySegmentPool(
      MemorySegmentPool delegate,
      MemoryReclaimer memoryReclaimer) {
    ValidationUtils.checkArgument(delegate != null, "Delegate memory segment pool must not be null");
    ValidationUtils.checkArgument(memoryReclaimer != null, "Memory reclaimer must not be null");
    this.delegate = delegate;
    this.memoryReclaimer = memoryReclaimer;
  }

  /** Marks the owner whose buffer is currently requesting memory pages. */
  public void setCurrentOwner(String ownerId) {
    ValidationUtils.checkArgument(ownerId != null, "Memory segment pool owner must not be null");
    ValidationUtils.checkState(
        currentOwnerId == null,
        "A memory segment pool owner is already active: " + currentOwnerId);
    this.currentOwnerId = ownerId;
  }

  /** Clears the current owner after its buffer write finishes. */
  public void clearCurrentOwner() {
    this.currentOwnerId = null;
  }

  @Override
  public int pageSize() {
    return delegate.pageSize();
  }

  @Override
  public void returnAll(List<MemorySegment> memorySegments) {
    delegate.returnAll(memorySegments);
  }

  @Override
  public int freePages() {
    return delegate.freePages();
  }

  @Override
  public MemorySegment nextSegment() {
    MemorySegment segment = delegate.nextSegment();
    // Reclamation requires an in-flight owner and is disabled while a reclamation callback is
    // running to prevent recursively selecting a victim that has not yet been disposed.
    if (segment != null || currentOwnerId == null || preempting) {
      return segment;
    }

    preempting = true;
    try {
      if (!memoryReclaimer.reclaim(currentOwnerId)) {
        return null;
      }
      // Retry this allocation once. A later page request may reclaim another inactive owner,
      // while this bounded retry prevents recursion if the callback does not return any pages.
      return delegate.nextSegment();
    } finally {
      preempting = false;
    }
  }

  @Override
  public void close() throws IOException {
    if (delegate instanceof Closeable) {
      ((Closeable) delegate).close();
    }
  }
}
