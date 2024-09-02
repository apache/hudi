/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.sorter;

import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;

public abstract class ExternalSorter<R extends Serializable> implements AutoCloseable {
  private static final String SUBFOLDER_PREFIX = "hudi/external-sorter";

  protected final String basePath;
  protected final long maxMemoryInBytes;
  protected final Comparator<R> comparator;
  protected final SizeEstimator<R> sizeEstimator;
  protected State state = State.INIT;
  private HoodieTimer insertTimer = HoodieTimer.create();
  private long timeTakenToInsertAndWriteRecord; // ms

  enum State {
    INIT,
    ADDING,
    FINISHED,
    CLOSED
  }

  public ExternalSorter(String basePath, long maxMemoryInBytes, Comparator<R> comparator, SizeEstimator<R> sizeEstimator) throws IOException {
    this.basePath = String.format("%s/%s-%s", basePath, SUBFOLDER_PREFIX, UUID.randomUUID());
    this.maxMemoryInBytes = maxMemoryInBytes;
    this.comparator = comparator;
    this.sizeEstimator = sizeEstimator;
    initBaseFile();
  }

  public void initBaseFile() throws IOException {
    File baseDir = new File(basePath);
    FileIOUtils.deleteDirectory(baseDir);
    FileIOUtils.mkdir(baseDir);
    baseDir.deleteOnExit();
  }

  public void add(R record) {
    if (state == State.CLOSED) {
      throw new HoodieIOException("Cannot add record to a closed sorter");
    }
    if (state == State.FINISHED) {
      throw new HoodieIOException("Cannot add record to a finished sorter");
    }
    if (state == State.INIT) {
      state = State.ADDING;
      startAdd();
    }
    addInner(record);
  }

  public void addAll(Iterator<R> records) {
    if (state == State.CLOSED) {
      throw new HoodieIOException("Cannot add record to a closed sorter");
    }
    if (state == State.FINISHED) {
      throw new HoodieIOException("Cannot add record to a finished sorter");
    }
    if (state == State.INIT) {
      state = State.ADDING;
      startAdd();
    }
    addAllInner(records);
  }

  // Must call this after adding all records and before calling getIterator
  public void finish() {
    if (state == State.CLOSED) {
      throw new HoodieIOException("Cannot finish a closed sorter");
    }
    if (state == State.FINISHED) {
      return;
    }
    finishAdd();
    finishInner();
    state = State.FINISHED;
  }

  public ClosableIterator<R> getIterator() {
    if (state == State.CLOSED) {
      throw new HoodieIOException("Cannot get iterator from a closed sorter");
    }
    if (state != State.FINISHED) {
      throw new HoodieIOException("Cannot get iterator from a sorter that is not finished");
    }
    return getIteratorInner();
  }

  private void startAdd() {
    insertTimer.startTimer();
  }

  private void finishAdd() {
    timeTakenToInsertAndWriteRecord = insertTimer.endTimer();
  }

  public long getTimeTakenToInsertAndWriteRecord() {
    return timeTakenToInsertAndWriteRecord;
  }

  public abstract void closeSorter();

  protected abstract void addInner(R record);

  protected abstract void addAllInner(Iterator<R> records);

  protected abstract void finishInner();

  protected abstract ClosableIterator<R> getIteratorInner();

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    closeSorter();
    try {
      FileIOUtils.deleteDirectory(new File(basePath));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up external sorter directory", e);
    }
    state = State.CLOSED;
  }
}
