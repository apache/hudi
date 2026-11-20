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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.storage.StoragePath;

import java.util.Objects;

/**
 * One archive file, its size on disk, and its position in the consumption window.
 *
 * <p>The size drives the byte budget and comes free from the storage listing, so nothing has to be
 * stat-ed a second time.
 */
public final class ArchiveFile {

  private final StoragePath path;
  private final long sizeBytes;
  private volatile ConsumeState state = ConsumeState.NOT_CONSUMED;

  public ArchiveFile(StoragePath path, long sizeBytes) {
    this.path = Objects.requireNonNull(path, "path");
    if (sizeBytes < 0L) {
      throw new IllegalArgumentException("negative size " + sizeBytes + " for " + path);
    }
    this.sizeBytes = sizeBytes;
  }

  public StoragePath path() {
    return path;
  }

  public long sizeBytes() {
    return sizeBytes;
  }

  /**
   * What this file costs against the byte budget. Floored at one byte so that a zero-length file
   * still occupies a slot rather than letting the window grow without bound.
   */
  public long budgetCost() {
    return Math.max(sizeBytes, 1L);
  }

  public ConsumeState state() {
    return state;
  }

  void setState(ConsumeState state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return path + " (" + sizeBytes + " bytes, " + state + ")";
  }
}
