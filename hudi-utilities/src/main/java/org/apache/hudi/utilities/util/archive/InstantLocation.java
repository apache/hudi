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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import java.util.Objects;

/** How one target instant resolved, and where it was found if it was. */
public final class InstantLocation {

  private static final InstantLocation NOT_FOUND =
      new InstantLocation(InstantSearchState.NOT_FOUND, Option.empty());
  private static final InstantLocation INDETERMINATE =
      new InstantLocation(InstantSearchState.INDETERMINATE, Option.empty());

  private final InstantSearchState state;
  private final Option<StoragePath> archiveFile;

  private InstantLocation(InstantSearchState state, Option<StoragePath> archiveFile) {
    this.state = state;
    this.archiveFile = archiveFile;
  }

  /** @param archiveFile the archive file the instant was located in; never null */
  public static InstantLocation found(StoragePath archiveFile) {
    return new InstantLocation(InstantSearchState.FOUND,
        Option.of(Objects.requireNonNull(archiveFile, "archiveFile")));
  }

  public static InstantLocation notFound() {
    return NOT_FOUND;
  }

  public static InstantLocation indeterminate() {
    return INDETERMINATE;
  }

  public InstantSearchState state() {
    return state;
  }

  /** Present only when the state is {@link InstantSearchState#FOUND}. */
  public Option<StoragePath> archiveFile() {
    return archiveFile;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InstantLocation)) {
      return false;
    }
    InstantLocation that = (InstantLocation) other;
    return state == that.state && Objects.equals(archiveFile, that.archiveFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, archiveFile);
  }

  @Override
  public String toString() {
    return state + (archiveFile.isPresent() ? " in " + archiveFile.get() : "");
  }
}
