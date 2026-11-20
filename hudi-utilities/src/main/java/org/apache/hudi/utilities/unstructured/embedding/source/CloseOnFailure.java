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

package org.apache.hudi.utilities.unstructured.embedding.source;

/**
 * Runs a {@link Release} of what a step opened when the step does not complete, for a try-with-resources block: whatever way
 * the block is left before {@link #succeeded()}, an exception or an Error included, the release runs, and a failure of the
 * release is suppressed into the block's failure, which stays the one thrown. After {@link #succeeded()} closing it does nothing.
 *
 * <pre>{@code
 * try (CloseOnFailure<IOException> guard = new CloseOnFailure<>(source::close)) {
 *   wrapped = new Wrapper(source);
 *   guard.succeeded();
 * }
 * }</pre>
 *
 * @param <E> the checked exception the release may throw ({@link RuntimeException} for none)
 */
public final class CloseOnFailure<E extends Exception> implements AutoCloseable {

  private final Release<E> release;
  private boolean succeeded;

  public CloseOnFailure(Release<E> release) {
    this.release = release;
  }

  /** The guarded step completed: closing releases nothing. */
  public void succeeded() {
    succeeded = true;
  }

  @Override
  public void close() throws E {
    if (!succeeded) {
      release.close();
    }
  }
}
