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

package org.apache.hudi.io.lsm;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface RecordReader<T> extends Closeable {

  /**
   * Reads one batch. The method should return null when reaching the end of the input.
   *
   * <p>The returned iterator object and any contained objects may be held onto by the source for
   * some time, so it should not be immediately reused by the reader.
   */
  @Nullable
  Iterator<T> read() throws IOException;

  /** Closes the reader and should release all resources. */
  @Override
  void close() throws IOException;
}
