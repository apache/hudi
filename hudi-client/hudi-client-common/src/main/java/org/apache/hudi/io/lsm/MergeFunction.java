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

package org.apache.hudi.io.lsm;

import org.apache.hudi.common.model.HoodieRecord;

/**
 * Merge function to merge multiple {@link KeyValue}s.
 *
 * <p>IMPORTANT, Object reusing inside the kv of the {@link #add} input:
 *
 * <ul>
 *   <li>Please don't save KeyValue and InternalRow references to the List: the KeyValue of the
 *       first two objects and the InternalRow object inside them are safe, but the reference of the
 *       third object may overwrite the reference of the first object.
 *   <li>You can save fields references: fields don't reuse their objects.
 * </ul>
 *
 * @param <T> result type
 */
public interface MergeFunction<T> {
  /** Reset the merge function to its default state. */
  void reset();

  /** Add the given {@link KeyValue} to the merge function. */
  void add(HoodieRecord kv);

  /** Get current merged value. */
  T getResult();
}
