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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

import java.util.Locale;

/**
 * Different concurrency modes for write operations.
 */
@EnumDescription("Concurrency modes for write operations.")
public enum WriteConcurrencyMode {
  // Only a single writer can perform write ops
  @EnumFieldDescription("Only one active writer to the table. Maximizes throughput.")
  SINGLE_WRITER,


  // Multiple writer can perform write ops with lazy conflict resolution using locks
  @EnumFieldDescription("Multiple writers can operate on the table with lazy conflict resolution "
      + "using locks. This means that only one writer succeeds if multiple writers write to the "
      + "same file group.")
  OPTIMISTIC_CONCURRENCY_CONTROL,

  // Multiple writer can perform write ops on a MOR table with non-blocking conflict resolution
  @EnumFieldDescription("Multiple writers can operate on the table with non-blocking conflict resolution. "
      + "The writers can write into the same file group with the conflicts resolved automatically "
      + "by the query reader and the compactor.")
  NON_BLOCKING_CONCURRENCY_CONTROL;

  public boolean supportsMultiWriter() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL || this == NON_BLOCKING_CONCURRENCY_CONTROL;
  }

  public static boolean supportsMultiWriter(String name) {
    return WriteConcurrencyMode.valueOf(name.toUpperCase(Locale.ROOT)).supportsMultiWriter();
  }

  public boolean isOptimisticConcurrencyControl() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL;
  }

  public boolean isNonBlockingConcurrencyControl() {
    return this == NON_BLOCKING_CONCURRENCY_CONTROL;
  }

  public static boolean isNonBlockingConcurrencyControl(String name) {
    return WriteConcurrencyMode.valueOf(name.toUpperCase()).isNonBlockingConcurrencyControl();
  }
}
