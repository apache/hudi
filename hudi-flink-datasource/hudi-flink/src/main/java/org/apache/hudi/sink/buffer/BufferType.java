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

package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Types of write buffer for buffered append write operations.
 */
@EnumDescription("Types of write buffer used by append write functions to buffer and sort records before writing to storage.")
public enum BufferType {

  @EnumFieldDescription("No buffering. Records are written directly without sorting.")
  NONE,

  @EnumFieldDescription("Bounded in-memory buffer with async write. Uses a pair of buffers where one accumulates "
      + "records while the other is being sorted and written asynchronously.")
  BOUNDED_IN_MEMORY,

  @EnumFieldDescription("Lock-free ring buffer using LMAX Disruptor. Provides better throughput for high-volume "
      + "write operations by decoupling record ingestion from sorting and writing.")
  DISRUPTOR
}
