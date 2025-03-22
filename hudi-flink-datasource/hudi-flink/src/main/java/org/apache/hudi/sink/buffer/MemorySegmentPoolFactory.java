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
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

/**
 * Factory to create {@code MemorySegmentPool}, currently only heap based memory pool {@code HeapMemorySegmentPool}
 * is supported.
 *
 * <p> todo support memory segment pool based on flink managed memory, currently support heap pool only, see HUDI-9189.
 */
public class MemorySegmentPoolFactory {
  public static MemorySegmentPool createMemorySegmentPool(Configuration conf) {
    long mergeReaderMem = 100; // constant 100MB
    long mergeMapMaxMem = conf.get(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
    long maxBufferSize = (long) ((conf.get(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024);
    final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
        FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
    ValidationUtils.checkState(maxBufferSize > 0, errMsg);

    return new HeapMemorySegmentPool(conf.get(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE), maxBufferSize);
  }
}
