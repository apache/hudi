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

package org.apache.hudi.sink.append;

import org.apache.hudi.common.util.queue.BufferType;
import org.apache.hudi.configuration.FlinkOptions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for {@link AppendWriteFunction} to handle rate limit if it was set.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class AppendWriteFunctions {

  private static final Logger LOG = LoggerFactory.getLogger(AppendWriteFunctions.class);

  /**
   * Creates a {@link AppendWriteFunction} instance based on the given configuration.
   */
  public static <I> AppendWriteFunction<I> create(Configuration conf, RowType rowType) {
    if (conf.get(FlinkOptions.WRITE_RATE_LIMIT) > 0) {
      return new AppendWriteFunctionWithRateLimit<>(rowType, conf);
    }

    String bufferType = resolveBufferType(conf);
    if (BufferType.DISRUPTOR.name().equalsIgnoreCase(bufferType)) {
      return new AppendWriteFunctionWithDisruptorSort<>(conf, rowType);
    } else if (BufferType.BOUNDED_IN_MEMORY.name().equalsIgnoreCase(bufferType)) {
      return new AppendWriteFunctionWithBufferSort<>(conf, rowType);
    }
    return new AppendWriteFunction<>(conf, rowType);
  }

  /**
   * Resolves the buffer type from configuration, handling backward compatibility.
   */
  public static String resolveBufferType(Configuration conf) {
    // New config takes precedence
    String bufferType = conf.get(FlinkOptions.WRITE_BUFFER_TYPE);
    if (!BufferType.NONE.name().equalsIgnoreCase(bufferType)) {
      return bufferType;
    }

    // Backward compatibility: write.buffer.sort.enabled=true → DISRUPTOR
    if (conf.get(FlinkOptions.WRITE_BUFFER_SORT_ENABLED)) {
      LOG.info("write.buffer.sort.enabled is deprecated. Use write.buffer.type=DISRUPTOR instead.");
      return BufferType.DISRUPTOR.name();
    }

    return BufferType.NONE.name();
  }

  /**
   * Resolves sort keys from configuration, defaulting to record key field(s) if not specified.
   */
  public static String resolveSortKeys(Configuration conf) {
    String sortKeys = conf.get(FlinkOptions.WRITE_BUFFER_SORT_KEYS);
    if (sortKeys == null || sortKeys.isEmpty()) {
      // Default to record key field(s)
      return conf.get(FlinkOptions.RECORD_KEY_FIELD);
    }
    return sortKeys;
  }
}
