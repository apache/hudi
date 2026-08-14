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

/**
 * Enum representing the memory type used for the Flink write buffer.
 *
 * <p>This determines how memory is allocated for buffering records
 * before they are flushed to storage during write operations.
 *
 * @see org.apache.hudi.configuration.FlinkOptions#WRITE_BUFFER_MEMORY_TYPE
 */
public enum BufferMemoryType {
  /**
   * Uses JVM heap memory for the write buffer.
   * This is the default memory type.
   */
  ON_HEAP,

  /**
   * Uses Flink managed memory for the write buffer.
   * Managed memory is controlled by the Flink framework and is accounted
   * for in the task manager's memory budget, which helps avoid OOM errors
   * in containerized environments.
   */
  MANAGED
}
