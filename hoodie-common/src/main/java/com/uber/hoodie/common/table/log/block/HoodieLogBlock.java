/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import java.io.IOException;

/**
 * Abstract interface defining a block in HoodieLogFile
 */
public interface HoodieLogBlock {
  byte[] getBytes() throws IOException;
  HoodieLogBlockType getBlockType();

  /**
   * Type of the log block
   * WARNING: This enum is serialized as the ordinal. Only add new enums at the end.
   */
  enum HoodieLogBlockType {
    COMMAND_BLOCK,
    DELETE_BLOCK,
    CORRUPT_BLOCK,
    AVRO_DATA_BLOCK
  }
}
