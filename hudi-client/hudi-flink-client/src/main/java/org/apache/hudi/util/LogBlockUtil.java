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

package org.apache.hudi.util;

import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

/**
 * Utils for operations on log blocks.
 */
public class LogBlockUtil {

  public static HoodieLogBlockType pickLogDataBlockFormat(HoodieWriteConfig config, HoodieTable hoodieTable) {
    Option<HoodieLogBlockType> logBlockTypeOpt = config.getLogDataBlockFormat();
    if (logBlockTypeOpt.isPresent()) {
      return logBlockTypeOpt.get();
    }

    // Fallback to deduce data-block type based on the base file format
    switch (hoodieTable.getBaseFileFormat()) {
      case PARQUET:
      case ORC:
        return HoodieLogBlockType.AVRO_DATA_BLOCK;
      case HFILE:
        return HoodieLogBlockType.HFILE_DATA_BLOCK;
      default:
        throw new HoodieException("Base file format " + hoodieTable.getBaseFileFormat()
            + " does not have associated log block type");
    }
  }
}
