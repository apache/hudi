/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.util;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonClientUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CommonClientUtils.class);

  public static void validateTableVersion(HoodieTableConfig tableConfig, HoodieWriteConfig writeConfig) {
    // mismatch of table versions.
    if (!tableConfig.getTableVersion().equals(writeConfig.getWriteVersion())) {
      // if table version is greater than 6, while writer version is 6, we can still allow it for upgrade
      if (tableConfig.getTableVersion().greaterThan(HoodieTableVersion.SIX) && writeConfig.getWriteVersion().equals(HoodieTableVersion.SIX)) {
        LOG.warn("Table version is greater than 6, while writer version is 6. Allowing it for upgrade.");
      } else {
        throw new HoodieNotSupportedException(String.format("Table version (%s) and Writer version (%s) do not match for table at: %s.",
            tableConfig.getTableVersion(), writeConfig.getWriteVersion(), writeConfig.getBasePath()));
      }
    }
    // incompatible configurations.
    if (tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT) && writeConfig.shouldWritePartialUpdates()) {
      throw new HoodieNotSupportedException("Partial updates are not supported for table versions < 8. "
          + "Please unset " + HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key());
    }

    if (tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT) && writeConfig.isNonBlockingConcurrencyControl()) {
      throw new HoodieNotSupportedException("Non-blocking concurrency control is not supported for table versions < 8.");
    }
  }

  /**
   * Returns the base file format.
   */
  public static HoodieFileFormat getBaseFileFormat(HoodieWriteConfig writeConfig, HoodieTableConfig tableConfig) {
    if (tableConfig.isMultipleBaseFileFormatsEnabled() && writeConfig.contains(HoodieWriteConfig.BASE_FILE_FORMAT)) {
      return writeConfig.getBaseFileFormat();
    }
    return tableConfig.getBaseFileFormat();
  }

  /**
   * Returns the log block type..
   */
  public static HoodieLogBlock.HoodieLogBlockType getLogBlockType(HoodieWriteConfig writeConfig, HoodieTableConfig tableConfig) {
    Option<HoodieLogBlock.HoodieLogBlockType> logBlockTypeOpt = writeConfig.getLogDataBlockFormat();
    if (logBlockTypeOpt.isPresent()) {
      return logBlockTypeOpt.get();
    }
    HoodieFileFormat baseFileFormat = getBaseFileFormat(writeConfig, tableConfig);
    switch (getBaseFileFormat(writeConfig, tableConfig)) {
      case PARQUET:
      case ORC:
        return HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;
      case HFILE:
        return HoodieLogBlock.HoodieLogBlockType.HFILE_DATA_BLOCK;
      default:
        throw new HoodieException("Base file format " + baseFileFormat
            + " does not have associated log block type");
    }
  }

  public static String generateWriteToken(TaskContextSupplier taskContextSupplier) {
    try {
      return FSUtils.makeWriteToken(
          taskContextSupplier.getPartitionIdSupplier().get(),
          taskContextSupplier.getStageIdSupplier().get(),
          taskContextSupplier.getAttemptIdSupplier().get()
      );
    } catch (Throwable t) {
      LOG.warn("Error generating write token, using default.", t);
      return HoodieLogFormat.DEFAULT_WRITE_TOKEN;
    }
  }
}
