/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.stream.Stream;

/**
 * Utilities for selecting the LSM file group reader.
 */
public final class LsmReaderUtils {

  private LsmReaderUtils() {
  }

  /**
   * Returns whether the file group can be read with the LSM reader for the configured merge type.
   */
  public static boolean shouldUseLsmReader(
      HoodieTableConfig tableConfig, Stream<HoodieLogFile> logFiles, String mergeType) {
    // The LSM reader collapses all sorted versions of a key. Skip-merge queries intentionally
    // expose those versions independently, so retain the classic unmerged reader for that mode.
    return !HoodieReaderConfig.REALTIME_SKIP_MERGE.equalsIgnoreCase(mergeType)
        && tableConfig.isLSMTreeStorageLayout()
        && logFiles.allMatch(logFile -> FSUtils.isNativeLogFile(logFile.getFileName()));
  }
}
