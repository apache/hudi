/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.func;

import com.uber.hoodie.table.HoodieTable;
import java.util.stream.Stream;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ViewCacheUtils {

  private static Logger logger = LogManager.getLogger(ViewCacheUtils.class);

  /**
   * Cache Latest file-system view for the partitions passed
   * @param hoodieTable Table where view needs to be cached
   * @param partitions Stream of partitions
   */
  public static void cachePartitionsView(HoodieTable hoodieTable, Stream<String> partitions) {
    partitions.forEach(partitionPath -> {
      // Populate latest version file-slice cache. No-op if already populated
      long numFileSlices = hoodieTable.getLatestFileSliceOnlyFSView().getLatestFileSlices(partitionPath).count();
      logger.info("Pre-populated view for partition=(" + partitionPath + "). numFileSlices=" + numFileSlices);
    });
  }

  public static void cachePartitionsViewAndSeal(HoodieTable hoodieTable, Stream<String> partitions) {
    cachePartitionsView(hoodieTable, partitions);
    hoodieTable.getLatestFileSliceOnlyFSView().seal();
  }
}