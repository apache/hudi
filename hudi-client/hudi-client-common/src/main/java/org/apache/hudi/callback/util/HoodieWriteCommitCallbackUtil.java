/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.util;

import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage.PrevFilePaths;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieCommitCallbackException;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util helps to prepare callback message.
 */
@Slf4j
public class HoodieWriteCommitCallbackUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Convert data to json string format.
   */
  public static String convertToJsonString(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (IOException e) {
      throw new HoodieCommitCallbackException("Callback service convert data to json failed", e);
    }
  }

  /**
   * Resolve the previous base file (and bootstrap base file, if any) for every
   * {@link HoodieWriteStat} that represents an update, using a populated
   * {@link BaseFileOnlyView}. The lookup is O(1) per stat against the cached view, so
   * this adds no I/O on top of what the writer already paid.
   *
   * <p>Feeds {@link org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage#getPrevFilePaths()}
   * so the callback message can ship actual file paths rather than forcing each callback
   * impl to rebuild a {@code FileSystemView}.
   */
  public static Map<String, PrevFilePaths> resolvePrevFilePaths(List<HoodieWriteStat> stats,
                                                                BaseFileOnlyView fsView) {
    Map<String, PrevFilePaths> pathsByFileId = new HashMap<>();
    if (stats == null || fsView == null) {
      return pathsByFileId;
    }
    for (HoodieWriteStat stat : stats) {
      String prevCommit = stat.getPrevCommit();
      if (StringUtils.isNullOrEmpty(prevCommit) || HoodieWriteStat.NULL_COMMIT.equals(prevCommit)) {
        continue;
      }
      Option<HoodieBaseFile> prev;
      try {
        prev = fsView.getBaseFileOn(stat.getPartitionPath(), prevCommit, stat.getFileId());
      } catch (Exception e) {
        // Best-effort: a remote view 4xx/5xx, a stale view, or a replaced file group must not
        // fail the commit. Drop the prev path for this stat and keep going.
        log.warn("Could not resolve prev base file for fileId={} prevCommit={}; skipping",
            stat.getFileId(), prevCommit, e);
        continue;
      }
      if (!prev.isPresent()) {
        continue;
      }
      HoodieBaseFile prevBaseFile = prev.get();
      Option<BaseFile> bootstrapBaseFile = prevBaseFile.getBootstrapBaseFile();
      String prevPath = prevBaseFile.getPath();
      String bootstrapPath = bootstrapBaseFile.isPresent() ? bootstrapBaseFile.get().getPath() : null;
      pathsByFileId.put(stat.getFileId(), new PrevFilePaths(prevPath, bootstrapPath));
    }
    return pathsByFileId;
  }
}
