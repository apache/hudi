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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.FileIOUtils.LOG;

public class ReplaceCommitValidateUtil {
  public static final String REPLACE_COMMIT_FILE_IDS = "replaceCommitFileIds";
  public static void validateReplaceCommit(HoodieTableMetaClient metaClient) {
    metaClient.reloadActiveTimeline();
    Set<String> replaceFileids = new HashSet<>();

    // Verify pending and completed replace commit
    Stream.concat(metaClient.getActiveTimeline().getCompletedReplaceTimeline().getInstantsAsStream(),
        metaClient.getActiveTimeline().filterInflights().filterPendingReplaceTimeline().getInstantsAsStream()).map(instant -> {
          try {
            HoodieReplaceCommitMetadata replaceCommitMetadata =
                HoodieReplaceCommitMetadata.fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(),
                HoodieReplaceCommitMetadata.class);
            if (!instant.isCompleted()) {
              return JsonUtils.getObjectMapper().readValue(replaceCommitMetadata.getExtraMetadata().get(REPLACE_COMMIT_FILE_IDS), String[].class);
            } else {
              return replaceCommitMetadata.getPartitionToReplaceFileIds().values().stream()
                  .flatMap(List::stream)
                  .toArray(String[]::new);
            }
          } catch (Exception e) {
            // If the key does not exist or there is a JSON parsing error, LOG reports an error and ignores it.
            LOG.error("Error when reading REPLACECOMMIT meta", e);
            return null;
          }
        }).filter(Objects::nonNull)
        .flatMap(Arrays::stream)
        .filter(fileId -> !replaceFileids.add(fileId))
        .findFirst()
        .ifPresent(s -> {
          throw new HoodieException("REPLACECOMMIT involves duplicate file ID " + s + ", which will cause the table status to be abnormal. The REPLACECOMMIT has been terminated.");
        });
  }
}
