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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All the metadata that gets stored along with a commit.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieReplaceCommitMetadata extends HoodieCommitMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieReplaceCommitMetadata.class);
  protected Map<String, List<String>> partitionToReplaceFileIds;

  // for ser/deser
  public HoodieReplaceCommitMetadata() {
    this(false);
  }

  public HoodieReplaceCommitMetadata(boolean compacted) {
    super(compacted);
    partitionToReplaceFileIds = new HashMap<>();
  }

  public void setPartitionToReplaceFileIds(Map<String, List<String>> partitionToReplaceFileIds) {
    this.partitionToReplaceFileIds = partitionToReplaceFileIds;
  }

  public void addReplaceFileId(String partitionPath, String fileId) {
    if (!partitionToReplaceFileIds.containsKey(partitionPath)) {
      partitionToReplaceFileIds.put(partitionPath, new ArrayList<>());
    }
    partitionToReplaceFileIds.get(partitionPath).add(fileId);
  }

  public List<String> getReplaceFileIds(String partitionPath) {
    return partitionToReplaceFileIds.get(partitionPath);
  }

  public Map<String, List<String>> getPartitionToReplaceFileIds() {
    return partitionToReplaceFileIds;
  }

  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToWriteStats.get(null));
      partitionToWriteStats.remove(null);
    }
    if (partitionToReplaceFileIds.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToReplaceFileIds.get(null));
      partitionToReplaceFileIds.remove(null);
    }
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieReplaceCommitMetadata that = (HoodieReplaceCommitMetadata) o;

    if (!partitionToWriteStats.equals(that.partitionToWriteStats)) {
      return false;
    }
    return compacted.equals(that.compacted);

  }

  @Override
  public int hashCode() {
    int result = partitionToWriteStats.hashCode();
    result = 31 * result + compacted.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "HoodieReplaceMetadata{" + "partitionToWriteStats=" + partitionToWriteStats
        + ", partitionToReplaceFileIds=" + partitionToReplaceFileIds
        + ", compacted=" + compacted
        + ", extraMetadata=" + extraMetadata
        + ", operationType=" + operationType + '}';
  }
}
