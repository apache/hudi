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

package org.apache.hudi.common.index.vector.search;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * A batch of rows to read from a single snapshot-resolved base file slice (RFC-109 §8). Produced
 * by the {@link VectorFetchPlanner} by grouping arbitrated candidates by file slice, so the read
 * handle can coalesce positions within one file/row-group/page. Compact and serializable — carries
 * only paths and row requests, never engine or SQL types.
 */
public final class VectorFetchTask implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String partitionPath;
  private final String fileId;
  private final String baseFilePath;
  private final String baseInstant;
  private final List<VectorRowRequest> requests;

  public VectorFetchTask(String partitionPath,
                         String fileId,
                         String baseFilePath,
                         String baseInstant,
                         List<VectorRowRequest> requests) {
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.baseFilePath = baseFilePath;
    this.baseInstant = baseInstant;
    this.requests = requests == null ? Collections.emptyList() : requests;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileId() {
    return fileId;
  }

  public String getBaseFilePath() {
    return baseFilePath;
  }

  public String getBaseInstant() {
    return baseInstant;
  }

  public List<VectorRowRequest> getRequests() {
    return requests;
  }

  public int size() {
    return requests.size();
  }
}
