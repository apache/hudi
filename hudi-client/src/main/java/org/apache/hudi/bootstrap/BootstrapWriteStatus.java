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

package org.apache.hudi.bootstrap;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;

/**
 * WriteStatus for Bootstrap.
 */
public class BootstrapWriteStatus extends WriteStatus {

  public static final class BootstrapSourceInfo implements Serializable {
    private final String bootstrapBasePath;
    private final String bootstrapPartitionPath;
    private final String fileName;

    public BootstrapSourceInfo(String bootstrapBasePath, String bootstrapPartitionPath, String fileName) {
      this.bootstrapBasePath = bootstrapBasePath;
      this.bootstrapPartitionPath = bootstrapPartitionPath;
      this.fileName = fileName;
    }

    public String getBootstrapBasePath() {
      return bootstrapBasePath;
    }

    public String getBootstrapPartitionPath() {
      return bootstrapPartitionPath;
    }

    public String getFileName() {
      return fileName;
    }
  }

  private BootstrapSourceInfo bootstrapSourceInfo;

  public BootstrapWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    super(trackSuccessRecords, failureFraction);
  }

  public BootstrapSourceInfo getBootstrapSourceInfo() {
    return bootstrapSourceInfo;
  }

  public Pair<BootstrapSourceInfo, HoodieWriteStat> getBootstrapSourceAndWriteStat() {
    return Pair.of(getBootstrapSourceInfo(), getStat());
  }

  public void setBootstrapSourceInfo(BootstrapSourceInfo bootstrapSourceInfo) {
    this.bootstrapSourceInfo = bootstrapSourceInfo;
  }
}
