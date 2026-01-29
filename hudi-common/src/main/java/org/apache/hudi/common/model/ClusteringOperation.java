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

import org.apache.hudi.avro.model.HoodieSliceInfo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates all the needed information about a clustering file slice. This is needed because spark serialization
 * does not work with avro objects.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ClusteringOperation implements Serializable {

  private String dataFilePath;
  private List<String> deltaFilePaths;
  private String fileId;
  private String partitionPath;
  private String bootstrapFilePath;
  private int version;

  public static ClusteringOperation create(HoodieSliceInfo sliceInfo) {
    return new ClusteringOperation(sliceInfo.getDataFilePath(), new ArrayList<>(sliceInfo.getDeltaFilePaths()), sliceInfo.getFileId(),
        sliceInfo.getPartitionPath(), sliceInfo.getBootstrapFilePath(), sliceInfo.getVersion());
  }
}
