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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HoodieDataTableUtils {

  /**
   * @return All hoodie files of the table from the file system.
   * @throws IOException upon errors.
   */
  static List<StoragePath> getBaseAndLogFilePathsFromFileSystem(
      HoodieTableMetadata tableMetadata,
      String basePath) throws IOException {
    List<String> allPartitionPaths = tableMetadata.getAllPartitionPaths()
        .stream().map(partitionPath ->
            FSUtils.constructAbsolutePath(basePath, partitionPath).toString())
        .collect(Collectors.toList());
    return tableMetadata.getAllFilesInPartitions(allPartitionPaths).values().stream()
        .map(fileStatuses ->
            fileStatuses.stream().map(fileStatus -> fileStatus.getPath())
                .collect(Collectors.toList()))
        .flatMap(list -> list.stream())
        .collect(Collectors.toList());
  }

}
