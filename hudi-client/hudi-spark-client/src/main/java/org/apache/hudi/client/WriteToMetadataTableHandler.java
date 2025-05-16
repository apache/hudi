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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

public interface WriteToMetadataTableHandler {

  void writeToMetadataTable(HoodieTable table, String instantTime, List<HoodieWriteStat> metadataWriteStatsSoFar, HoodieCommitMetadata metadata);

  default HoodieWriteMetadata<HoodieData<WriteStatus>> streamWriteToMetadataTable(HoodieTable table, HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata, String instantTime) {
    return writeMetadata;
  }
}
