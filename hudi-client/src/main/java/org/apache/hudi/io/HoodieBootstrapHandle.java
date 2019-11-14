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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

public class HoodieBootstrapHandle<T extends HoodieRecordPayload> extends HoodieCreateHandle<T> {

  public HoodieBootstrapHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T> hoodieTable,
      String partitionPath, String fileId) {
    super(config, commitTime, hoodieTable, partitionPath, fileId,
        Pair.of(HoodieAvroUtils.RECORD_KEY_SCHEMA,
            HoodieAvroUtils.addMetadataFields(HoodieAvroUtils.RECORD_KEY_SCHEMA)));
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }
}
