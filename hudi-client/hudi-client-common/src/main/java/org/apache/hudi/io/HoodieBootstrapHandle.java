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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

/**
 * This class is essentially same as Create Handle but overrides two things
 * 1) Schema : Metadata bootstrap writes only metadata fields as part of write. So, setup the writer schema accordingly.
 * 2) canWrite is overridden to always return true so that skeleton file and bootstrap file is aligned and we don't end up
 *    writing more than 1 skeleton file for the same bootstrap file.
 * @param <T> HoodieRecordPayload
 */
public class HoodieBootstrapHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  public HoodieBootstrapHandle(HoodieWriteConfig config, String commitTime, HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    super(config, commitTime, hoodieTable, partitionPath, fileId,
        Option.of(HoodieAvroUtils.RECORD_KEY_SCHEMA), taskContextSupplier);
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }
}