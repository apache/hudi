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

package org.apache.hudi.index;

import java.util.List;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

public class FlinkHoodieNonIndex<T extends HoodieRecordPayload> extends FlinkHoodieIndex<T> {

  public FlinkHoodieNonIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    throw new UnsupportedOperationException("Unsupport operation.");
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatuses, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> hoodieRecords, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    return null;
  }
}
