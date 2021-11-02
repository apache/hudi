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

package org.apache.hudi.index;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

/**
 * Base flink implementation of {@link HoodieIndex}.
 * @param <T> payload type
 */
public abstract class FlinkHoodieIndex<T extends HoodieRecordPayload> extends HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  protected FlinkHoodieIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public abstract List<WriteStatus> updateLocation(List<WriteStatus> writeStatuses,
                                                   HoodieEngineContext context,
                                                   HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException;

  @Override
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public abstract List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records,
                                                    HoodieEngineContext context,
                                                    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException;

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public HoodieData<HoodieRecord<T>> tagLocation(
      HoodieData<HoodieRecord<T>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException {
    return HoodieList.of(tagLocation(HoodieList.getList(records), context, hoodieTable));
  }

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException {
    return HoodieList.of(updateLocation(HoodieList.getList(writeStatuses), context, hoodieTable));
  }
}
