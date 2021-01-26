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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

public abstract class JavaHoodieIndex<T extends HoodieRecordPayload> extends HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  protected JavaHoodieIndex(HoodieWriteConfig config) {
    super(config);
  }

  public static JavaHoodieIndex createIndex(HoodieWriteConfig config) {
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      Object instance = ReflectionUtils.loadClass(config.getIndexClass(), config);
      if (!(instance instanceof HoodieIndex)) {
        throw new HoodieIndexException(config.getIndexClass() + " is not a subclass of HoodieIndex");
      }
      return (JavaHoodieIndex) instance;
    }

    // TODO more indexes to be added
    switch (config.getIndexType()) {
      case INMEMORY:
        return new JavaInMemoryHashIndex(config);
      default:
        throw new HoodieIndexException("Unsupported index type " + config.getIndexType());
    }
  }

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract List<WriteStatus> updateLocation(List<WriteStatus> writeStatuses,
                                                   HoodieEngineContext context,
                                                   HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException;

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records,
                                                    HoodieEngineContext context,
                                                    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException;
}
