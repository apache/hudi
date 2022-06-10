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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

public class JavaInsertPreppedCommitActionExecutor<T>
    extends BaseJavaCommitActionExecutor<T> {

  private final List<HoodieRecord<T>> preppedRecords;

  public JavaInsertPreppedCommitActionExecutor(HoodieJavaEngineContext context,
                                               HoodieWriteConfig config, HoodieTable table,
                                               String instantTime, List<HoodieRecord<T>> preppedRecords) {
    super(context, config, table, instantTime, WriteOperationType.INSERT_PREPPED);
    this.preppedRecords = preppedRecords;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    return super.execute(preppedRecords);
  }
}
