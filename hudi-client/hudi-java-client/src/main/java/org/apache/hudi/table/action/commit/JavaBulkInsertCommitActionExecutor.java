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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;
import java.util.Map;

public class JavaBulkInsertCommitActionExecutor<T extends HoodieRecordPayload<T>> extends BaseJavaCommitActionExecutor<T> {

  private final List<HoodieRecord<T>> inputRecords;
  private final Option<BulkInsertPartitioner> bulkInsertPartitioner;

  public JavaBulkInsertCommitActionExecutor(HoodieJavaEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                            String instantTime, List<HoodieRecord<T>> inputRecords,
                                            Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    this(context, config, table, instantTime, inputRecords, bulkInsertPartitioner, Option.empty());
  }

  public JavaBulkInsertCommitActionExecutor(HoodieJavaEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                            String instantTime, List<HoodieRecord<T>> inputRecords,
                                            Option<BulkInsertPartitioner> bulkInsertPartitioner,
                                            Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, WriteOperationType.BULK_INSERT, extraMetadata);
    this.inputRecords = inputRecords;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    try {
      return JavaBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, table, config,
          this, true, bulkInsertPartitioner);
    } catch (HoodieInsertException ie) {
      throw ie;
    } catch (Throwable e) {
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }
}
