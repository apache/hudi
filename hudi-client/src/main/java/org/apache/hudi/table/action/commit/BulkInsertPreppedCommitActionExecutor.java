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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BulkInsertPreppedCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends CommitActionExecutor<T> {

  private final JavaRDD<HoodieRecord<T>> preppedInputRecordRdd;
  private final Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner;

  public BulkInsertPreppedCommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, JavaRDD<HoodieRecord<T>> preppedInputRecordRdd,
      Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner) {
    super(jsc, config, table, instantTime, WriteOperationType.BULK_INSERT);
    this.preppedInputRecordRdd = preppedInputRecordRdd;
    this.userDefinedBulkInsertPartitioner = userDefinedBulkInsertPartitioner;
  }

  @Override
  public HoodieWriteMetadata execute() {
    try {
      return BulkInsertHelper.bulkInsert(preppedInputRecordRdd, instantTime, (HoodieTable<T>) table, config,
          this, false, userDefinedBulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }

}