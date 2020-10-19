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

import java.util.Map;
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

public class BulkInsertCommitActionExecutor<T extends HoodieRecordPayload<T>> extends CommitActionExecutor<T> {

  private final JavaRDD<HoodieRecord<T>> inputRecordsRDD;
  private final Option<BulkInsertPartitioner<T>> bulkInsertPartitioner;

  public BulkInsertCommitActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config, HoodieTable table,
                                        String instantTime, JavaRDD<HoodieRecord<T>> inputRecordsRDD,
                                        Option<BulkInsertPartitioner<T>> bulkInsertPartitioner) {
    this(jsc, config, table, instantTime, inputRecordsRDD, bulkInsertPartitioner, Option.empty());
  }

  public BulkInsertCommitActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config, HoodieTable table,
                                        String instantTime, JavaRDD<HoodieRecord<T>> inputRecordsRDD,
                                        Option<BulkInsertPartitioner<T>> bulkInsertPartitioner,
                                        Option<Map<String, String>> extraMetadata) {
    super(jsc, config, table, instantTime, WriteOperationType.BULK_INSERT, extraMetadata);
    this.inputRecordsRDD = inputRecordsRDD;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  @Override
  public HoodieWriteMetadata execute() {
    try {
      return BulkInsertHelper.bulkInsert(inputRecordsRDD, instantTime, (HoodieTable<T>) table, config,
          this, true, bulkInsertPartitioner);
    } catch (HoodieInsertException ie) {
      throw ie;
    } catch (Throwable e) {
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }
}
