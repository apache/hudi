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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DeleteCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends CommitActionExecutor<T> {

  private final JavaRDD<HoodieKey> keys;

  public DeleteCommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, JavaRDD<HoodieKey> keys) {
    super(jsc, config, table, instantTime, WriteOperationType.DELETE);
    this.keys = keys;
  }

  public HoodieWriteMetadata execute() {
    return DeleteHelper.execute(instantTime, keys, jsc, config, (HoodieTable<T>)table, this);
  }
}
