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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
 */
public class SparkInsertOverwritePartitioner extends UpsertPartitioner {

  private static final Logger LOG = LogManager.getLogger(SparkInsertOverwritePartitioner.class);

  public SparkInsertOverwritePartitioner(WorkloadProfile profile, HoodieEngineContext context, HoodieTable table,
                                         HoodieWriteConfig config) {
    super(profile, context, table, config);
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  @Override
  protected List<SmallFile> getSmallFiles(String partitionPath) {
    // for overwrite, we ignore all existing files. So do not consider any file to be smallFiles
    return Collections.emptyList();
  }
}
