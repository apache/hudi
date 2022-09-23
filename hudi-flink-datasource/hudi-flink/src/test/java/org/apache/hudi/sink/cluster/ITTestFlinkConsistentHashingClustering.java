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

package org.apache.hudi.sink.cluster;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.clustering.update.strategy.FlinkConsistentBucketDuplicateUpdateStrategy;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ITTestFlinkConsistentHashingClustering {

  @TempDir
  File tempFile;

  @Test
  public void testScheduleMergePlan() throws ExecutionException, InterruptedException {
    // Create hoodie table and insert into data.
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

    Map<String, String> options = getDefaultConsistentHashingOption();

    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    tableEnv.executeSql(TestSQL.INSERT_T1).await();

    TimeUnit.SECONDS.sleep(3);
  }


  private Map<String, String> getDefaultConsistentHashingOption() {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    options.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.name());
    options.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
    options.put(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE.key(), HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
    options.put(FlinkOptions.CLUSTERING_UPDATE_STRATEGY.key(), FlinkConsistentBucketDuplicateUpdateStrategy.class.getName());
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");

    return options;
  }
}
