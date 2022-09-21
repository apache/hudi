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

package org.apache.hudi.table.action.build;

import org.apache.hudi.avro.model.HoodieBuildPlan;
import org.apache.hudi.avro.model.HoodieBuildTask;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BuildStatus;
import org.apache.hudi.common.model.HoodieBuildCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.BuildUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieBuildException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.QualifiedTableName;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import java.util.ArrayList;
import java.util.List;

public class SparkExecuteBuildCommitActionExecutor<T extends HoodieRecordPayload, I, K, O>
    extends BaseActionExecutor<T, I, K, O, HoodieBuildCommitMetadata> {

  private HoodieBuildPlan buildPlan;

  public SparkExecuteBuildCommitActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               HoodieTable<T, I, K, O> table,
                                               String instantTime) {
    super(context, config, table, instantTime);
    this.buildPlan = BuildUtils.getBuildPlan(table.getMetaClient(),
            HoodieTimeline.getBuildRequestedInstant(instantTime))
        .map(Pair::getRight)
        .orElseThrow(() -> new HoodieBuildException("No plan found for this build:" + instantTime));
  }

  @Override
  public HoodieBuildCommitMetadata execute() {
    HoodieInstant requestInstant = HoodieTimeline.getBuildRequestedInstant(instantTime);
    table.getActiveTimeline().transitionBuildRequestedToInflight(requestInstant, Option.empty());
    table.getMetaClient().reloadActiveTimeline();

    Schema schema;
    try {
      schema = new TableSchemaResolver(table.getMetaClient()).getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieBuildException("Fail to get table schema for build action", e);
    }
    SerializableSchema serializableSchema = new SerializableSchema(schema);

    String indexFolderPath = table.getMetaClient().getIndexFolderPath();
    List<HoodieBuildTask> buildTasks = buildPlan.getTasks();
    SerializableConfiguration conf = new SerializableConfiguration(context.getHadoopConf().get());
    JavaSparkContext sparkContext = HoodieSparkEngineContext.getSparkContext(context);
    HoodieJavaRDD<BuildStatus> buildStatusRDD =
        HoodieJavaRDD.of(sparkContext.parallelize(buildTasks, buildTasks.size())
            .mapPartitions(buildTasksPartition -> {
              List<BuildStatus> buildStatuses = new ArrayList<>();
              buildTasksPartition.forEachRemaining(buildTask -> {
                BuildStatus buildStatus = new BuildTaskExecutor(buildTask, table.getConfig().getBasePath(),
                    indexFolderPath, serializableSchema, conf).execute();
                buildStatuses.add(buildStatus);
              });

              return buildStatuses.iterator();
            }));

    // Invalidate cached table for queries do not access related table through {@code DefaultSource}
    SessionCatalog sessionCatalog = SparkSession.active().sessionState().catalog();
    String databaseName = table.getMetaClient().getTableConfig().getDatabaseName();
    String tableName = table.getMetaClient().getTableConfig().getTableName();
    if (StringUtils.isNullOrEmpty(databaseName)) {
      databaseName = sessionCatalog.getCurrentDatabase();
    }
    QualifiedTableName qualifiedTableName = new QualifiedTableName(databaseName, tableName);
    sessionCatalog.invalidateCachedTable(qualifiedTableName);

    List<BuildStatus> buildStatuses = buildStatusRDD.collectAsList();
    return BuildUtils.convertToCommitMetadata(buildStatuses);
  }
}
