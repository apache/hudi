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

package org.apache.hudi.integ.testsuite;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.integ.testsuite.dag.nodes.CleanNode;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.RollbackNode;
import org.apache.hudi.integ.testsuite.dag.nodes.ScheduleCompactNode;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.utilities.schema.SchemaProvider;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Note: Getters are manually defined instead of using Lombok's @Getter annotation.
 * This is because Scala classes in this module reference these getters, and the Scala
 * compiler runs before Lombok annotation processing. Using @Getter would cause Scala
 * compilation errors as the generated methods wouldn't be visible yet. To avoid
 * complicating the build with custom compilation order configuration, we use explicit
 * getter methods that are available during Scala compilation.
 */
@Slf4j
public abstract class HoodieTestSuiteWriter implements Serializable {

  protected HoodieDeltaStreamerWrapper deltaStreamerWrapper;
  protected HoodieWriteConfig writeConfig;
  protected SparkRDDWriteClient writeClient;
  protected HoodieTestSuiteJob.HoodieTestSuiteConfig cfg;
  protected Option<Checkpoint> lastCheckpoint;
  protected HoodieReadClient hoodieReadClient;
  protected Properties props;
  protected String schema;
  protected transient Configuration configuration;
  protected transient JavaSparkContext sparkContext;
  protected static Set<String> VALID_DAG_NODES_TO_ALLOW_WRITE_CLIENT_IN_DELTASTREAMER_MODE = new HashSet<>(
      Arrays.asList(RollbackNode.class.getName(), CleanNode.class.getName(), ScheduleCompactNode.class.getName()));

  public HoodieTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteJob.HoodieTestSuiteConfig cfg, String schema) throws Exception {
    // We ensure that only 1 instance of HoodieWriteClient is instantiated for a HoodieTestSuiteWriter
    // This does not instantiate a HoodieWriteClient until a
    // {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Option)} is invoked.
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(jsc);
    this.deltaStreamerWrapper = new HoodieDeltaStreamerWrapper(cfg, jsc);
    this.hoodieReadClient = new HoodieReadClient(context, cfg.targetBasePath);
    this.writeConfig = getHoodieClientConfig(cfg, props, schema);
    this.writeClient = new SparkRDDWriteClient(context, writeConfig);
    this.cfg = cfg;
    this.configuration = jsc.hadoopConfiguration();
    this.sparkContext = jsc;
    this.props = props;
    this.schema = schema;
  }

  public HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }

  private HoodieWriteConfig getHoodieClientConfig(HoodieTestSuiteJob.HoodieTestSuiteConfig cfg, Properties props, String schema) {

    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.targetBasePath)
            .withPayloadConfig(HoodiePayloadConfig.newBuilder()
                .withPayloadOrderingFields(cfg.sourceOrderingFields)
                .withPayloadClass(cfg.payloadClassName)
                .build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.valueOf(cfg.indexType)).build())
            .withProps(props);
    builder = builder.withSchema(schema);
    return builder.build();
  }

  private boolean allowWriteClientAccess(DagNode dagNode) {
    if (VALID_DAG_NODES_TO_ALLOW_WRITE_CLIENT_IN_DELTASTREAMER_MODE.contains(dagNode.getClass().getName())) {
      return true;
    }
    return false;
  }

  public abstract void shutdownResources();

  public abstract RDD<GenericRecord> getNextBatch() throws Exception;

  public abstract Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> fetchSource() throws Exception;

  public abstract Option<String> startCommit();

  public abstract JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception;

  public abstract JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception;

  public abstract JavaRDD<WriteStatus> insertOverwrite(Option<String> instantTime) throws Exception;

  public abstract JavaRDD<WriteStatus> insertOverwriteTable(Option<String> instantTime) throws Exception;

  public abstract JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception;

  public abstract HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(Option<String> instantTime) throws Exception;

  public abstract void inlineClustering() throws Exception;

  public abstract Option<String> scheduleCompaction(Option<Map<String, String>> previousCommitExtraMetadata) throws Exception;

  public abstract void commit(JavaRDD<WriteStatus> records, JavaRDD<DeltaWriteStats> generatedDataStats,
                              Option<String> instantTime);

  public abstract void commitCompaction(Option<String> instantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) throws Exception;

  public SparkRDDWriteClient getWriteClient(DagNode dagNode) throws IllegalAccessException {
    if (cfg.useDeltaStreamer & !allowWriteClientAccess(dagNode)) {
      throw new IllegalAccessException("cannot access write client when testing in deltastreamer mode");
    }
    synchronized (this) {
      if (writeClient == null) {
        this.writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(this.sparkContext), getHoodieClientConfig(cfg, props, schema));
      }
    }
    return writeClient;
  }

  public HoodieDeltaStreamerWrapper getDeltaStreamerWrapper() {
    return deltaStreamerWrapper;
  }

  public HoodieTestSuiteJob.HoodieTestSuiteConfig getCfg() {
    return cfg;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  public Option<Checkpoint> getLastCheckpoint() {
    return lastCheckpoint;
  }

  public Properties getProps() {
    return props;
  }

  public String getSchema() {
    return schema;
  }
}

