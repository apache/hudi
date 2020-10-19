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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.integ.testsuite.dag.nodes.CleanNode;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.RollbackNode;
import org.apache.hudi.integ.testsuite.dag.nodes.ScheduleCompactNode;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Operation;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A writer abstraction for the Hudi test suite. This class wraps different implementations of writers used to perform
 * write operations into the target hudi dataset. Current supported writers are {@link HoodieDeltaStreamerWrapper}
 * and {@link HoodieWriteClient}.
 */
public class HoodieTestSuiteWriter {

  private HoodieDeltaStreamerWrapper deltaStreamerWrapper;
  private HoodieWriteClient writeClient;
  protected HoodieTestSuiteConfig cfg;
  private Option<String> lastCheckpoint;
  private HoodieReadClient hoodieReadClient;
  private Properties props;
  private String schema;
  private transient Configuration configuration;
  private transient JavaSparkContext sparkContext;
  private static Set<String> VALID_DAG_NODES_TO_ALLOW_WRITE_CLIENT_IN_DELTASTREAMER_MODE = new HashSet<>(
      Arrays.asList(RollbackNode.class.getName(), CleanNode.class.getName(), ScheduleCompactNode.class.getName()));

  public HoodieTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema) throws
      Exception {
    this(jsc, props, cfg, schema, true);
  }

  public HoodieTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema,
      boolean rollbackInflight) throws Exception {
    // We ensure that only 1 instance of HoodieWriteClient is instantiated for a HoodieTestSuiteWriter
    // This does not instantiate a HoodieWriteClient until a
    // {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Option)} is invoked.
    this.deltaStreamerWrapper = new HoodieDeltaStreamerWrapper(cfg, jsc);
    this.hoodieReadClient = new HoodieReadClient(jsc, cfg.targetBasePath);
    if (!cfg.useDeltaStreamer) {
      this.writeClient = new HoodieWriteClient(jsc, getHoodieClientConfig(cfg, props, schema), rollbackInflight);
    }
    this.cfg = cfg;
    this.configuration = jsc.hadoopConfiguration();
    this.sparkContext = jsc;
    this.props = props;
    this.schema = schema;
  }

  private HoodieWriteConfig getHoodieClientConfig(HoodieTestSuiteConfig cfg, Properties props, String schema) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.targetBasePath)
            .withAutoCommit(false)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName).build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
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

  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return this.deltaStreamerWrapper.fetchSource();
  }

  public Option<String> startCommit() {
    if (cfg.useDeltaStreamer) {
      return Option.of(HoodieActiveTimeline.createNewInstantTime());
    } else {
      return Option.of(writeClient.startCommit());
    }
  }

  public JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.upsert(Operation.UPSERT);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.upsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insert();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.bulkInsert();
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.bulkInsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> compact(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.compact();
    } else {
      if (!instantTime.isPresent()) {
        Option<Pair<String, HoodieCompactionPlan>> compactionPlanPair = Option
            .fromJavaOptional(hoodieReadClient.getPendingCompactions()
                .stream().findFirst());
        if (compactionPlanPair.isPresent()) {
          instantTime = Option.of(compactionPlanPair.get().getLeft());
        }
      }
      if (instantTime.isPresent()) {
        return writeClient.compact(instantTime.get());
      } else {
        return null;
      }
    }
  }

  public Option<String> scheduleCompaction(Option<Map<String, String>> previousCommitExtraMetadata) throws
      Exception {
    if (!cfg.useDeltaStreamer) {
      deltaStreamerWrapper.scheduleCompact();
      return Option.empty();
    } else {
      return writeClient.scheduleCompaction(previousCommitExtraMetadata);
    }
  }

  public void commit(JavaRDD<WriteStatus> records, Option<String> instantTime) {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Option)} **/
      extraMetadata.put(HoodieDeltaStreamerWrapper.CHECKPOINT_KEY, lastCheckpoint.get());
      writeClient.commit(instantTime.get(), records, Option.of(extraMetadata));
    }
  }

  public HoodieWriteClient getWriteClient(DagNode dagNode) throws IllegalAccessException {
    if (cfg.useDeltaStreamer & !allowWriteClientAccess(dagNode)) {
      throw new IllegalAccessException("cannot access write client when testing in deltastreamer mode");
    }
    synchronized (this) {
      if (writeClient == null) {
        this.writeClient = new HoodieWriteClient(this.sparkContext, getHoodieClientConfig(cfg, props, schema), false);
      }
    }
    return writeClient;
  }

  public HoodieDeltaStreamerWrapper getDeltaStreamerWrapper() {
    return deltaStreamerWrapper;
  }

  public HoodieTestSuiteConfig getCfg() {
    return cfg;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }
}
