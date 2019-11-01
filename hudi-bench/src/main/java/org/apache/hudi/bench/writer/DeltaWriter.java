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

package org.apache.hudi.bench.writer;

import org.apache.hudi.HoodieReadClient;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.bench.job.HoodieDeltaStreamerWrapper;
import org.apache.hudi.bench.job.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A writer abstraction for the Hudi test suite. This class wraps different implementations of writers used to perform
 * write operations into the target hudi dataset. Current supported writers are {@link HoodieDeltaStreamerWrapper}
 * and {@link HoodieWriteClient}.
 */
public class DeltaWriter {

  private HoodieDeltaStreamerWrapper deltaStreamerWrapper;
  private HoodieWriteClient writeClient;
  private HoodieTestSuiteConfig cfg;
  private Option<String> lastCheckpoint;
  private HoodieReadClient hoodieReadClient;
  private transient Configuration configuration;
  private transient JavaSparkContext sparkContext;

  public DeltaWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema) throws
      Exception {
    this(jsc, props, cfg, schema, true);
  }

  public DeltaWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema,
      boolean rollbackInflight) throws Exception {
    // We ensure that only 1 instance of HoodieWriteClient is instantiated for a DeltaWriter
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

  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return this.deltaStreamerWrapper.fetchSource();
  }

  public Option<String> startCommit() {
    if (cfg.useDeltaStreamer) {
      return Option.of(HoodieActiveTimeline.createNewCommitTime());
    } else {
      return Option.of(writeClient.startCommit());
    }
  }

  public JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.upsert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.upsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.bulkInsert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.bulkInsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> compact(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.compact(instantTime);
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

  public void commit(JavaRDD<WriteStatus> records, Option<String> instantTime) {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Option)} **/
      extraMetadata.put(HoodieDeltaStreamerWrapper.CHECKPOINT_KEY, lastCheckpoint.get());
      writeClient.commit(instantTime.get(), records, Option.of(extraMetadata));
    }
  }

  public HoodieWriteClient getWriteClient() throws IllegalAccessException {
    if (cfg.useDeltaStreamer) {
      throw new IllegalAccessException("cannot access write client when testing in deltastreamer mode");
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
