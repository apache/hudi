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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A writer abstraction for the Hudi test suite. This class wraps different implementations of writers used to perform write operations into the target hudi dataset. Current supported writers are
 * {@link HoodieDeltaStreamerWrapper} and {@link SparkRDDWriteClient}.
 */
public class HoodieInlineTestSuiteWriter extends HoodieTestSuiteWriter {

  private static Logger log = LoggerFactory.getLogger(HoodieInlineTestSuiteWriter.class);

  private static final String GENERATED_DATA_PATH = "generated.data.path";

  public HoodieInlineTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteConfig cfg, String schema) throws Exception {
    super(jsc, props, cfg, schema);
  }

  @Override
  public void shutdownResources() {
    if (this.deltaStreamerWrapper != null) {
      log.info("Shutting down DS wrapper gracefully ");
      this.deltaStreamerWrapper.shutdownGracefully();
    }
    if (this.writeClient != null) {
      log.info("Closing local write client");
      this.writeClient.close();
    }
  }

  @Override
  public RDD<GenericRecord> getNextBatch() throws Exception {
    Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
    lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
    JavaRDD<HoodieRecord> inputRDD = nextBatch.getRight().getRight();
    return inputRDD.map(r -> (GenericRecord) ((HoodieAvroRecord) r).getData()
        .getInsertValue(new Schema.Parser().parse(schema)).get()).rdd();
  }

  @Override
  public Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return this.deltaStreamerWrapper.fetchSource();
  }

  @Override
  public Option<String> startCommit() {
    if (cfg.useDeltaStreamer) {
      return Option.of(writeClient.createNewInstantTime(true));
    } else {
      return Option.of(writeClient.startCommit());
    }
  }

  @Override
  public JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.upsert(WriteOperationType.UPSERT);
    } else {
      Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.upsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  @Override
  public JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insert();
    } else {
      Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  @Override
  public JavaRDD<WriteStatus> insertOverwrite(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insertOverwrite();
    } else {
      Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insertOverwrite(nextBatch.getRight().getRight(), instantTime.get()).getWriteStatuses();
    }
  }

  @Override
  public JavaRDD<WriteStatus> insertOverwriteTable(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insertOverwriteTable();
    } else {
      Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.insertOverwriteTable(nextBatch.getRight().getRight(), instantTime.get()).getWriteStatuses();
    }
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.bulkInsert();
    } else {
      Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Option.of(nextBatch.getValue().getLeft());
      return writeClient.bulkInsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(Option<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      JavaRDD<WriteStatus> toReturn = deltaStreamerWrapper.compact();
      HoodieWriteMetadata writeMetadata = new HoodieWriteMetadata();
      writeMetadata.setWriteStatuses(toReturn);
      return writeMetadata;
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
        HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = writeClient.compact(instantTime.get());
        return compactionMetadata;
      } else {
        return null;
      }
    }
  }

  @Override
  public void inlineClustering() {
    if (!cfg.useDeltaStreamer) {
      Option<String> clusteringInstantOpt = writeClient.scheduleClustering(Option.empty());
      clusteringInstantOpt.ifPresent(clusteringInstant -> {
        // inline cluster should auto commit as the user is never given control
        log.warn("Clustering instant :: " + clusteringInstant);
        writeClient.cluster(clusteringInstant, true);
      });
    } else {
      // TODO: fix clustering to be done async https://issues.apache.org/jira/browse/HUDI-1590
      throw new IllegalArgumentException("Clustering cannot be triggered with deltastreamer");
    }
  }

  @Override
  public Option<String> scheduleCompaction(Option<Map<String, String>> previousCommitExtraMetadata) throws
      Exception {
    if (cfg.useDeltaStreamer) {
      deltaStreamerWrapper.scheduleCompact();
      return Option.empty();
    } else {
      return writeClient.scheduleCompaction(previousCommitExtraMetadata);
    }
  }

  @Override
  public void commit(JavaRDD<WriteStatus> records, JavaRDD<DeltaWriteStats> generatedDataStats,
                     Option<String> instantTime) {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(SparkRDDWriteClient, JavaRDD, Option)} **/
      extraMetadata.put(HoodieDeltaStreamer.CHECKPOINT_KEY, lastCheckpoint.get().getCheckpointKey());
      if (generatedDataStats != null && generatedDataStats.count() > 1) {
        // Just stores the path where this batch of data is generated to
        extraMetadata.put(GENERATED_DATA_PATH, generatedDataStats.map(s -> s.getFilePath()).collect().get(0));
      }
      writeClient.commit(instantTime.get(), records, Option.of(extraMetadata));
    }
  }

  @Override
  public void commitCompaction(Option<String> instantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) throws IOException {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(SparkRDDWriteClient, JavaRDD, Option)} **/
      extraMetadata.put(HoodieDeltaStreamer.CHECKPOINT_KEY, lastCheckpoint.get().getCheckpointKey());
      // to fix HUDI-9392
      /*if (generatedDataStats != null && generatedDataStats.count() > 1) {
        // Just stores the path where this batch of data is generated to
        extraMetadata.put(GENERATED_DATA_PATH, generatedDataStats.map(s -> s.getFilePath()).collect().get(0));
      }*/
      //HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(writeClient.getConfig(), writeClient.getEngineContext());
      //HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(table, instantTime.get(), HoodieJavaRDD.of(records), writeClient.getConfig().getSchema());
      writeClient.commitCompaction(instantTime.get(), writeMetadata, Option.empty());
    }
  }
}
