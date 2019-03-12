package com.uber.hoodie.bench.writer;

import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.bench.job.HoodieDeltaStreamerWrapper;
import com.uber.hoodie.bench.job.HudiTestSuiteJob.HudiTestSuiteConfig;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A writer abstraction for the Hudi test suite. This class wraps different implementations of writers used to perform
 * write operations into the target hudi dataset. Current supported writers are {@link HoodieDeltaStreamerWrapper}
 * and {@link HoodieWriteClient}
 */
public class DeltaWriter {

  private HoodieDeltaStreamerWrapper deltaStreamerWrapper;
  private HoodieWriteClient writeClient;
  private HudiTestSuiteConfig cfg;
  private Optional<String> lastCheckpoint;
  private HoodieReadClient hoodieReadClient;
  private transient Configuration configuration;
  private transient JavaSparkContext sparkContext;

  public DeltaWriter(JavaSparkContext jsc, Properties props, HudiTestSuiteConfig cfg, String schema) throws
      Exception {
    this(jsc, props, cfg, schema, true);
  }

  public DeltaWriter(JavaSparkContext jsc, Properties props, HudiTestSuiteConfig cfg, String schema,
      boolean rollbackInflight) throws Exception {
    // We ensure that only 1 instance of HoodieWriteClient is instantiated for a DeltaWriter
    /** This does not instantiate a HoodieWriteClient until a
     * {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Optional)} is invoked. **/
    this.deltaStreamerWrapper = new HoodieDeltaStreamerWrapper(cfg, jsc);
    this.hoodieReadClient = new HoodieReadClient(jsc, cfg.targetBasePath);
    if (!cfg.useDeltaStreamer) {
      this.writeClient = new HoodieWriteClient(jsc, getHoodieClientConfig(cfg, props, schema), rollbackInflight);
    }
    this.cfg = cfg;
    this.configuration = jsc.hadoopConfiguration();
    this.sparkContext = jsc;
  }

  private HoodieWriteConfig getHoodieClientConfig(HudiTestSuiteConfig cfg, Properties props, String schema) {
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

  public Optional<String> startCommit() {
    if (cfg.useDeltaStreamer) {
      return Optional.of(HoodieActiveTimeline.createNewCommitTime());
    } else {
      return Optional.of(writeClient.startCommit());
    }
  }

  public JavaRDD<WriteStatus> upsert(Optional<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.upsert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Optional.of(nextBatch.getValue().getLeft());
      return writeClient.upsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> insert(Optional<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.insert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Optional.of(nextBatch.getValue().getLeft());
      return writeClient.insert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> bulkInsert(Optional<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.bulkInsert(instantTime);
    } else {
      Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> nextBatch = fetchSource();
      lastCheckpoint = Optional.of(nextBatch.getValue().getLeft());
      return writeClient.bulkInsert(nextBatch.getRight().getRight(), instantTime.get());
    }
  }

  public JavaRDD<WriteStatus> compact(Optional<String> instantTime) throws Exception {
    if (cfg.useDeltaStreamer) {
      return deltaStreamerWrapper.compact(instantTime);
    } else {
      if (!instantTime.isPresent()) {
        Optional<Pair<String, HoodieCompactionPlan>> compactionPlanPair = hoodieReadClient.getPendingCompactions()
            .stream().findFirst();
        if (compactionPlanPair.isPresent()) {
          instantTime = Optional.of(compactionPlanPair.get().getLeft());
        }
      }
      if (instantTime.isPresent()) {
        return writeClient.compact(instantTime.get());
      } else {
        return null;
      }
    }
  }

  public void commit(JavaRDD<WriteStatus> records, Optional<String> instantTime) {
    if (!cfg.useDeltaStreamer) {
      Map<String, String> extraMetadata = new HashMap<>();
      /** Store the checkpoint in the commit metadata just like
       * {@link HoodieDeltaStreamer#commit(HoodieWriteClient, JavaRDD, Optional)} **/
      extraMetadata.put(HoodieDeltaStreamerWrapper.CHECKPOINT_KEY, lastCheckpoint.get());
      writeClient.commit(instantTime.get(), records, Optional.of(extraMetadata));
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

  public HudiTestSuiteConfig getCfg() {
    return cfg;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }
}
