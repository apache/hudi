package com.uber.hoodie.bench.job;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Extends the {@link HoodieDeltaStreamer} to expose certain operations helpful in running the Test Suite.
 * This is done to achieve 2 things 1) Leverage some components of {@link HoodieDeltaStreamer} 2)
 * Piggyback on the suite to test {@link HoodieDeltaStreamer}
 */
public class HoodieDeltaStreamerWrapper extends HoodieDeltaStreamer {

  public HoodieDeltaStreamerWrapper(Config cfg, JavaSparkContext jssc) throws Exception {
    super(cfg, jssc);
  }

  public HoodieDeltaStreamerWrapper(Config cfg, JavaSparkContext jssc, FileSystem fs, HiveConf conf) throws Exception {
    super(cfg, jssc, fs, conf);
  }

  public JavaRDD<WriteStatus> upsert(Optional<String> instantTime) throws
      Exception {
    return deltaSyncService.getDeltaSync().syncOnce().getRight();
  }

  public JavaRDD<WriteStatus> insert(Optional<String> instantTime) throws Exception { //
    return upsert(instantTime);
  }

  public JavaRDD<WriteStatus> bulkInsert(Optional<String> instantTime) throws
      Exception {
    return upsert(instantTime);
  }

  public JavaRDD<WriteStatus> compact(Optional<String> instantTime) throws Exception { //
    return upsert(instantTime);
  }

  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return deltaSyncService.getDeltaSync().readFromSource(deltaSyncService.getDeltaSync().getCommitTimelineOpt());
  }

}
