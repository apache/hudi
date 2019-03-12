package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.configuration.DeltaConfig;
import com.uber.hoodie.bench.helpers.HiveServerWrapper;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.util.collection.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLQueryNode extends DagNode<Boolean> {

  HiveServerWrapper hiveServerWrapper;

  public SparkSQLQueryNode(DeltaConfig.Config config) {
    this.config = config;
    this.hiveServerWrapper = new HiveServerWrapper(config);
  }

  public Boolean execute(JavaSparkContext jsc, DeltaWriter writer) throws Exception {
    log.info("Executing spark sql query node...");
    this.hiveServerWrapper.startLocalHiveServiceIfNeeded(writer.getConfiguration());
    this.hiveServerWrapper.syncToLocalHiveIfNeeded(writer);
    SparkSession session = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
    if (this.config.getHiveQueueName() != null) {
      session.sql("set spark.queue.name=" + this.config.getHiveQueueName());
    }
    for (Pair<String, Integer> queryAndResult : this.config.getHiveQueries()) {
      log.info("Running => " + queryAndResult.getLeft());
      Dataset<Row> res = session.sql(queryAndResult.getLeft());
      if (res.count() == 0) {
        assert 0 == queryAndResult.getRight();
      } else {
        assert ((Row [])res.collect())[0].getInt(0) == queryAndResult.getRight();
      }
      log.info("Successfully validated query!");
    }
    this.hiveServerWrapper.stopLocalHiveServiceIfNeeded();
    return true;
  }

}
