package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.bench.configuration.DeltaConfig;
import com.uber.hoodie.bench.helpers.HiveServerWrapper;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.hive.HiveSyncConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveQueryNode extends DagNode<Boolean> {

  private HiveServerWrapper hiveServerWrapper;

  public HiveQueryNode(DeltaConfig.Config config) {
    this.config = config;
    this.hiveServerWrapper = new HiveServerWrapper(config);
  }

  public Boolean execute(DeltaWriter writer) throws Exception {
    log.info("Executing hive query node..." + this.getName());
    this.hiveServerWrapper.startLocalHiveServiceIfNeeded(writer.getConfiguration());
    // this.hiveServerWrapper.syncToLocalHiveIfNeeded(writer);
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(writer.getDeltaStreamerWrapper()
        .getDeltaSyncService().getDeltaSync().getProps(), writer.getDeltaStreamerWrapper()
        .getDeltaSyncService().getDeltaSync().getCfg().targetBasePath);
    this.hiveServerWrapper.syncToLocalHiveIfNeeded(writer);
    Connection con = DriverManager.getConnection(hiveSyncConfig.jdbcUrl, hiveSyncConfig.hiveUser,
        hiveSyncConfig.hivePass);
    Statement stmt = con.createStatement();
    if (this.config.getHiveExecutionEngine() != null) {
      executeStatement("set hive.execution.engine=" + this.config.getHiveExecutionEngine(), stmt);
    }
    if (this.config.getHiveQueueName() != null) {
      executeStatement("set " + getQueueNameKeyForExecutionEngine(this.config.getHiveExecutionEngine())
          + "=" + this.config.getHiveQueueName(), stmt);
    }
    // Allow queries without partition predicate
    executeStatement("set hive.strict.checks.large.query=false", stmt);
    // Dont gather stats for the table created
    executeStatement("set hive.stats.autogather=false", stmt);
    for (Pair<String, Integer> queryAndResult : this.config.getHiveQueries()) {
      log.info("Running => " + queryAndResult.getLeft());
      ResultSet res = stmt.executeQuery(queryAndResult.getLeft());
      if (res.getRow() == 0) {
        assert 0 == queryAndResult.getRight();
      } else {
        assert res.getInt(0) == queryAndResult.getRight();
      }
      log.info("Successfully validated query!");
    }
    this.hiveServerWrapper.stopLocalHiveServiceIfNeeded();
    return true;
  }

  private void executeStatement(String query, Statement stmt) throws SQLException {
    log.info("Executing statement " + stmt.toString());
    stmt.execute(query);
  }

  private String getQueueNameKeyForExecutionEngine(String executionEngine) {
    if (executionEngine == null) {
      return "mapred.job.queue.name";
    }
    switch (executionEngine) {
      case "mr":
        return "mapred.job.queue.name";
      case "spark":
        return "spark.yarn.queue";
      default:
        throw new IllegalArgumentException("unknown execution engine");
    }
  }

}
