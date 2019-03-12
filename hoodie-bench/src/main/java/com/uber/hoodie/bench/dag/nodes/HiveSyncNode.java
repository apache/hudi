package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.helpers.HiveServerWrapper;
import com.uber.hoodie.bench.writer.DeltaWriter;

public class HiveSyncNode extends DagNode<Boolean> {

  private HiveServerWrapper hiveServerWrapper;

  public HiveSyncNode(Config config) {
    this.config = config;
    this.hiveServerWrapper = new HiveServerWrapper(config);
  }

  public void execute(DeltaWriter writer) throws Exception {
    log.info("Executing hive sync node...");
    this.hiveServerWrapper.startLocalHiveServiceIfNeeded(writer.getConfiguration());
    this.hiveServerWrapper.syncToLocalHiveIfNeeded(writer);
    writer.getDeltaStreamerWrapper().getDeltaSyncService().getDeltaSync().syncHive();
    this.hiveServerWrapper.stopLocalHiveServiceIfNeeded();
  }

  public HiveServerWrapper getHiveServerWrapper() {
    return hiveServerWrapper;
  }
}
