package com.uber.hoodie.bench.helpers;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.hive.util.HiveTestService;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.server.HiveServer2;

public class HiveServerWrapper {

  private HiveTestService hiveService;
  private HiveServer2 hiveServer;
  private Config config;

  public HiveServerWrapper(Config config) {
    this.config = config;
  }

  public void startLocalHiveServiceIfNeeded(Configuration configuration) throws IOException {
    if (config.isHiveLocal()) {
      hiveService = new HiveTestService(configuration);
      hiveServer = hiveService.start();
      configuration.iterator().forEachRemaining(a -> {
        System.out.println(a);
      });
    }
  }

  public void syncToLocalHiveIfNeeded(DeltaWriter writer) {
    if (this.config.isHiveLocal()) {
      writer.getDeltaStreamerWrapper().getDeltaSyncService().getDeltaSync()
          .syncHive(getLocalHiveServer().getHiveConf());
    } else {
      writer.getDeltaStreamerWrapper().getDeltaSyncService().getDeltaSync().syncHive();
    }
  }

  public void stopLocalHiveServiceIfNeeded() throws IOException {
    if (config.isHiveLocal()) {
      hiveServer.stop();
      hiveService.stop();
    }
  }

  public HiveServer2 getLocalHiveServer() {
    return hiveServer;
  }
}
