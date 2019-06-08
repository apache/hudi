package com.uber.hoodie.utilities;

import com.beust.jcommander.Parameter;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.config.AbstractCommandConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieRollback {

  private static final Logger LOG = Logger.getLogger(HoodieRollback.class);
  private final Config cfg;

  public HoodieRollback(Config cfg) {
    this.cfg = cfg;
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config);
  }

  public int rollback(JavaSparkContext jsc) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, cfg.basePath);
    if (client.rollback(cfg.commitTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", cfg.commitTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", cfg.commitTime));
      return -1;
    }
  }

  public int rollbackToSavepoint(JavaSparkContext jsc) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, cfg.basePath);
    if (client.rollbackToSavepoint(cfg.savepointTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", cfg.savepointTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", cfg.savepointTime));
      return -1;
    }
  }

  public static class Config extends AbstractCommandConfig {

    @Parameter(names = {"--commit-time",
        "-sp"}, description = "Commit time for rollback")
    public String commitTime = null;

    @Parameter(names = {"--savepoint-time",
        "-sp"}, description = "Savepoint time for rollback")
    public String savepointTime = null;

    @Parameter(names = {"--base-path",
        "-bp"}, description = "Base path for the hoodie dataset", required = true)
    public String basePath = null;
  }
}
