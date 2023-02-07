package org.apache.hudi.async;

import org.apache.hudi.client.BaseClusterer;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieSparkClusteringClient;
import org.apache.hudi.common.engine.HoodieEngineContext;

/**
 * Async clustering service for Spark datasource.
 */
public class SparkAsyncClusteringService extends AsyncClusteringService {

  public SparkAsyncClusteringService(HoodieEngineContext engineContext, BaseHoodieWriteClient writeClient) {
    super(engineContext, writeClient);
  }

  @Override
  protected BaseClusterer createClusteringClient(BaseHoodieWriteClient client) {
    return new HoodieSparkClusteringClient(client);
  }
}
