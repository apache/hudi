package org.apache.hudi.client.utils;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkReleaseResources {

  /**
   * Called after each write commit, compaction commit and clustering commit
   * to unpersist all RDDs persisted or cached per table.
   * @param context the relevant {@link HoodieEngineContext}
   * @param config writer configs {@link HoodieWriteConfig}
   * @param basePath table base path
   * @param instantTime instant time for which the RDDs need to be unpersisted.
   */
  public static void releaseCachedData(HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      String basePath,
                                      String instantTime) {
    // If we do not explicitly release the resource, spark will automatically manage the resource and clean it up automatically
    // see: https://spark.apache.org/docs/latest/rdd-programming-guide.html#removing-data
    if (config.areReleaseResourceEnabled()) {
      HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) context;
      Map<Integer, JavaRDD<?>> allCachedRdds = sparkEngineContext.getJavaSparkContext().getPersistentRDDs();
      List<Integer> allDataIds = new ArrayList<>(sparkEngineContext.removeCachedDataIds(HoodieData.HoodieDataCacheKey.of(basePath, instantTime)));
      if (config.isMetadataTableEnabled()) {
        String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
        allDataIds.addAll(sparkEngineContext.removeCachedDataIds(HoodieData.HoodieDataCacheKey.of(metadataTableBasePath, instantTime)));
      }
      for (int id : allDataIds) {
        if (allCachedRdds.containsKey(id)) {
          allCachedRdds.get(id).unpersist();
        }
      }
    }
  }
}
