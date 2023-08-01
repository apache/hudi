package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

public class JavaHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {
  private transient BaseHoodieWriteClient writeClient;

  /**
   * Hudi backed table metadata writer.
   *
   * @param hadoopConf                 Hadoop configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected JavaHoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy, HoodieEngineContext engineContext,
                                                Option<String> inflightInstantTimestamp) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }


  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry = Registry.getRegistry("HoodieMetadata");
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected BaseHoodieWriteClient getWriteClient() {
    if (writeClient == null) {
      writeClient = new HoodieJavaWriteClient(engineContext, metadataWriteConfig);
    }
    return writeClient;
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Java metadata table yet.");
  }
}
