package org.apache.hudi.utilities.benchmarking;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.IOException;

public class SparkHoodieBackedTableMetadataBenchmarkWriter extends SparkHoodieBackedTableMetadataWriter {

  SparkHoodieBackedTableMetadataBenchmarkWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp,
                                       boolean streamingWrites) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, streamingWrites);
  }


  protected boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                       Option<String> inflightInstantTimestamp) throws IOException {
    // no op.
    return false;
  }

  protected void initMetadataMetaClient() throws IOException {
    metadataMetaClient = initializeMetaClient();
  }
}
