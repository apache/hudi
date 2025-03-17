package org.apache.hudi.metadata;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;

public class SparkMetadataWriterFactory {

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig, HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp, HoodieTableConfig tableConfig) {
    if (tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      return SparkHoodieBackedTableMetadataWriterTableVersionSix.create(conf, writeConfig, context, inflightInstantTimestamp);
    } else {
      return SparkHoodieBackedTableMetadataWriter.create(conf, writeConfig, context, inflightInstantTimestamp);
    }
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context, Option<String> inflightInstantTimestamp, HoodieTableConfig tableConfig) {
    if (tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      return new SparkHoodieBackedTableMetadataWriterTableVersionSix(conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
    } else {
      return new SparkHoodieBackedTableMetadataWriter(conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
    }
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig, HoodieEngineContext context, HoodieTableConfig tableConfig) {
    return create(conf, writeConfig, context, Option.empty(), tableConfig);
  }
}
