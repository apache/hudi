package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Helper class for syncing Hudi commit data with external metastores.
 */
public class SyncUtilHelpers {
  private static final Logger LOG = LogManager.getLogger(SyncUtilHelpers.class);

  /**
   * Create an instance of an implementation of {@link AbstractSyncTool} that will sync all the relevant meta information
   * with an external metastore such as Hive etc. to ensure Hoodie tables can be queried or read via external systems.
   *
   * @param metaSyncFQCN  The class that implements the sync of the metadata.
   * @param props          property map.
   * @param hadoopConfig   Hadoop confs.
   * @param fs             Filesystem used.
   * @param targetBasePath The target base path that contains the hoodie table.
   * @param baseFileFormat The file format used by the hoodie table (defaults to PARQUET).
   */
  public static void runHoodieMetaSync(String metaSyncFQCN,
                                       TypedProperties props,
                                       Configuration hadoopConfig,
                                       FileSystem fs,
                                       String targetBasePath,
                                       String baseFileFormat) {
    try {
      instantiateMetaSyncTool(metaSyncFQCN, props, hadoopConfig, fs, targetBasePath, baseFileFormat).syncHoodieTable();
    } catch (Throwable e) {
      throw new HoodieException("Could not sync using the meta sync class " + metaSyncFQCN, e);
    }
  }

  static AbstractSyncTool instantiateMetaSyncTool(String metaSyncFQCN,
                                                 TypedProperties props,
                                                 Configuration hadoopConfig,
                                                 FileSystem fs,
                                                 String targetBasePath,
                                                 String baseFileFormat) {
    TypedProperties properties = new TypedProperties();
    properties.putAll(props);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), targetBasePath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), baseFileFormat);

    if (ReflectionUtils.hasConstructor(metaSyncFQCN,
        new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class})) {
      return ((AbstractSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
          new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class},
          properties, hadoopConfig, fs));
    } else {
      LOG.warn("Falling back to deprecated constructor for class: " + metaSyncFQCN);
      try {
        return ((AbstractSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
            new Class<?>[] {Properties.class, FileSystem.class}, properties, fs));
      } catch (Throwable t) {
        throw new HoodieException("Could not load meta sync class " + metaSyncFQCN, t);
      }
    }
  }
}
