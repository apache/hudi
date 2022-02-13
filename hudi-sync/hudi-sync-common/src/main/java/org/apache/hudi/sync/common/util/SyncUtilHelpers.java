package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.Properties;

/**
 * Helper class for syncing Hudi commit data with external metastores.
 */
public class SyncUtilHelpers {

  /**
   * Create an instance of an implementation of {@link AbstractSyncTool} that will sync all the relevant meta information
   * with an external metastore such as Hive etc. to ensure Hoodie tables can be queried or read via external systems.
   *
   * @param metaSyncClass  The class that implements the sync of the metadata.
   * @param props          property map.
   * @param hadoopConfig   Hadoop confs.
   * @param fs             Filesystem used.
   * @param targetBasePath The target base path that contains the hoodie table.
   * @param baseFileFormat The file format used by the hoodie table (defaults to PARQUET).
   * @return
   */
  public static void createAndSyncHoodieMeta(String metaSyncClass,
                                             TypedProperties props,
                                             Configuration hadoopConfig,
                                             FileSystem fs,
                                             String targetBasePath,
                                             String baseFileFormat) {
    try {
      createMetaSyncClass(metaSyncClass, props, hadoopConfig, fs, targetBasePath, baseFileFormat).syncHoodieTable();
    } catch (Throwable e) {
      throw new HoodieException("Could not sync using the meta sync class " + metaSyncClass, e);
    }
  }

  static AbstractSyncTool createMetaSyncClass(String metaSyncClass,
                                             TypedProperties props,
                                             Configuration hadoopConfig,
                                             FileSystem fs,
                                             String targetBasePath,
                                             String baseFileFormat) {
    TypedProperties properties = new TypedProperties();
    properties.putAll(props);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH, targetBasePath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT, baseFileFormat);

    try {
      return ((AbstractSyncTool) ReflectionUtils.loadClass(metaSyncClass,
          new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class},
          properties, hadoopConfig, fs));
    } catch (HoodieException e) {
      // fallback to old interface
      return ((AbstractSyncTool) ReflectionUtils.loadClass(metaSyncClass,
          new Class<?>[] {Properties.class, FileSystem.class}, properties, fs));
    } catch (Throwable e) {
      throw new HoodieException("Could not load meta sync class " + metaSyncClass, e);
    }
  }
}
