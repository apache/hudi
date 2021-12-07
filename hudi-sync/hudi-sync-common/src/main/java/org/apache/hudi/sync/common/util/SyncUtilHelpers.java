package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

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
   * @param baseFileFormat The file format used by the hoodie table (defauls to PARQUET).
   * @return
   * @throws IOException
   */
  public static void createAndSyncHoodieMeta(String metaSyncClass, TypedProperties props, Configuration hadoopConfig, FileSystem fs,
                                             String targetBasePath, String baseFileFormat) {
    TypedProperties properties = new TypedProperties();
    properties.putAll(props);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH, targetBasePath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT, baseFileFormat);
    ((AbstractSyncTool) ReflectionUtils.loadClass(metaSyncClass,
        new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class},
        properties, hadoopConfig, fs)).syncHoodieTable();
  }
}
