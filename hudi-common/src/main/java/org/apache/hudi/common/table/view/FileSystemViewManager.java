/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Functions.Function2;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A container that can potentially hold one or more table's file-system views. There is one view for each table.
 * This is a view built against a timeline containing completed actions. In an embedded timeline-server mode, this
 * typically holds only one table's view. In a stand-alone server mode, this can hold more than one table's views.
 * <p>
 * FileSystemView can be stored "locally" using the following storage mechanisms: a. In Memory b. Spillable Map c.
 * RocksDB.
 * <p>
 * But there can be cases where the file-system view is managed remoted. For example : Embedded Timeline Server). In
 * this case, the clients will configure a remote filesystem view client (RemoteHoodieTableFileSystemView) for the
 * table which can connect to the remote file system view and fetch views. There are 2 modes here : REMOTE_FIRST and
 * REMOTE_ONLY. REMOTE_FIRST : The file-system view implementation on client side will act as a remote proxy. In case, if
 * there is problem (or exceptions) querying remote file-system view, a backup local file-system view(using either one
 * of in-memory, spillable, rocksDB) is used to server file-system view queries. REMOTE_ONLY : In this case, there is no
 * backup local file-system view. If there is problem (or exceptions) querying remote file-system view, then the
 * exceptions are percolated back to client.
 * <p>
 * FileSystemViewManager is designed to encapsulate the file-system view storage from clients using the file-system
 * view. FileSystemViewManager uses a factory to construct specific implementation of file-system view and passes it to
 * clients for querying.
 */
public class FileSystemViewManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemViewManager.class);

  private static final String HOODIE_METASERVER_FILE_SYSTEM_VIEW_CLASS = "org.apache.hudi.common.table.view.HoodieMetaserverFileSystemView";

  private final StorageConfiguration<?> conf;
  // The View Storage config used to store file-system views
  private final FileSystemViewStorageConfig viewStorageConfig;
  // Factory Map to create file-system views
  private final Function2<HoodieTableMetaClient, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator;
  // Map from Base-Path to View
  private final ConcurrentHashMap<String, SyncableFileSystemView> globalViewMap;

  private FileSystemViewManager(
      HoodieEngineContext context,
      FileSystemViewStorageConfig viewStorageConfig,
      Function2<HoodieTableMetaClient, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator) {
    this.conf = context.getStorageConf();
    this.viewStorageConfig = viewStorageConfig;
    this.viewCreator = viewCreator;
    this.globalViewMap = new ConcurrentHashMap<>();
  }

  /**
   * Drops reference to File-System Views. Future calls to view results in creating a new view
   *
   * @param basePath Hoodie table base path
   */
  public void clearFileSystemView(String basePath) {
    SyncableFileSystemView view = globalViewMap.remove(basePath);
    if (view != null) {
      view.close();
    }
  }

  /**
   * Main API to get the file-system view for the base-path.
   *
   * @param basePath Hoodie table base path
   * @return {@link SyncableFileSystemView}
   */
  public SyncableFileSystemView getFileSystemView(String basePath) {
    return globalViewMap.computeIfAbsent(basePath, (path) -> {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(conf.newInstance()).setBasePath(path).build();
      return viewCreator.apply(metaClient, viewStorageConfig);
    });
  }

  /**
   * Main API to get the file-system view for the base-path.
   *
   * @param metaClient HoodieTableMetaClient
   * @return {@link SyncableFileSystemView}
   */
  public SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) {
    return globalViewMap.computeIfAbsent(metaClient.getBasePath().toString(),
        (path) -> viewCreator.apply(metaClient, viewStorageConfig));
  }

  /**
   * Closes all views opened.
   */
  public void close() {
    if (!this.globalViewMap.isEmpty()) {
      this.globalViewMap.values().forEach(SyncableFileSystemView::close);
      this.globalViewMap.clear();
    }
  }

  // FACTORY METHODS FOR CREATING FILE-SYSTEM VIEWS

  /**
   * Create RocksDB based file System view for a table.
   *
   * @param engineContext engine context used
   * @param viewConf View Storage Configuration
   * @param metaClient HoodieTableMetaClient
   * @param metadataCreator creates {@link HoodieTableMetadata}
   * @return
   */
  private static RocksDbBasedFileSystemView createRocksDBBasedFileSystemView(HoodieEngineContext engineContext, FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient,
                                                                             boolean metadataTableEnabled,
                                                                             SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
    LOG.info("Creating RocksDB based view for basePath {}.", metaClient.getBasePath());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    HoodieTableMetadata tableMetadata = getTableMetadata(engineContext, metaClient, metadataTableEnabled, metadataCreator);
    return new RocksDbBasedFileSystemView(tableMetadata, metaClient, timeline, viewConf);
  }

  /**
   * Create a spillable Map based file System view for a table.
   *
   * @param viewConf   View Storage Configuration
   * @param metaClient HoodieTableMetaClient
   * @return {@link SpillableMapBasedFileSystemView}
   */
  private static SpillableMapBasedFileSystemView createSpillableMapBasedFileSystemView(HoodieEngineContext engineContext, FileSystemViewStorageConfig viewConf,
                                                                                       HoodieTableMetaClient metaClient, HoodieCommonConfig commonConfig,
                                                                                       boolean metadataTableEnabled,
                                                                                       SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
    LOG.info("Creating SpillableMap based view for basePath {}.", metaClient.getBasePath());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    HoodieTableMetadata tableMetadata = getTableMetadata(engineContext, metaClient, metadataTableEnabled, metadataCreator);
    return new SpillableMapBasedFileSystemView(tableMetadata, metaClient, timeline, viewConf, commonConfig);
  }

  /**
   * Create an in-memory file System view for a table.
   */
  private static HoodieTableFileSystemView createInMemoryFileSystemView(HoodieEngineContext engineContext, FileSystemViewStorageConfig viewConf,
                                                                        HoodieTableMetaClient metaClient, boolean metadataTableEnabled,
                                                                        SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
    LOG.info("Creating InMemory based view for basePath {}.", metaClient.getBasePath());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    HoodieTableMetadata tableMetadata = getTableMetadata(engineContext, metaClient, metadataTableEnabled, metadataCreator);
    if (metaClient.getMetaserverConfig().isMetaserverEnabled()) {
      return (HoodieTableFileSystemView) ReflectionUtils.loadClass(HOODIE_METASERVER_FILE_SYSTEM_VIEW_CLASS,
          new Class<?>[]{HoodieTableMetaClient.class, HoodieTimeline.class, HoodieMetaserverConfig.class},
          metaClient, timeline, metaClient.getMetaserverConfig());
    }
    return new HoodieTableFileSystemView(tableMetadata, metaClient, timeline, viewConf.isIncrementalTimelineSyncEnabled());
  }

  private static HoodieTableMetadata getTableMetadata(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                                                      boolean metadataTableEnabled,
                                                      SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
    if (metadataTableEnabled && metaClient.getTableConfig().isMetadataTableAvailable()) {
      ValidationUtils.checkArgument(metadataCreator != null, "Metadata supplier is null. Cannot instantiate metadata file system view");
      return metadataCreator.apply(metaClient);
    } else {
      return new FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig(), metaClient.getStorage(), metaClient.getBasePath().toString());
    }
  }

  public static HoodieTableFileSystemView createInMemoryFileSystemView(
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, HoodieMetadataConfig metadataConfig) {
    return createInMemoryFileSystemViewWithTimeline(engineContext, metaClient, metadataConfig,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
  }

  public static HoodieTableFileSystemView createInMemoryFileSystemViewWithTimeline(
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      HoodieMetadataConfig metadataConfig,
      HoodieTimeline timeline) {
    LOG.info("Creating InMemory based view for basePath {}.", metaClient.getBasePath());
    HoodieTableMetadata tableMetadata = getTableMetadata(engineContext, metaClient, metadataConfig.isEnabled(),
        unused -> metaClient.getTableFormat().getMetadataFactory().create(engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString()));

    if (metaClient.getMetaserverConfig().isMetaserverEnabled()) {
      return (HoodieTableFileSystemView) ReflectionUtils.loadClass(HOODIE_METASERVER_FILE_SYSTEM_VIEW_CLASS,
          new Class<?>[]{HoodieTableMetaClient.class, HoodieTimeline.class, HoodieMetaserverConfig.class},
          metaClient, timeline, metaClient.getMetaserverConfig());
    }
    return new HoodieTableFileSystemView(tableMetadata, metaClient, timeline);
  }

  /**
   * Create a remote file System view for a table.
   *
   * @param viewConf   View Storage Configuration
   * @param metaClient Hoodie Table MetaClient for the table.
   * @return {@link RemoteHoodieTableFileSystemView}
   */
  private static RemoteHoodieTableFileSystemView createRemoteFileSystemView(FileSystemViewStorageConfig viewConf,
                                                                            HoodieTableMetaClient metaClient) {
    LOG.info("Creating remote view for basePath {}. Server={}:{}, Timeout={}", metaClient.getBasePath(),
        viewConf.getRemoteViewServerHost(), viewConf.getRemoteViewServerPort(), viewConf.getRemoteTimelineClientTimeoutSecs());
    return new RemoteHoodieTableFileSystemView(metaClient, viewConf);
  }

  public static FileSystemViewManager createViewManagerWithTableMetadata(
      final HoodieEngineContext context,
      final HoodieMetadataConfig metadataConfig,
      final FileSystemViewStorageConfig config,
      final HoodieCommonConfig commonConfig) {
    return createViewManager(context, metadataConfig, config, commonConfig,
        metaClient -> metaClient.getTableFormat().getMetadataFactory().create(context, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true));
  }

  public static FileSystemViewManager createViewManager(final HoodieEngineContext context,
                                                        final HoodieMetadataConfig metadataConfig,
                                                        final FileSystemViewStorageConfig config,
                                                        final HoodieCommonConfig commonConfig) {
    return createViewManager(context, metadataConfig, config, commonConfig, null);
  }

  /**
   * Main Factory method for building file-system views.
   */
  public static FileSystemViewManager createViewManager(final HoodieEngineContext context,
                                                        final HoodieMetadataConfig metadataConfig,
                                                        final FileSystemViewStorageConfig config,
                                                        final HoodieCommonConfig commonConfig,
                                                        final SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
    LOG.info("Creating View Manager with storage type {}.", config.getStorageType());
    boolean metadataTableEnabled = metadataConfig.isEnabled();
    switch (config.getStorageType()) {
      case EMBEDDED_KV_STORE:
        LOG.debug("Creating embedded rocks-db based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConf) -> createRocksDBBasedFileSystemView(context, viewConf, metaClient, metadataTableEnabled, metadataCreator));
      case SPILLABLE_DISK:
        LOG.debug("Creating Spillable Disk based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConf) -> createSpillableMapBasedFileSystemView(context, viewConf, metaClient, commonConfig, metadataTableEnabled, metadataCreator));
      case MEMORY:
        LOG.debug("Creating in-memory based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConfig) -> createInMemoryFileSystemView(context, viewConfig, metaClient, metadataTableEnabled, metadataCreator));
      case REMOTE_ONLY:
        LOG.debug("Creating remote only table view");
        return new FileSystemViewManager(context, config, (metaClient, viewConfig) -> createRemoteFileSystemView(viewConfig,
            metaClient));
      case REMOTE_FIRST:
        LOG.debug("Creating remote first table view");
        return new FileSystemViewManager(context, config, (metaClient, viewConfig) -> {
          RemoteHoodieTableFileSystemView remoteFileSystemView =
              createRemoteFileSystemView(viewConfig, metaClient);
          SecondaryViewCreator secondaryViewSupplier = new SecondaryViewCreator(viewConfig, metaClient, commonConfig, metadataTableEnabled, metadataCreator);
          return new PriorityBasedFileSystemView(remoteFileSystemView, secondaryViewSupplier, context);
        });
      default:
        throw new IllegalArgumentException("Unknown file system view type :" + config.getStorageType());
    }
  }

  static class SecondaryViewCreator implements SerializableFunctionUnchecked<HoodieEngineContext, SyncableFileSystemView> {    private final FileSystemViewStorageConfig viewConfig;
    private final HoodieTableMetaClient metaClient;
    private final HoodieCommonConfig commonConfig;
    private final boolean metadataTableEnabled;
    private final SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator;

    SecondaryViewCreator(FileSystemViewStorageConfig viewConfig,
                         HoodieTableMetaClient metaClient, HoodieCommonConfig commonConfig,
                         boolean metadataTableEnabled,
                         SerializableFunctionUnchecked<HoodieTableMetaClient, HoodieTableMetadata> metadataCreator) {
      this.viewConfig = viewConfig;
      this.metaClient = metaClient;
      this.commonConfig = commonConfig;
      this.metadataTableEnabled = metadataTableEnabled;
      this.metadataCreator = metadataCreator;
    }

    @Override
    public SyncableFileSystemView apply(HoodieEngineContext engineContext) {
      switch (viewConfig.getSecondaryStorageType()) {
        case MEMORY:
          return createInMemoryFileSystemView(engineContext, viewConfig, metaClient, metadataTableEnabled, metadataCreator);
        case EMBEDDED_KV_STORE:
          return createRocksDBBasedFileSystemView(engineContext, viewConfig, metaClient, metadataTableEnabled, metadataCreator);
        case SPILLABLE_DISK:
          return createSpillableMapBasedFileSystemView(engineContext, viewConfig, metaClient, commonConfig, metadataTableEnabled, metadataCreator);
        default:
          throw new IllegalArgumentException("Secondary Storage type can only be in-memory or spillable. Was :"
              + viewConfig.getSecondaryStorageType());
      }
    }
  }
}
