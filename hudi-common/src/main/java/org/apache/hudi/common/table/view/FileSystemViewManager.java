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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Functions.Function2;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A container that can potentially hold one or more table's file-system views. There is one view for each table.
 * This is a view built against a timeline containing completed actions. In an embedded timeline-server mode, this
 * typically holds only one table's view. In a stand-alone server mode, this can hold more than one table's views.
 *
 * FileSystemView can be stored "locally" using the following storage mechanisms: a. In Memory b. Spillable Map c.
 * RocksDB
 *
 * But there can be cases where the file-system view is managed remoted. For example : Embedded Timeline Server). In
 * this case, the clients will configure a remote filesystem view client (RemoteHoodieTableFileSystemView) for the
 * table which can connect to the remote file system view and fetch views. THere are 2 modes here : REMOTE_FIRST and
 * REMOTE_ONLY REMOTE_FIRST : The file-system view implementation on client side will act as a remote proxy. In case, if
 * there is problem (or exceptions) querying remote file-system view, a backup local file-system view(using either one
 * of in-memory, spillable, rocksDB) is used to server file-system view queries REMOTE_ONLY : In this case, there is no
 * backup local file-system view. If there is problem (or exceptions) querying remote file-system view, then the
 * exceptions are percolated back to client.
 *
 * FileSystemViewManager is designed to encapsulate the file-system view storage from clients using the file-system
 * view. FileSystemViewManager uses a factory to construct specific implementation of file-system view and passes it to
 * clients for querying.
 */
public class FileSystemViewManager {
  private static final Logger LOG = LogManager.getLogger(FileSystemViewManager.class);

  private final SerializableConfiguration conf;
  // The View Storage config used to store file-system views
  private final FileSystemViewStorageConfig viewStorageConfig;
  // Map from Base-Path to View
  private final ConcurrentHashMap<String, SyncableFileSystemView> globalViewMap;
  // Factory Map to create file-system views
  private final Function2<HoodieTableMetaClient, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator;

  private FileSystemViewManager(HoodieEngineContext context, FileSystemViewStorageConfig viewStorageConfig,
      Function2<HoodieTableMetaClient, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator) {
    this.conf = context.getHadoopConf();
    this.viewStorageConfig = viewStorageConfig;
    this.globalViewMap = new ConcurrentHashMap<>();
    this.viewCreator = viewCreator;
  }

  /**
   * Drops reference to File-System Views. Future calls to view results in creating a new view
   *
   * @param basePath
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
   * @param basePath
   * @return
   */
  public SyncableFileSystemView getFileSystemView(String basePath) {
    return globalViewMap.computeIfAbsent(basePath, (path) -> {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(conf.newCopy()).setBasePath(path).build();
      return viewCreator.apply(metaClient, viewStorageConfig);
    });
  }

  /**
   * Main API to get the file-system view for the base-path.
   *
   * @param metaClient HoodieTableMetaClient
   * @return
   */
  public SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) {
    return globalViewMap.computeIfAbsent(metaClient.getBasePath(),
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
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param metaClient HoodieTableMetaClient
   * @return
   */
  private static RocksDbBasedFileSystemView createRocksDBBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new RocksDbBasedFileSystemView(metaClient, timeline, viewConf);
  }

  /**
   * Create a spillable Map based file System view for a table.
   *
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param metaClient HoodieTableMetaClient
   * @return
   */
  private static SpillableMapBasedFileSystemView createSpillableMapBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient, HoodieCommonConfig commonConfig) {
    LOG.info("Creating SpillableMap based view for basePath " + metaClient.getBasePath());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new SpillableMapBasedFileSystemView(metaClient, timeline, viewConf, commonConfig);
  }

  /**
   * Create an in-memory file System view for a table.
   *
   */
  private static HoodieTableFileSystemView createInMemoryFileSystemView(HoodieMetadataConfig metadataConfig, FileSystemViewStorageConfig viewConf,
                                                                        HoodieTableMetaClient metaClient, SerializableSupplier<HoodieTableMetadata> metadataSupplier) {
    LOG.info("Creating InMemory based view for basePath " + metaClient.getBasePath());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    if (metaClient.getTableConfig().isMetadataTableEnabled()) {
      ValidationUtils.checkArgument(metadataSupplier != null, "Metadata supplier is null. Cannot instantiate metadata file system view");
      return new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
          metadataSupplier.get());
    }
    return new HoodieTableFileSystemView(metaClient, timeline, viewConf.isIncrementalTimelineSyncEnabled());
  }

  public static HoodieTableFileSystemView createInMemoryFileSystemView(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                                                                       boolean useMetadataTable) {

    return createInMemoryFileSystemViewWithTimeline(engineContext, metaClient, useMetadataTable,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());

  }

  public static HoodieTableFileSystemView createInMemoryFileSystemViewWithTimeline(HoodieEngineContext engineContext,
                                                                                   HoodieTableMetaClient metaClient,
                                                                                   boolean useMetadataTable,
                                                                                   HoodieTimeline timeline) {
    LOG.info("Creating InMemory based view for basePath " + metaClient.getBasePath());
    if (useMetadataTable) {
      return new HoodieMetadataFileSystemView(engineContext, metaClient, timeline);
    }
    return new HoodieTableFileSystemView(metaClient, timeline);
  }

  /**
   * Create a remote file System view for a table.
   *
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param metaClient Hoodie Table MetaClient for the table.
   * @return
   */
  private static RemoteHoodieTableFileSystemView createRemoteFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient) {
    LOG.info("Creating remote view for basePath " + metaClient.getBasePath() + ". Server="
        + viewConf.getRemoteViewServerHost() + ":" + viewConf.getRemoteViewServerPort() + ", Timeout="
        + viewConf.getRemoteTimelineClientTimeoutSecs());
    return new RemoteHoodieTableFileSystemView(viewConf.getRemoteViewServerHost(), viewConf.getRemoteViewServerPort(),
        metaClient, viewConf.getRemoteTimelineClientTimeoutSecs());
  }

  public static FileSystemViewManager createViewManager(final HoodieEngineContext context,
                                                        final HoodieMetadataConfig metadataConfig,
                                                        final FileSystemViewStorageConfig config,
                                                        final HoodieCommonConfig commonConfig) {
    return createViewManager(context, metadataConfig, config, commonConfig, (SerializableSupplier<HoodieTableMetadata>) null);
  }

  public static FileSystemViewManager createViewManager(final HoodieEngineContext context,
                                                        final HoodieMetadataConfig metadataConfig,
                                                        final FileSystemViewStorageConfig config,
                                                        final HoodieCommonConfig commonConfig,
                                                        final String basePath) {
    return createViewManager(context, metadataConfig, config, commonConfig,
        () -> HoodieTableMetadata.create(context, metadataConfig, basePath));
  }

  /**
   * Main Factory method for building file-system views.
   *
   */
  public static FileSystemViewManager createViewManager(final HoodieEngineContext context,
                                                        final HoodieMetadataConfig metadataConfig,
                                                        final FileSystemViewStorageConfig config,
                                                        final HoodieCommonConfig commonConfig,
                                                        final SerializableSupplier<HoodieTableMetadata> metadataSupplier) {
    LOG.info("Creating View Manager with storage type :" + config.getStorageType());
    final SerializableConfiguration conf = context.getHadoopConf();
    switch (config.getStorageType()) {
      case EMBEDDED_KV_STORE:
        LOG.info("Creating embedded rocks-db based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConf) -> createRocksDBBasedFileSystemView(conf, viewConf, metaClient));
      case SPILLABLE_DISK:
        LOG.info("Creating Spillable Disk based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConf) -> createSpillableMapBasedFileSystemView(conf, viewConf, metaClient, commonConfig));
      case MEMORY:
        LOG.info("Creating in-memory based Table View");
        return new FileSystemViewManager(context, config,
            (metaClient, viewConfig) -> createInMemoryFileSystemView(metadataConfig, viewConfig, metaClient, metadataSupplier));
      case REMOTE_ONLY:
        LOG.info("Creating remote only table view");
        return new FileSystemViewManager(context, config, (metaClient, viewConfig) -> createRemoteFileSystemView(conf,
            viewConfig, metaClient));
      case REMOTE_FIRST:
        LOG.info("Creating remote first table view");
        return new FileSystemViewManager(context, config, (metaClient, viewConfig) -> {
          RemoteHoodieTableFileSystemView remoteFileSystemView =
              createRemoteFileSystemView(conf, viewConfig, metaClient);
          SyncableFileSystemView secondaryView;
          switch (viewConfig.getSecondaryStorageType()) {
            case MEMORY:
              secondaryView = createInMemoryFileSystemView(metadataConfig, viewConfig, metaClient, metadataSupplier);
              break;
            case EMBEDDED_KV_STORE:
              secondaryView = createRocksDBBasedFileSystemView(conf, viewConfig, metaClient);
              break;
            case SPILLABLE_DISK:
              secondaryView = createSpillableMapBasedFileSystemView(conf, viewConfig, metaClient, commonConfig);
              break;
            default:
              throw new IllegalArgumentException("Secondary Storage type can only be in-memory or spillable. Was :"
                  + viewConfig.getSecondaryStorageType());
          }
          return new PriorityBasedFileSystemView(remoteFileSystemView, secondaryView);
        });
      default:
        throw new IllegalArgumentException("Unknown file system view type :" + config.getStorageType());
    }
  }
}
