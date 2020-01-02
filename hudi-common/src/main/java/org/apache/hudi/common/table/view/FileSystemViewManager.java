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

import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.util.Functions.Function2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A container that can potentially hold one or more dataset's file-system views. There is one view for each dataset.
 * This is a view built against a timeline containing completed actions. In an embedded timeline-server mode, this
 * typically holds only one dataset's view. In a stand-alone server mode, this can hold more than one dataset's views.
 *
 * FileSystemView can be stored "locally" using the following storage mechanisms: a. In Memory b. Spillable Map c.
 * RocksDB
 *
 * But there can be cases where the file-system view is managed remoted. For example : Embedded Timeline Server). In
 * this case, the clients will configure a remote filesystem view client (RemoteHoodieTableFileSystemView) for the
 * dataset which can connect to the remote file system view and fetch views. THere are 2 modes here : REMOTE_FIRST and
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
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemViewManager.class);

  private final SerializableConfiguration conf;
  // The View Storage config used to store file-system views
  private final FileSystemViewStorageConfig viewStorageConfig;
  // Map from Base-Path to View
  private final ConcurrentHashMap<String, SyncableFileSystemView> globalViewMap;
  // Factory Map to create file-system views
  private final Function2<String, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator;

  public FileSystemViewManager(SerializableConfiguration conf, FileSystemViewStorageConfig viewStorageConfig,
      Function2<String, FileSystemViewStorageConfig, SyncableFileSystemView> viewCreator) {
    this.conf = new SerializableConfiguration(conf);
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
    return globalViewMap.computeIfAbsent(basePath, (path) -> viewCreator.apply(path, viewStorageConfig));
  }

  /**
   * Closes all views opened.
   */
  public void close() {
    this.globalViewMap.values().stream().forEach(v -> v.close());
    this.globalViewMap.clear();
  }

  // FACTORY METHODS FOR CREATING FILE-SYSTEM VIEWS

  /**
   * Create RocksDB based file System view for a dataset.
   * 
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param basePath Base Path of dataset
   * @return
   */
  private static RocksDbBasedFileSystemView createRocksDBBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.newCopy(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new RocksDbBasedFileSystemView(metaClient, timeline, viewConf);
  }

  /**
   * Create a spillable Map based file System view for a dataset.
   * 
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param basePath Base Path of dataset
   * @return
   */
  private static SpillableMapBasedFileSystemView createSpillableMapBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    LOG.info("Creating SpillableMap based view for basePath {}", basePath);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.newCopy(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new SpillableMapBasedFileSystemView(metaClient, timeline, viewConf);
  }

  /**
   * Create an in-memory file System view for a dataset.
   * 
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param basePath Base Path of dataset
   * @return
   */
  private static HoodieTableFileSystemView createInMemoryFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    LOG.info("Creating InMemory based view for basePath {}", basePath);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.newCopy(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new HoodieTableFileSystemView(metaClient, timeline, viewConf.isIncrementalTimelineSyncEnabled());
  }

  /**
   * Create a remote file System view for a dataset.
   * 
   * @param conf Hadoop Configuration
   * @param viewConf View Storage Configuration
   * @param metaClient Hoodie Table MetaClient for the dataset.
   * @return
   */
  private static RemoteHoodieTableFileSystemView createRemoteFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient) {
    LOG.info("Creating remote view for basePath {}. Server={}:{}", metaClient.getBasePath(), viewConf.getRemoteViewServerHost(), viewConf.getRemoteViewServerPort());
    return new RemoteHoodieTableFileSystemView(viewConf.getRemoteViewServerHost(), viewConf.getRemoteViewServerPort(),
        metaClient);
  }

  /**
   * Main Factory method for building file-system views.
   * 
   * @param conf Hadoop Configuration
   * @param config View Storage Configuration
   * @return
   */
  public static FileSystemViewManager createViewManager(final SerializableConfiguration conf,
      final FileSystemViewStorageConfig config) {
    LOG.info("Creating View Manager with storage type :{}", config.getStorageType());
    switch (config.getStorageType()) {
      case EMBEDDED_KV_STORE:
        LOG.info("Creating embedded rocks-db based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConf) -> createRocksDBBasedFileSystemView(conf, viewConf, basePath));
      case SPILLABLE_DISK:
        LOG.info("Creating Spillable Disk based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConf) -> createSpillableMapBasedFileSystemView(conf, viewConf, basePath));
      case MEMORY:
        LOG.info("Creating in-memory based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConfig) -> createInMemoryFileSystemView(conf, viewConfig, basePath));
      case REMOTE_ONLY:
        LOG.info("Creating remote only table view");
        return new FileSystemViewManager(conf, config, (basePath, viewConfig) -> createRemoteFileSystemView(conf,
            viewConfig, new HoodieTableMetaClient(conf.newCopy(), basePath)));
      case REMOTE_FIRST:
        LOG.info("Creating remote first table view");
        return new FileSystemViewManager(conf, config, (basePath, viewConfig) -> {
          RemoteHoodieTableFileSystemView remoteFileSystemView =
              createRemoteFileSystemView(conf, viewConfig, new HoodieTableMetaClient(conf.newCopy(), basePath));
          SyncableFileSystemView secondaryView = null;
          switch (viewConfig.getSecondaryStorageType()) {
            case MEMORY:
              secondaryView = createInMemoryFileSystemView(conf, viewConfig, basePath);
              break;
            case EMBEDDED_KV_STORE:
              secondaryView = createRocksDBBasedFileSystemView(conf, viewConfig, basePath);
              break;
            case SPILLABLE_DISK:
              secondaryView = createSpillableMapBasedFileSystemView(conf, viewConfig, basePath);
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
