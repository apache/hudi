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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.TIMESTAMP_AS_OF;

/**
 * Given a path is a part of - Hoodie table = accepts ONLY the latest version of each path - Non-Hoodie table = then
 * always accept
 * <p>
 * We can set this filter, on a query engine's Hadoop Config and if it respects path filters, then you should be able to
 * query both hoodie and non-hoodie tables as you would normally do.
 * <p>
 * hadoopConf.setClass("mapreduce.input.pathFilter.class", org.apache.hudi.hadoop .HoodieROTablePathFilter.class,
 * org.apache.hadoop.fs.PathFilter.class)
 */
public class HoodieROTablePathFilter implements Configurable, PathFilter, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieROTablePathFilter.class);

  /**
   * Its quite common, to have all files from a given partition path be passed into accept(), cache the check for hoodie
   * metadata for known partition paths and the latest versions of files.
   */
  private Map<String, HashSet<Path>> hoodiePathCache;

  /**
   * Paths that are known to be non-hoodie tables.
   */
  Set<String> nonHoodiePathCache;

  /**
   * Table Meta Client Cache.
   */
  Map<String, HoodieTableMetaClient> metaClientCache;

  /**
   * Hadoop configurations for the FileSystem.
   */
  private SerializableConfiguration conf;

  private transient HoodieLocalEngineContext engineContext;


  private transient FileSystem fs;

  public HoodieROTablePathFilter() {
    this(new Configuration());
  }

  public HoodieROTablePathFilter(Configuration conf) {
    this.hoodiePathCache = new ConcurrentHashMap<>();
    this.nonHoodiePathCache = new HashSet<>();
    this.conf = new SerializableConfiguration(conf);
    this.metaClientCache = new HashMap<>();
  }

  /**
   * Obtain the path, two levels from provided path.
   *
   * @return said path if available, null otherwise
   */
  private Path safeGetParentsParent(Path path) {
    if (path.getParent() != null && path.getParent().getParent() != null
        && path.getParent().getParent().getParent() != null) {
      return path.getParent().getParent().getParent();
    }
    return null;
  }

  @Override
  public boolean accept(Path path) {

    if (engineContext == null) {
      this.engineContext = new HoodieLocalEngineContext(this.conf.get());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking acceptance for path " + path);
    }
    Path folder = null;
    try {
      if (fs == null) {
        fs = path.getFileSystem(conf.get());
      }

      // Assumes path is a file
      folder = path.getParent(); // get the immediate parent.
      // Try to use the caches.
      if (nonHoodiePathCache.contains(folder.toString())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Accepting non-hoodie path from cache: " + path);
        }
        return true;
      }

      if (hoodiePathCache.containsKey(folder.toString())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("%s Hoodie path checked against cache, accept => %s \n", path,
              hoodiePathCache.get(folder.toString()).contains(path)));
        }
        return hoodiePathCache.get(folder.toString()).contains(path);
      }

      // Skip all files that are descendants of .hoodie in its path.
      String filePath = path.toString();
      if (filePath.contains("/" + HoodieTableMetaClient.METAFOLDER_NAME + "/")
          || filePath.endsWith("/" + HoodieTableMetaClient.METAFOLDER_NAME)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Skipping Hoodie Metadata file  %s \n", filePath));
        }
        return false;
      }

      // Perform actual checking.
      Path baseDir;
      if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
        HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
        metadata.readFromFS();
        baseDir = HoodieHiveUtils.getNthParent(folder, metadata.getPartitionDepth());
      } else {
        baseDir = safeGetParentsParent(folder);
      }

      if (baseDir != null) {
        // Check whether baseDir in nonHoodiePathCache
        if (nonHoodiePathCache.contains(baseDir.toString())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Accepting non-hoodie path from cache: " + path);
          }
          return true;
        }
        HoodieTableFileSystemView fsView = null;
        try {
          HoodieTableMetaClient metaClient = metaClientCache.get(baseDir.toString());
          if (null == metaClient) {
            metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(baseDir.toString()).setLoadActiveTimelineOnLoad(true).build();
            metaClientCache.put(baseDir.toString(), metaClient);
          }

          if (getConf().get(TIMESTAMP_AS_OF.key()) != null) {
            // Build FileSystemViewManager with specified time, it's necessary to set this config when you may
            // access old version files. For example, in spark side, using "hoodie.datasource.read.paths"
            // which contains old version files, if not specify this value, these files will be filtered.
            fsView = FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(engineContext,
                metaClient, HoodieInputFormatUtils.buildMetadataConfig(getConf()),
                metaClient.getActiveTimeline().filterCompletedInstants().findInstantsBeforeOrEquals(getConf().get(TIMESTAMP_AS_OF.key())));
          } else {
            fsView = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
                metaClient, HoodieInputFormatUtils.buildMetadataConfig(getConf()));
          }
          String partition = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), folder);
          List<HoodieBaseFile> latestFiles = fsView.getLatestBaseFiles(partition).collect(Collectors.toList());
          // populate the cache
          if (!hoodiePathCache.containsKey(folder.toString())) {
            hoodiePathCache.put(folder.toString(), new HashSet<>());
          }
          LOG.info("Based on hoodie metadata from base path: " + baseDir.toString() + ", caching " + latestFiles.size()
              + " files under " + folder);
          for (HoodieBaseFile lfile : latestFiles) {
            hoodiePathCache.get(folder.toString()).add(new Path(lfile.getPath()));
          }

          // accept the path, if its among the latest files.
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%s checked after cache population, accept => %s \n", path,
                hoodiePathCache.get(folder.toString()).contains(path)));
          }
          return hoodiePathCache.get(folder.toString()).contains(path);
        } catch (TableNotFoundException e) {
          // Non-hoodie path, accept it.
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("(1) Caching non-hoodie path under %s with basePath %s \n", folder.toString(), baseDir.toString()));
          }
          nonHoodiePathCache.add(folder.toString());
          nonHoodiePathCache.add(baseDir.toString());
          return true;
        } finally {
          if (fsView != null) {
            fsView.close();
          }
        }
      } else {
        // files is at < 3 level depth in FS tree, can't be hoodie dataset
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("(2) Caching non-hoodie path under %s \n", folder.toString()));
        }
        nonHoodiePathCache.add(folder.toString());
        return true;
      }
    } catch (Exception e) {
      String msg = "Error checking path :" + path + ", under folder: " + folder;
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new SerializableConfiguration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf.get();
  }
}
