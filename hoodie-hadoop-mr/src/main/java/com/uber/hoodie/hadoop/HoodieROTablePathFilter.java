/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.hoodie.hadoop;

import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.InvalidDatasetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Given a path is a part of
 * - Hoodie dataset => accepts ONLY the latest version of each path
 * - Non-Hoodie dataset => then always accept
 *
 * We can set this filter, on a query engine's Hadoop Config & if it respects path filters, then
 * you should be able to query both hoodie & non-hoodie datasets as you would normally do.
 *
 * hadoopConf.setClass("mapreduce.input.pathFilter.class",
 *                      com.uber.hoodie.hadoop.HoodieROTablePathFilter.class,
 *                      org.apache.hadoop.fs.PathFilter.class)
 *
 */
public class HoodieROTablePathFilter implements PathFilter, Serializable {

    public static final Log LOG = LogFactory.getLog(HoodieInputFormat.class);

    /**
     * Its quite common, to have all files from a given partition path be passed into accept(),
     * cache the check for hoodie metadata for known partition paths & the latest versions of files
     */
    private HashMap<String, HashSet<Path>> hoodiePathCache;

    /**
     * Paths that are known to be non-hoodie datasets.
     */
    private HashSet<String> nonHoodiePathCache;


    public HoodieROTablePathFilter() {
        hoodiePathCache = new HashMap<>();
        nonHoodiePathCache = new HashSet<>();
    }

    /**
     * Obtain the path, two levels from provided path
     *
     * @return said path if available, null otherwise
     */
    private Path safeGetParentsParent(Path path) {
        if (path.getParent() != null && path.getParent().getParent() != null && path.getParent().getParent().getParent() != null) {
            return path.getParent().getParent().getParent();
        }
        return null;
    }


    @Override
    public boolean accept(Path path) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking acceptance for path " + path);
        }
        Path folder = null;
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            if (fs.isDirectory(path)) {
                return true;
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
                    LOG.debug(String.format("%s Hoodie path checked against cache, accept => %s \n",
                            path,
                            hoodiePathCache.get(folder.toString()).contains(path)));
                }
                return hoodiePathCache.get(folder.toString()).contains(path);
            }

            // Perform actual checking.
            Path baseDir;
            if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
                HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
                metadata.readFromFS();
                baseDir = HoodieHiveUtil.getNthParent(folder, metadata.getPartitionDepth());
            } else  {
                baseDir = safeGetParentsParent(folder);
            }

            if (baseDir != null) {
                try {
                    HoodieTableMetadata metadata = new HoodieTableMetadata(fs, baseDir.toString());
                    FileStatus[] latestFiles = metadata.getLatestVersions(fs.listStatus(folder));
                    // populate the cache
                    if (!hoodiePathCache.containsKey(folder.toString())) {
                        hoodiePathCache.put(folder.toString(), new HashSet<Path>());
                    }
                    LOG.info("Based on hoodie metadata from base path: " + baseDir.toString() +
                            ", caching " + latestFiles.length+" files under "+ folder);
                    for (FileStatus lfile: latestFiles) {
                        hoodiePathCache.get(folder.toString()).add(lfile.getPath());
                    }

                    // accept the path, if its among the latest files.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s checked after cache population, accept => %s \n",
                                path,
                                hoodiePathCache.get(folder.toString()).contains(path)));
                    }
                    return hoodiePathCache.get(folder.toString()).contains(path);
                } catch (InvalidDatasetException e) {
                   // Non-hoodie path, accept it.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("(1) Caching non-hoodie path under %s \n",
                                folder.toString()));
                    }
                    nonHoodiePathCache.add(folder.toString());
                    return true;
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
            String msg = "Error checking path :" + path +", under folder: "+ folder;
            LOG.error(msg, e);
            throw new HoodieException(msg, e);
        }
    }
}
