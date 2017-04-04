/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table;

import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.DatasetNotFoundException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * <code>HoodieTableMetaClient</code> allows to access meta-data about a hoodie table
 * It returns meta-data about commits, savepoints, compactions, cleanups as a <code>HoodieTimeline</code>
 * Create an instance of the <code>HoodieTableMetaClient</code> with FileSystem and basePath to start getting the meta-data.
 * <p>
 * All the timelines are computed lazily, once computed the timeline is cached and never refreshed.
 * Use the <code>HoodieTimeline.reload()</code> to refresh timelines.
 *
 * @see HoodieTimeline
 * @since 0.3.0
 */
public class HoodieTableMetaClient implements Serializable {
    private final transient static Logger log = LogManager.getLogger(HoodieTableMetaClient.class);
    public static String METAFOLDER_NAME = ".hoodie";

    private String basePath;
    private transient FileSystem fs;
    private String metaPath;
    private HoodieTableType tableType;
    private HoodieTableConfig tableConfig;
    private HoodieActiveTimeline activeTimeline;
    private HoodieArchivedTimeline archivedTimeline;

    public HoodieTableMetaClient(FileSystem fs, String basePath) throws DatasetNotFoundException {
        // Do not load any timeline by default
        this(fs, basePath, false);
    }

    public HoodieTableMetaClient(FileSystem fs, String basePath, boolean loadActiveTimelineOnLoad)
        throws DatasetNotFoundException {
        log.info("Loading HoodieTableMetaClient from " + basePath);
        this.basePath = basePath;
        this.fs = fs;
        Path basePathDir = new Path(this.basePath);
        this.metaPath = basePath + File.separator + METAFOLDER_NAME;
        Path metaPathDir = new Path(this.metaPath);
        DatasetNotFoundException.checkValidDataset(fs, basePathDir, metaPathDir);
        this.tableConfig = new HoodieTableConfig(fs, metaPath);
        this.tableType = tableConfig.getTableType();
        log.info("Finished Loading Table of type " + tableType + " from " + basePath);
        if (loadActiveTimelineOnLoad) {
            log.info("Loading Active commit timeline for " + basePath);
            getActiveTimeline();
        }
    }

    /**
     * For serailizing and de-serializing
     *
     * @deprecated
     */
    public HoodieTableMetaClient() {
    }

    /**
     * This method is only used when this object is deserialized in a spark executor.
     *
     * @deprecated
     */
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.fs = FSUtils.getFs();
    }

    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException {
        out.defaultWriteObject();
    }

    /**
     * @return Base path
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * @return Hoodie Table Type
     */
    public HoodieTableType getTableType() {
        return tableType;
    }

    /**
     * @return Meta path
     */
    public String getMetaPath() {
        return metaPath;
    }

    /**
     * @return Table Config
     */
    public HoodieTableConfig getTableConfig() {
        return tableConfig;
    }

    /**
     * Get the FS implementation for this table
     * @return
     */
    public FileSystem getFs() {
        return fs;
    }

    /**
     * Get the active instants as a timeline
     *
     * @return Active instants timeline
     * @throws IOException
     */
    public synchronized HoodieActiveTimeline getActiveTimeline() {
        if (activeTimeline == null) {
            activeTimeline = new HoodieActiveTimeline(fs, metaPath);
        }
        return activeTimeline;
    }

    /**
     * Get the archived commits as a timeline. This is costly operation, as all data from the
     * archived files are read. This should not be used, unless for historical debugging purposes
     *
     * @return Active commit timeline
     * @throws IOException
     */
    public synchronized HoodieArchivedTimeline getArchivedTimeline() {
        if (archivedTimeline == null) {
            archivedTimeline = new HoodieArchivedTimeline(fs, metaPath);
        }
        return archivedTimeline;
    }

    /**
     * Helper method to initialize a given path, as a given storage type and table name
     *
     * @param fs
     * @param basePath
     * @param tableType
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HoodieTableMetaClient initTableType(FileSystem fs, String basePath, HoodieTableType tableType, String tableName) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
        return HoodieTableMetaClient.initializePathAsHoodieDataset(fs, basePath, properties);
    }

    /**
     * Helper method to initialize a given path as a hoodie dataset with configs passed in as as Properties
     *
     * @param fs
     * @param basePath
     * @param props
     * @return Instance of HoodieTableMetaClient
     * @throws IOException
     */
    public static HoodieTableMetaClient initializePathAsHoodieDataset(FileSystem fs,
        String basePath, Properties props) throws IOException {
        log.info("Initializing " + basePath + " as hoodie dataset " + basePath);
        Path basePathDir = new Path(basePath);
        if (!fs.exists(basePathDir)) {
            fs.mkdirs(basePathDir);
        }
        Path metaPathDir = new Path(basePath, METAFOLDER_NAME);
        if (!fs.exists(metaPathDir)) {
            fs.mkdirs(metaPathDir);
        }
        HoodieTableConfig.createHoodieProperties(fs, metaPathDir, props);
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        log.info("Finished initializing Table of type " + metaClient.getTableConfig().getTableType()
            + " from " + basePath);
        return metaClient;
    }

    // HELPER METHODS TO CREATE META FILE NAMES
    public static FileStatus[] scanFiles(FileSystem fs, Path metaPath, PathFilter nameFilter)
        throws IOException {
        return fs.listStatus(metaPath, nameFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        HoodieTableMetaClient that = (HoodieTableMetaClient) o;
        return Objects.equals(basePath, that.basePath) && tableType == that.tableType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(basePath, tableType);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieTableMetaClient{");
        sb.append("basePath='").append(basePath).append('\'');
        sb.append(", metaPath='").append(metaPath).append('\'');
        sb.append(", tableType=").append(tableType);
        sb.append('}');
        return sb.toString();
    }
}
