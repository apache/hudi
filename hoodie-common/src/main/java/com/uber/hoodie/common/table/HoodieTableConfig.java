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

import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieFileFormat;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

/**
 * Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc
 * Configurations are loaded from hoodie.properties, these properties are usually set during initializing a path as hoodie base path
 * and never changes during the lifetime of a hoodie dataset.
 *
 * @see HoodieTableMetaClient
 * @since 0.3.0
 */
public class HoodieTableConfig implements Serializable {
    private final transient static Logger log = LogManager.getLogger(HoodieTableConfig.class);

    public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
    public static final String HOODIE_TABLE_NAME_PROP_NAME = "hoodie.table.name";
    public static final String HOODIE_TABLE_TYPE_PROP_NAME = "hoodie.table.type";
    public static final String HOODIE_RO_FILE_FORMAT_PROP_NAME =
        "hoodie.table.ro.file.format";
    public static final String HOODIE_RT_FILE_FORMAT_PROP_NAME =
        "hoodie.table.rt.file.format";
    public static final String HOODIE_PAYLOAD_CLASS_PROP_NAME = "hoodie.compaction.payload.class";

    public static final HoodieTableType DEFAULT_TABLE_TYPE = HoodieTableType.COPY_ON_WRITE;
    public static final HoodieFileFormat DEFAULT_RO_FILE_FORMAT = HoodieFileFormat.PARQUET;
    public static final HoodieFileFormat DEFAULT_RT_FILE_FORMAT = HoodieFileFormat.HOODIE_LOG;
    public static final String DEFAULT_PAYLOAD_CLASS = HoodieAvroPayload.class.getName();
    private Properties props;

    public HoodieTableConfig(FileSystem fs, String metaPath) {
        Properties props = new Properties();
        Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
        log.info("Loading dataset properties from " + propertyPath);
        try {
            try (FSDataInputStream inputStream = fs.open(propertyPath)) {
                props.load(inputStream);
            }
        } catch (IOException e) {
            throw new HoodieIOException("Could not load Hoodie properties from " + propertyPath, e);
        }
        this.props = props;
    }

    /**
     * For serailizing and de-serializing
     * @deprecated
     */
    public HoodieTableConfig() {
    }

    /**
     * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties)
     *
     * @param metadataFolder
     * @param properties
     * @throws IOException
     */
    public static void createHoodieProperties(FileSystem fs, Path metadataFolder,
        Properties properties) throws IOException {
        if (!fs.exists(metadataFolder)) {
            fs.mkdirs(metadataFolder);
        }
        Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
        FSDataOutputStream outputStream = fs.create(propertyPath);
        try {
            if (!properties.containsKey(HOODIE_TABLE_NAME_PROP_NAME)) {
                throw new IllegalArgumentException(
                    HOODIE_TABLE_NAME_PROP_NAME + " property needs to be specified");
            }
            if (!properties.containsKey(HOODIE_TABLE_TYPE_PROP_NAME)) {
                properties.setProperty(HOODIE_TABLE_TYPE_PROP_NAME, DEFAULT_TABLE_TYPE.name());
            }
            if (properties.getProperty(HOODIE_TABLE_TYPE_PROP_NAME) == HoodieTableType.MERGE_ON_READ.name()
                && !properties.containsKey(HOODIE_PAYLOAD_CLASS_PROP_NAME)) {
                    properties.setProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME, DEFAULT_PAYLOAD_CLASS);
            }
            properties
                .store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        } finally {
            outputStream.close();
        }
    }


    /**
     * Read the table type from the table properties and if not found, return the default
     *
     * @return
     */
    public HoodieTableType getTableType() {
        if (props.containsKey(HOODIE_TABLE_TYPE_PROP_NAME)) {
            return HoodieTableType.valueOf(props.getProperty(HOODIE_TABLE_TYPE_PROP_NAME));
        }
        return DEFAULT_TABLE_TYPE;
    }

    /**
     * Read the payload class for HoodieRecords from the table properties
     *
     * @return
     */
    public String getPayloadClass() {
        return props.getProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME, DEFAULT_PAYLOAD_CLASS);
    }

    /**
     * Read the table name
     *
     * @return
     */
    public String getTableName() {
        return props.getProperty(HOODIE_TABLE_NAME_PROP_NAME);
    }

    /**
     * Get the Read Optimized Storage Format
     *
     * @return HoodieFileFormat for the Read Optimized Storage format
     */
    public HoodieFileFormat getROFileFormat() {
        if (props.containsKey(HOODIE_RO_FILE_FORMAT_PROP_NAME)) {
            return HoodieFileFormat.valueOf(props.getProperty(HOODIE_RO_FILE_FORMAT_PROP_NAME));
        }
        return DEFAULT_RO_FILE_FORMAT;
    }

    /**
     * Get the Read Optimized Storage Format
     *
     * @return HoodieFileFormat for the Read Optimized Storage format
     */
    public HoodieFileFormat getRTFileFormat() {
        if (props.containsKey(HOODIE_RT_FILE_FORMAT_PROP_NAME)) {
            return HoodieFileFormat.valueOf(props.getProperty(HOODIE_RT_FILE_FORMAT_PROP_NAME));
        }
        return DEFAULT_RT_FILE_FORMAT;
    }

}
