/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.hive.client;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.hoodie.hive.HoodieHiveConfiguration;
import com.uber.hoodie.hive.HoodieHiveDatasetException;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.model.StoragePartition;
import com.uber.hoodie.hive.model.TablePartition;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client to access HDFS
 */
public class HoodieFSClient {
    final public static String PARQUET_EXTENSION = ".parquet";
    final public static String PARQUET_EXTENSION_ZIPPED = ".parquet.gz";
    private final static Logger LOG = LoggerFactory.getLogger(HoodieFSClient.class);
    private final HoodieHiveConfiguration conf;
    private final FileSystem fs;

    public HoodieFSClient(HoodieHiveConfiguration configuration) {
        this.conf = configuration;
        try {
            this.fs = FileSystem.get(configuration.getConfiguration());
        } catch (IOException e) {
            throw new HoodieHiveDatasetException(
                "Could not initialize file system from configuration", e);
        }
    }

    /**
     * Read the parquet schema from a parquet File
     *
     * @param parquetFilePath
     * @return
     * @throws IOException
     */
    public MessageType readSchemaFromDataFile(Path parquetFilePath) throws IOException {
        LOG.info("Reading schema from " + parquetFilePath);

        if (!fs.exists(parquetFilePath)) {
            throw new IllegalArgumentException(
                "Failed to read schema from data file " + parquetFilePath
                    + ". File does not exist.");
        }
        ParquetMetadata fileFooter =
            ParquetFileReader.readFooter(conf.getConfiguration(), parquetFilePath);
        return fileFooter.getFileMetaData().getSchema();
    }

    /**
     * Find the last data file under the partition path.
     *
     * @param metadata
     * @param partitionPathString
     * @return
     */
    public Path lastDataFileForDataset(HoodieDatasetReference metadata,
        String partitionPathString) {
        try {
            Path partitionPath = new Path(partitionPathString);
            if (!fs.exists(partitionPath)) {
                throw new HoodieHiveDatasetException(
                    "Partition path " + partitionPath + " not found in Dataset " + metadata);
            }

            RemoteIterator<LocatedFileStatus> files = fs.listFiles(partitionPath, true);
            // Iterate over the list. List is generally is listed in chronological order becasue of the date partitions
            // Get the latest schema
            Path returnPath = null;
            while (files.hasNext()) {
                Path path = files.next().getPath();
                if (path.getName().endsWith(PARQUET_EXTENSION) || path.getName()
                    .endsWith(PARQUET_EXTENSION_ZIPPED)) {
                    if(returnPath == null || path.toString().compareTo(returnPath.toString()) > 0) {
                        returnPath = path;
                    }
                }
            }
            if (returnPath != null) {
                return returnPath;
            }
            throw new HoodieHiveDatasetException(
                "No data file found in path " + partitionPath + " for dataset " + metadata);
        } catch (IOException e) {
            throw new HoodieHiveDatasetException(
                "Failed to get data file in path " + partitionPathString + " for dataset "
                    + metadata, e);
        }
    }

    /**
     * Finds all the files/directories that match the pattern under the {@link HoodieDatasetReference} basePath
     *
     * @param metadata
     * @param pattern
     * @return
     */
    public FileStatus[] getDirectoriesMatchingPattern(HoodieDatasetReference metadata, String pattern) {
        try {
            Path path = new Path(metadata.getBaseDatasetPath() + pattern);
            FileStatus[] status = fs.globStatus(path);
            List<FileStatus> returns = Lists.newArrayList();
            for(FileStatus st:status) {
                if(!st.getPath().toString().contains(".distcp")) {
                    // Ignore temporary directories created by distcp
                    returns.add(st);
                }
            }
            return returns.toArray(new FileStatus[returns.size()]);
        } catch (IOException e) {
            throw new HoodieHiveDatasetException(
                "IOException when reading directories under dataset " + metadata + " with pattern "
                    + pattern, e);
        }
    }

    /**
     * Get the list of storage partitions which does not have its equivalent hive partitions
     *
     * @param tablePartitions
     * @param storagePartitions
     * @return
     */
    public List<StoragePartition> getUnregisteredStoragePartitions(
        List<TablePartition> tablePartitions, List<StoragePartition> storagePartitions) {
        Set<String> paths = Sets.newHashSet();
        for (TablePartition tablePartition : tablePartitions) {
            paths.add(tablePartition.getLocation().toUri().getPath());
        }
        List<StoragePartition> missing = Lists.newArrayList();
        for (StoragePartition storagePartition : storagePartitions) {
            String hdfsPath = storagePartition.getPartitionPath().toUri().getPath();
            if (!paths.contains(hdfsPath)) {
                missing.add(storagePartition);
            }
        }
        return missing;
    }

    /**
     * Get the list of storage partitions which does not have its equivalent hive partitions
     *
     * @param tablePartitions
     * @param storagePartitions
     * @return
     */
    public List<StoragePartition> getChangedStoragePartitions(
        List<TablePartition> tablePartitions, List<StoragePartition> storagePartitions) {
        Map<String, String> paths = Maps.newHashMap();
        for (TablePartition tablePartition : tablePartitions) {
            String[] partitionKeyValueStr = tablePartition.getPartitionFieldValues();
            Arrays.sort(partitionKeyValueStr);
            paths.put(Arrays.toString(partitionKeyValueStr), tablePartition.getLocation().toUri().getPath());
        }

        List<StoragePartition> changed = Lists.newArrayList();
        for (StoragePartition storagePartition : storagePartitions) {
            String[] partitionKeyValues = storagePartition.getPartitionFieldValues();
            Arrays.sort(partitionKeyValues);
            String partitionKeyValueStr = Arrays.toString(partitionKeyValues);
            String hdfsPath = storagePartition.getPartitionPath().toUri().getPath();
            if (paths.containsKey(partitionKeyValueStr) && !paths.get(partitionKeyValueStr).equals(hdfsPath)) {
                changed.add(storagePartition);
            }
        }
        return changed;
    }

    public int calculateStorageHash(FileStatus[] paths) {
        return Objects.hashCode(paths);
    }

}
