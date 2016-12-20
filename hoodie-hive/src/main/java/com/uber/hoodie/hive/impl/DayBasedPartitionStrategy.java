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

package com.uber.hoodie.hive.impl;

import com.uber.hoodie.hive.PartitionStrategy;
import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple day based partitions.
 * Storage is of this format yyyy/mm/dd
 * Table is partitioned by dateStringFieldName=MM/dd/yyyy
 */
public class DayBasedPartitionStrategy implements PartitionStrategy {
    private Logger LOG = LoggerFactory.getLogger(DayBasedPartitionStrategy.class);
    private final String dateStringFieldName;
    private final DateTimeFormatter dtfOut;

    public DayBasedPartitionStrategy() {
        this.dateStringFieldName = "datestr";
        this.dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd");
    }

    @Override public FileStatus[] scanAllPartitions(HoodieDatasetReference ref, HoodieFSClient fsClient) {
        return fsClient.getDirectoriesMatchingPattern(ref, "/*/*/*");
    }

    @Override public String[] getHivePartitionFieldNames() {
        return new String[] {dateStringFieldName};
    }

    @Override
    public String[] convertPartitionToValues(HoodieDatasetReference metadata, Path partition) {
        //yyyy/mm/dd
        String basePath = metadata.getBaseDatasetPath();
        String partitionPath = partition.toUri().getPath();
        if (!partitionPath.contains(basePath)) {
            throw new IllegalArgumentException(
                "Partition path " + partitionPath + " is not part of the dataset " + metadata);
        }
        // Get the partition part and remove the / as well at the end
        String partitionPart = partitionPath.substring(basePath.length() + 1);
        LOG.info("Extracting parts from " + partitionPart);
        int year = extractPart(partitionPart, 0);
        int mm = extractPart(partitionPart, 1);
        int dd = extractPart(partitionPart, 2);
        DateTime dateTime = new DateTime(year, mm, dd, 0, 0);
        return new String[] {dtfOut.print(dateTime)};
    }

    private int extractPart(String pathString, int index) {
        String[] parts = pathString.split("/");
        String part = parts[index];
        return Integer.parseInt(part);
    }

}
