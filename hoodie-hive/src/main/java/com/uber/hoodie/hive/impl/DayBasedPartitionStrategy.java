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

import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.hive.HoodieHiveDatasetException;
import com.uber.hoodie.hive.PartitionStrategy;
import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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

    @Override public List<String> scanAllPartitions(HoodieDatasetReference ref, HoodieFSClient fsClient) {
        try {
            return FSUtils.getAllPartitionPaths(fsClient.getFs(), ref.getBaseDatasetPath(), true);
        } catch (IOException ioe) {
            throw new HoodieHiveDatasetException(
                    "IOException when listing partitions under dataset " + ref , ioe);
        }
    }

    @Override public String[] getHivePartitionFieldNames() {
        return new String[] {dateStringFieldName};
    }

    @Override
    public String[] convertPartitionToValues(HoodieDatasetReference metadata, String partitionPath) {
        //yyyy/mm/dd
        String[] splits = partitionPath.split("/");
        if (splits.length != 3) {
            throw new IllegalArgumentException(
                    "Partition path " + partitionPath + " is not in the form yyyy/mm/dd ");
        }
        // Get the partition part and remove the / as well at the end
        int year = Integer.parseInt(splits[0]);
        int mm = Integer.parseInt(splits[1]);
        int dd = Integer.parseInt(splits[2]);
        DateTime dateTime = new DateTime(year, mm, dd, 0, 0);
        return new String[] {dtfOut.print(dateTime)};
    }
}
