/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All the metadata that gets stored along with a commit.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieCommitMetadata implements Serializable {
    private static volatile Logger log = LogManager.getLogger(HoodieCommitMetadata.class);
    protected Map<String, List<HoodieWriteStat>> partitionToWriteStats;

    private Map<String, String> extraMetadataMap;

    public HoodieCommitMetadata() {
        extraMetadataMap = new HashMap<>();
        partitionToWriteStats = new HashMap<>();
    }

    public void addWriteStat(String partitionPath, HoodieWriteStat stat) {
        if (!partitionToWriteStats.containsKey(partitionPath)) {
            partitionToWriteStats.put(partitionPath, new ArrayList<>());
        }
        partitionToWriteStats.get(partitionPath).add(stat);
    }

    public void addMetadata(String metaKey, String value) {
        extraMetadataMap.put(metaKey, value);
    }

    public List<HoodieWriteStat> getWriteStats(String partitionPath) {
        return partitionToWriteStats.get(partitionPath);
    }

    public Map<String, String> getExtraMetadata() { return extraMetadataMap; }

    public Map<String, List<HoodieWriteStat>> getPartitionToWriteStats() {
        return partitionToWriteStats;
    }

    public String getMetadata(String metaKey) {
        return extraMetadataMap.get(metaKey);
    }

    public HashMap<String, String> getFileIdAndRelativePaths() {
        HashMap<String, String> filePaths = new HashMap<>();
        // list all partitions paths
        for (Map.Entry<String, List<HoodieWriteStat>> entry: getPartitionToWriteStats().entrySet()) {
            for (HoodieWriteStat stat: entry.getValue()) {
                filePaths.put(stat.getFileId(), stat.getPath());
            }
        }
        return filePaths;
    }

    public HashMap<String, String> getFileIdAndFullPaths(String basePath) {
        HashMap<String, String> fullPaths = new HashMap<>();
        for (Map.Entry<String, String> entry: getFileIdAndRelativePaths().entrySet()) {
            String fullPath = (entry.getValue() != null) ? (new Path(basePath, entry.getValue())).toString() : null;
            fullPaths.put(entry.getKey(), fullPath);
        } return fullPaths;
    }

    public String toJsonString() throws IOException {
        if(partitionToWriteStats.containsKey(null)) {
            log.info("partition path is null for " + partitionToWriteStats.get(null));
            partitionToWriteStats.remove(null);
        }
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
        return mapper.defaultPrettyPrintingWriter().writeValueAsString(this);
    }

    public static HoodieCommitMetadata fromJsonString(String jsonStr) throws IOException {
        if (jsonStr == null || jsonStr.isEmpty()) {
            // For empty commit file (no data or somethings bad happen).
            return new HoodieCommitMetadata();
        }
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
        return mapper.readValue(jsonStr, HoodieCommitMetadata.class);
    }

    // Here the functions are named "fetch" instead of "get", to get avoid of the json conversion.
    public long fetchTotalPartitionsWritten() {
        return partitionToWriteStats.size();
    }

    public long fetchTotalFilesInsert() {
        long totalFilesInsert = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                if (stat.getPrevCommit() != null && stat.getPrevCommit().equals("null")) {
                    totalFilesInsert ++;
                }
            }
        }
        return totalFilesInsert;
    }

    public long fetchTotalFilesUpdated() {
        long totalFilesUpdated = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                if (stat.getPrevCommit() != null && !stat.getPrevCommit().equals("null")) {
                    totalFilesUpdated ++;
                }
            }
        }
        return totalFilesUpdated;
    }

    public long fetchTotalUpdateRecordsWritten() {
        long totalUpdateRecordsWritten = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                totalUpdateRecordsWritten += stat.getNumUpdateWrites();
            }
        }
        return totalUpdateRecordsWritten;
    }

    public long fetchTotalInsertRecordsWritten() {
        long totalInsertRecordsWritten = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                if (stat.getPrevCommit() != null && stat.getPrevCommit().equals("null")) {
                    totalInsertRecordsWritten += stat.getNumWrites();
                }
            }
        }
        return totalInsertRecordsWritten;
    }

    public long fetchTotalRecordsWritten() {
        long totalRecordsWritten = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                totalRecordsWritten += stat.getNumWrites();
            }
        }
        return totalRecordsWritten;
    }

    public long fetchTotalBytesWritten() {
        long totalBytesWritten = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                totalBytesWritten += stat.getTotalWriteBytes();
            }
        }
        return totalBytesWritten;
    }

    public long fetchTotalWriteErrors() {
        long totalWriteErrors = 0;
        for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
            for (HoodieWriteStat stat : stats) {
                totalWriteErrors += stat.getTotalWriteErrors();
            }
        }
        return totalWriteErrors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HoodieCommitMetadata that = (HoodieCommitMetadata) o;

        return partitionToWriteStats != null ?
            partitionToWriteStats.equals(that.partitionToWriteStats) :
            that.partitionToWriteStats == null;

    }

    @Override
    public int hashCode() {
        return partitionToWriteStats != null ? partitionToWriteStats.hashCode() : 0;
    }

    public static HoodieCommitMetadata fromBytes(byte[] bytes) throws IOException {
        return fromJsonString(new String(bytes, Charset.forName("utf-8")));
    }
}
