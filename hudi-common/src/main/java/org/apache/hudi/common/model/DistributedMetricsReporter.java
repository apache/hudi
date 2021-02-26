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

package org.apache.hudi.common.model;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * Observability related metrics collection operations.
 */
public class DistributedMetricsReporter implements Serializable {
  public static long ONE_MB = 1024 * 1024;
  public static long USEC_PER_SEC = 1000 * 1000;

  public static final String DISTRIBUTED_METRICS_REGISTRY_NAME = "DistributedMetrics";
  public static final String TABLE_NAME = "tableName";
  public static final String STAGE_ID = "stageId";
  public static final String PARTITION_ID = "partitionId";
  public static final String IO_TYPE = "IoType";
  public static final String HOST_NAME = "hostName";
  private HashMap<String, Object> props = new HashMap<>();
  public DistributedMetricsReporter(Option<Registry> registry, String tableName, long stageId, long partitionId) {
    props.put(DISTRIBUTED_METRICS_REGISTRY_NAME, registry);
    props.put(TABLE_NAME, tableName);
    props.put(STAGE_ID, stageId);
    props.put(PARTITION_ID, partitionId);
    props.put(HOST_NAME, getHostName());
  }

  private String getHostName() {
    String host = "Unknown";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // use "unknown"
    }
    return host;
  }

  public HashMap<String, Object> getPropertiesMap() {
    return props;
  }

  public void reportWriteMetrics() {
    WriteMetrics.reportWriteStats(props);
  }

  public void reportBloomIndexMetrics() {
    BloomIndexMetrics.reportBloomIndexStats(props);
  }

  public static class WriteMetrics {
    // define a unique metric name string for each metric to be collected.
    public static final String PARQUET_NORMALIZED_WRITE_TIME = "writeTimePerRecordInUSec";
    public static final String PARQUET_CUMULATIVE_WRITE_TIME_IN_USEC = "cumulativeParquetWriteTimeInUSec";
    public static final String PARQUET_WRITE_TIME_PER_MB_IN_USEC = "writeTimePerMBInUSec";
    public static final String PARQUET_WRITE_THROUGHPUT_MBPS = "writeThroughputMBps";
    public static final String TOTAL_RECORDS_WRITTEN = "totalRecordsWritten";
    public static final String TOTAL_BYTES_WRITTEN = "totalBytesWritten";

    private static String getWriteMetricKey(String metric, HashMap<String, Object> props) {
      return String.format("%s.%s.%s.%s.%d", props.get(TABLE_NAME), metric, props.get(IO_TYPE),
          props.get(HOST_NAME), props.get(PARTITION_ID));
    }

    private static String getConsolidatedWriteMetricKey(String metric, HashMap<String, Object> params) {
      return String.format("%s.consolidated.%s.%s", params.get(TABLE_NAME), metric, params.get(IO_TYPE));
    }

    public static void reportWriteStats(HashMap<String, Object> props) {

      Option<Registry> registry = (Option<Registry>) props.get(DISTRIBUTED_METRICS_REGISTRY_NAME);
      long cumulativeWriteTimeInUsec = (Long)props.get(PARQUET_CUMULATIVE_WRITE_TIME_IN_USEC);
      long totalRecs = (Long) props.get(TOTAL_RECORDS_WRITTEN);
      long writeTimePerRecordInUSec = cumulativeWriteTimeInUsec / Math.max(totalRecs, 1);
      long writeTimePerMBInUSec = (long) (cumulativeWriteTimeInUsec
          * ((double) ONE_MB / (double) Math.max((Long)props.get(TOTAL_BYTES_WRITTEN), 1)));
      long writeThroughputMBps = (USEC_PER_SEC / Math.max(writeTimePerMBInUSec, 1));

      if (registry.isPresent()) {
        // executor specific stats
        registry.get().add(getWriteMetricKey(PARQUET_NORMALIZED_WRITE_TIME, props), writeTimePerRecordInUSec);
        registry.get().add(getWriteMetricKey(PARQUET_CUMULATIVE_WRITE_TIME_IN_USEC, props), cumulativeWriteTimeInUsec);
        registry.get().add(getWriteMetricKey(TOTAL_RECORDS_WRITTEN, props), totalRecs);
        registry.get().add(getWriteMetricKey(PARQUET_WRITE_TIME_PER_MB_IN_USEC, props), writeTimePerMBInUSec);
        registry.get().add(getWriteMetricKey(PARQUET_WRITE_THROUGHPUT_MBPS,props), writeThroughputMBps);

        // stats consolidated from all executors
        registry.get().add(getConsolidatedWriteMetricKey(TOTAL_RECORDS_WRITTEN, props), totalRecs);
        registry.get().add(getConsolidatedWriteMetricKey(PARQUET_CUMULATIVE_WRITE_TIME_IN_USEC, props),
            cumulativeWriteTimeInUsec);
      }
    }
  }

  public static class BloomIndexMetrics {
    public static final String SEARCH_TIME_IN_USEC = "bloomIndexSearchTime";
    public static final String CANDIDATE_RECORDS = "bloomIndexCandidateRecords";
    public static final String FILE_RECORDS = "bloomIndexFileRecords";
    public static final String MATCHING_RECORDS = "bloomIndexMatchingRecords";
    public static final String HIT_RATIO_AGAINST_FILE_RECORDS = "bloomIndexHitRatioVsFileRecords";
    public static final String HIT_RATIO_AGAINST_CANDIDATE_RECORDS = "bloomIndexHitRatioVsCandidateRecords";
    public static final String CANDIDATE_TO_FILE_RECORDS_RATIO = "bloomIndexCandidatesToFileRecordsRatio";
    public static final String SEARCH_TIME_PER_MATCHING_RECORD = "bloomIndexSearchTimePerMatchingRecord";
    public static final String TIME_TAKEN_PER_INSPECTION = "bloomIndexTimeTakenPerInspection";

    private static String getBloomIndexMetricKey(String metric, HashMap<String, Object> props) {
      return String.format("%s.%s.%s.%d", props.get(TABLE_NAME), metric, props.get(HOST_NAME), props.get(PARTITION_ID));
    }

    private static String getConsolidatedBloomIndexMetricKey(String metric, HashMap<String, Object> props) {
      return String.format("%s.consolidated.%s", props.get(TABLE_NAME), metric);
    }

    // inputs:
    // searchTimeInUSecs (total time taken for the search)
    // candidateRecords (needles being searched)
    // fileRecords  (haystack in consideration)
    // matchingRecords (needles found)
    //
    // metrics:
    // hitRatioAgainstFileRecords  = matchingRecords / fileRecords  (needles found to haystack ratio)
    // hitRatioAgainstCandidateRecords = matchingRecords / candidateRecords (percentage needles found)
    // candidateToFileRatio = candidateRecords / FileRecords (needles to haystack ratio)
    // numInspections  (number of needle/hay items inspected)
    // perMatchingRecordSearchTime = SearchTimeInUsec / matchingRecords (search time per needle found)
    // timeTakenPerInspection = SearchTimeInUsec / numInspections

    public static void reportBloomIndexStats(HashMap<String, Object> props) {

      Option<Registry> registry = (Option<Registry>) props.get(DISTRIBUTED_METRICS_REGISTRY_NAME);
      long cumulativeSearchTimeInUsec = (Long)props.get(SEARCH_TIME_IN_USEC);
      long matchingRecords = (Long) props.get(MATCHING_RECORDS);
      long fileRecords = (Long) props.get(FILE_RECORDS);
      long candidateRecords = (Long) props.get(CANDIDATE_RECORDS);

      long searchTimePerMatchingRecord = cumulativeSearchTimeInUsec / Math.max(matchingRecords, 1);
      long timeTakenPerInspection = cumulativeSearchTimeInUsec / Math.max(fileRecords, 1);
      long candidateToFileRatio = candidateRecords / Math.max(fileRecords, 1);
      long hitRatioAgainstCandidateRecords = matchingRecords / Math.max(candidateRecords, 1);
      long hitRatioAgainstFileRecords = matchingRecords / Math.max(fileRecords, 1);

      if (registry.isPresent()) {
        // executor specific stats
        registry.get().add(getBloomIndexMetricKey(SEARCH_TIME_IN_USEC, props), cumulativeSearchTimeInUsec);
        registry.get().add(getBloomIndexMetricKey(CANDIDATE_RECORDS, props), candidateRecords);
        registry.get().add(getBloomIndexMetricKey(FILE_RECORDS, props), fileRecords);
        registry.get().add(getBloomIndexMetricKey(MATCHING_RECORDS, props), matchingRecords);
        registry.get().add(getBloomIndexMetricKey(HIT_RATIO_AGAINST_FILE_RECORDS, props), hitRatioAgainstFileRecords);
        registry.get().add(getBloomIndexMetricKey(HIT_RATIO_AGAINST_CANDIDATE_RECORDS, props), hitRatioAgainstCandidateRecords);
        registry.get().add(getBloomIndexMetricKey(CANDIDATE_TO_FILE_RECORDS_RATIO, props), candidateToFileRatio);
        registry.get().add(getBloomIndexMetricKey(SEARCH_TIME_PER_MATCHING_RECORD, props), searchTimePerMatchingRecord);
        registry.get().add(getBloomIndexMetricKey(TIME_TAKEN_PER_INSPECTION, props), timeTakenPerInspection);

        // stats consolidated from all executors
        registry.get().add(getConsolidatedBloomIndexMetricKey(FILE_RECORDS, props), fileRecords);
        registry.get().add(getConsolidatedBloomIndexMetricKey(SEARCH_TIME_IN_USEC, props), cumulativeSearchTimeInUsec);
      }
    }
  }
}
