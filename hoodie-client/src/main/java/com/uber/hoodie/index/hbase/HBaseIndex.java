/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieDependentSystemUnavailableException;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * Hoodie Index implementation backed by HBase
 */
public class HBaseIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
  private static final byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
  private static final byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
  private static final byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");

  private static Logger logger = LogManager.getLogger(HBaseIndex.class);
  private static Connection hbaseConnection = null;
  private final String tableName;

  public HBaseIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getHbaseTableName();
    addShutDownHook();
  }

  @Override
  public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
    //TODO : Change/Remove filterExists in HoodieReadClient() and revisit
    throw new UnsupportedOperationException("HBase index does not implement check exist");
  }

  private Connection getHBaseConnection() {
    Configuration hbaseConfig = HBaseConfiguration.create();
    String quorum = config.getHbaseZkQuorum();
    hbaseConfig.set("hbase.zookeeper.quorum", quorum);
    String port = String.valueOf(config.getHbaseZkPort());
    hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
    try {
      return ConnectionFactory.createConnection(hbaseConfig);
    } catch (IOException e) {
      throw new HoodieDependentSystemUnavailableException(
          HoodieDependentSystemUnavailableException.HBASE, quorum + ":" + port);
    }
  }

  /**
   * Since we are sharing the HbaseConnection across tasks in a JVM, make sure the HbaseConnectio is
   * closed when JVM exits
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          hbaseConnection.close();
        } catch (Exception e) {
          // fail silently for any sort of exception
        }
      }
    });
  }

  private Get generateStatement(String key) throws IOException {
    return new Get(Bytes.toBytes(key)).setMaxVersions(1)
        .addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
        .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN)
        .addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN);
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty() && (commitTimeline
        .containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs))
        || HoodieTimeline
        .compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(), commitTs,
            HoodieTimeline.GREATER));
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>,
      Iterator<HoodieRecord<T>>> locationTagFunction(HoodieTableMetaClient metaClient) {

    return (Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>>)
        (partitionNum, hoodieRecordIterator) -> {

          Integer multiGetBatchSize = config.getHbaseIndexGetBatchSize();

          // Grab the global HBase connection
          synchronized (HBaseIndex.class) {
            if (hbaseConnection == null || hbaseConnection.isClosed()) {
              hbaseConnection = getHBaseConnection();
            }
          }
          List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
          HTable hTable = null;
          try {
            hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
            List<Get> statements = new ArrayList<>();
            List<HoodieRecord> currentBatchOfRecords = new LinkedList<>();
            // Do the tagging.
            while (hoodieRecordIterator.hasNext()) {
              HoodieRecord rec = hoodieRecordIterator.next();
              statements.add(generateStatement(rec.getRecordKey()));
              currentBatchOfRecords.add(rec);
              // iterator till we reach batch size
              if (statements.size() >= multiGetBatchSize || !hoodieRecordIterator.hasNext()) {
                // get results for batch from Hbase
                Result[] results = hTable.get(statements);
                // clear statements to be GC'd
                statements.clear();
                for (Result result : results) {
                  // first, attempt to grab location from HBase
                  HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
                  if (result.getRow() != null) {
                    String keyFromResult = Bytes.toString(result.getRow());
                    String commitTs = Bytes
                        .toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                    String fileId = Bytes
                        .toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
                    String partitionPath = Bytes
                        .toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                    if (checkIfValidCommit(metaClient, commitTs)) {
                      currentRecord = new HoodieRecord(
                          new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                          currentRecord.getData());
                      currentRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                      taggedRecords.add(currentRecord);
                      // the key from Result and the key being processed should be same
                      assert (currentRecord.getRecordKey().contentEquals(keyFromResult));
                    } else { //if commit is invalid, treat this as a new taggedRecord
                      taggedRecords.add(currentRecord);
                    }
                  } else {
                    taggedRecords.add(currentRecord);
                  }
                }
              }
            }
          } catch (IOException e) {
            throw new HoodieIndexException(
                "Failed to Tag indexed locations because of exception with HBase Client", e);
          } finally {
            if (hTable != null) {
              try {
                hTable.close();
              } catch (IOException e) {
                // Ignore
              }
            }

          }
          return taggedRecords.iterator();
        };
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    return recordRDD.mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true);
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {
    return (Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>>) (partition,
        statusIterator) -> {
      Integer multiPutBatchSize = config.getHbaseIndexPutBatchSize();

      List<WriteStatus> writeStatusList = new ArrayList<>();
      // Grab the global HBase connection
      synchronized (HBaseIndex.class) {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
          hbaseConnection = getHBaseConnection();
        }
      }
      HTable hTable = null;
      try {
        hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
        while (statusIterator.hasNext()) {
          WriteStatus writeStatus = statusIterator.next();
          List<Put> puts = new ArrayList<>();
          List<Delete> deletes = new ArrayList<>();
          try {
            for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
              if (!writeStatus.isErrored(rec.getKey())) {
                java.util.Optional<HoodieRecordLocation> loc = rec.getNewLocation();
                if (loc.isPresent()) {
                  if (rec.getCurrentLocation() != null) {
                    // This is an update, no need to update index
                    continue;
                  }
                  Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN,
                      Bytes.toBytes(loc.get().getCommitTime()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN,
                      Bytes.toBytes(loc.get().getFileId()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN,
                      Bytes.toBytes(rec.getPartitionPath()));
                  puts.add(put);
                } else {
                  //Delete existing index for a deleted record
                  Delete delete = new Delete(Bytes.toBytes(rec.getRecordKey()));
                  deletes.add(delete);
                }
              }
              if (puts.size() + deletes.size() < multiPutBatchSize) {
                continue;
              }
              doPutsAndDeletes(hTable, puts, deletes);
            }
            //process remaining puts and deletes, if any
            doPutsAndDeletes(hTable, puts, deletes);
          } catch (Exception e) {
            Exception we = new Exception("Error updating index for " + writeStatus, e);
            logger.error(we);
            writeStatus.setGlobalError(we);
          }
          writeStatusList.add(writeStatus);
        }
      } catch (IOException e) {
        throw new HoodieIndexException(
            "Failed to Update Index locations because of exception with HBase Client", e);
      } finally {
        if (hTable != null) {
          try {
            hTable.close();
          } catch (IOException e) {
            // Ignore
          }
        }
      }
      return writeStatusList.iterator();
    };
  }

  /**
   * Helper method to facilitate performing puts and deletes in Hbase
   */
  private void doPutsAndDeletes(HTable hTable, List<Put> puts, List<Delete> deletes)
      throws IOException {
    if (puts.size() > 0) {
      hTable.put(puts);
    }
    if (deletes.size() > 0) {
      hTable.delete(deletes);
    }
    hTable.flushCommits();
    puts.clear();
    deletes.clear();
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {
    return writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Rollback in HbaseIndex is managed via method {@link #checkIfValidCommit()}
    return true;
  }

  /**
   * Only looks up by recordKey
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  @VisibleForTesting
  public void setHbaseConnection(Connection hbaseConnection) {
    HBaseIndex.hbaseConnection = hbaseConnection;
  }
}