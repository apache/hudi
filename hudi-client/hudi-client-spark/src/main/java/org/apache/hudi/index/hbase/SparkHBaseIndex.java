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

package org.apache.hudi.index.hbase;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkConfigUtils;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;

/**
 * Spark implement of HBase index.
 */
public class SparkHBaseIndex<T extends HoodieRecordPayload> extends BaseHoodieHBaseIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  private static final Logger LOG = LogManager.getLogger(SparkHBaseIndex.class);

  public SparkHBaseIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context, BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> baseHoodieTable) throws HoodieIndexException {

    return recordRDD.mapPartitionsWithIndex(locationTagFunction(baseHoodieTable.getMetaClient()), true);
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
      HoodieTableMetaClient metaClient) {

    return (Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>>) (partitionNum,
                                                                                       hoodieRecordIterator) -> {

      int multiGetBatchSize = config.getHbaseIndexGetBatchSize();

      // Grab the global HBase connection
      synchronized (SparkHBaseIndex.class) {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
          hbaseConnection = getHBaseConnection();
        }
      }
      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      try (HTable hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName))) {
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
            Result[] results = doGet(hTable, statements);
            // clear statements to be GC'd
            statements.clear();
            for (Result result : results) {
              // first, attempt to grab location from HBase
              HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
              if (result.getRow() != null) {
                String keyFromResult = Bytes.toString(result.getRow());
                String commitTs = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                String fileId = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
                String partitionPath = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                if (checkIfValidCommit(metaClient, commitTs)) {
                  currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                      currentRecord.getData());
                  currentRecord.unseal();
                  currentRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                  currentRecord.seal();
                  taggedRecords.add(currentRecord);
                  // the key from Result and the key being processed should be same
                  assert (currentRecord.getRecordKey().contentEquals(keyFromResult));
                } else { // if commit is invalid, treat this as a new taggedRecord
                  taggedRecords.add(currentRecord);
                }
              } else {
                taggedRecords.add(currentRecord);
              }
            }
          }
        }
      } catch (IOException e) {
        throw new HoodieIndexException("Failed to Tag indexed locations because of exception with HBase Client", e);
      }
      return taggedRecords.iterator();
    };
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context, BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> baseHoodieTable) throws HoodieIndexException {
    JavaSparkContext jsc =  HoodieSparkEngineContext.getSparkContext(context);
    final HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = createQPSResourceAllocator(this.config);
    setPutBatchSize(writeStatusRDD, hBaseIndexQPSResourceAllocator, jsc);
    LOG.info("multiPutBatchSize: before hbase puts" + multiPutBatchSize);
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(SparkConfigUtils.getWriteStatusStorageLevel(config.getProps()));
    return writeStatusJavaRDD;
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {

    return (Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>>) (partition, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();
      // Grab the global HBase connection
      synchronized (SparkHBaseIndex.class) {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
          hbaseConnection = getHBaseConnection();
        }
      }
      try (BufferedMutator mutator = hbaseConnection.getBufferedMutator(TableName.valueOf(tableName))) {
        while (statusIterator.hasNext()) {
          WriteStatus writeStatus = statusIterator.next();
          List<Mutation> mutations = new ArrayList<>();
          try {
            for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
              if (!writeStatus.isErrored(rec.getKey())) {
                Option<HoodieRecordLocation> loc = rec.getNewLocation();
                if (loc.isPresent()) {
                  if (rec.getCurrentLocation() != null) {
                    // This is an update, no need to update index
                    continue;
                  }
                  Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN, Bytes.toBytes(loc.get().getInstantTime()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN, Bytes.toBytes(loc.get().getFileId()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN, Bytes.toBytes(rec.getPartitionPath()));
                  mutations.add(put);
                } else {
                  // Delete existing index for a deleted record
                  Delete delete = new Delete(Bytes.toBytes(rec.getRecordKey()));
                  mutations.add(delete);
                }
              }
              if (mutations.size() < multiPutBatchSize) {
                continue;
              }
              doMutations(mutator, mutations);
            }
            // process remaining puts and deletes, if any
            doMutations(mutator, mutations);
          } catch (Exception e) {
            Exception we = new Exception("Error updating index for " + writeStatus, e);
            LOG.error(we);
            writeStatus.setGlobalError(we);
          }
          writeStatusList.add(writeStatus);
        }
      } catch (IOException e) {
        throw new HoodieIndexException("Failed to Update Index locations because of exception with HBase Client", e);
      }
      return writeStatusList.iterator();
    };
  }

  private void setPutBatchSize(JavaRDD<WriteStatus> writeStatusRDD,
                               HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator, final JavaSparkContext jsc) {
    if (config.getHbaseIndexPutBatchSizeAutoCompute()) {
      SparkConf conf = jsc.getConf();
      int maxExecutors = conf.getInt(DEFAULT_SPARK_EXECUTOR_INSTANCES_CONFIG_NAME, 1);
      if (conf.getBoolean(DEFAULT_SPARK_DYNAMIC_ALLOCATION_ENABLED_CONFIG_NAME, false)) {
        maxExecutors =
            Math.max(maxExecutors, conf.getInt(DEFAULT_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG_NAME, 1));
      }

      /*
       * Each writeStatus represents status information from a write done in one of the IOHandles. If a writeStatus has
       * any insert, it implies that the corresponding task contacts HBase for doing puts, since we only do puts for
       * inserts from HBaseIndex.
       */
      final Tuple2<Long, Integer> numPutsParallelismTuple = getHBasePutAccessParallelism(writeStatusRDD);
      final long numPuts = numPutsParallelismTuple._1;
      final int hbasePutsParallelism = numPutsParallelismTuple._2;
      this.numRegionServersForTable = getNumRegionServersAliveForTable();
      final float desiredQPSFraction =
          hBaseIndexQPSResourceAllocator.calculateQPSFractionForPutsTime(numPuts, this.numRegionServersForTable);
      LOG.info("Desired QPSFraction :" + desiredQPSFraction);
      LOG.info("Number HBase puts :" + numPuts);
      LOG.info("Hbase Puts Parallelism :" + hbasePutsParallelism);
      final float availableQpsFraction =
          hBaseIndexQPSResourceAllocator.acquireQPSResources(desiredQPSFraction, numPuts);
      LOG.info("Allocated QPS Fraction :" + availableQpsFraction);
      multiPutBatchSize = putBatchSizeCalculator.getBatchSize(numRegionServersForTable, maxQpsPerRegionServer,
          hbasePutsParallelism, maxExecutors, SLEEP_TIME_MILLISECONDS, availableQpsFraction);
      LOG.info("multiPutBatchSize :" + multiPutBatchSize);
    }
  }

  public Tuple2<Long, Integer> getHBasePutAccessParallelism(final JavaRDD<WriteStatus> writeStatusRDD) {
    final JavaPairRDD<Long, Integer> insertOnlyWriteStatusRDD = writeStatusRDD
        .filter(w -> w.getStat().getNumInserts() > 0).mapToPair(w -> new Tuple2<>(w.getStat().getNumInserts(), 1));
    return insertOnlyWriteStatusRDD.fold(new Tuple2<>(0L, 0), (w, c) -> new Tuple2<>(w._1 + c._1, w._2 + c._2));
  }

}
