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

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieRecord;

import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.exception.HoodieDependentSystemUnavailableException;
import com.uber.hoodie.exception.HoodieIndexException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Hoodie Index implementation backed by HBase
 */
public class HBaseIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {
    private final static byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
    private final static byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
    private final static byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
    private final static byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");

    private static Logger logger = LogManager.getLogger(HBaseIndex.class);

    private final String tableName;

    public HBaseIndex(HoodieWriteConfig config, JavaSparkContext jsc) {
        super(config, jsc);
        this.tableName = config.getProps().getProperty(HoodieIndexConfig.HBASE_TABLENAME_PROP);
    }

    @Override
    public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(
        JavaRDD<HoodieKey> hoodieKeys, HoodieTableMetaClient metaClient) {
        throw new UnsupportedOperationException("HBase index does not implement check exist yet");
    }

    private static Connection hbaseConnection = null;

    private Connection getHBaseConnection() {
        Configuration hbaseConfig = HBaseConfiguration.create();
        String quorum = config.getProps().getProperty(HoodieIndexConfig.HBASE_ZKQUORUM_PROP);
        hbaseConfig.set("hbase.zookeeper.quorum", quorum);
        String port = config.getProps().getProperty(HoodieIndexConfig.HBASE_ZKPORT_PROP);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
        try {
            return ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {
            throw new HoodieDependentSystemUnavailableException(
                HoodieDependentSystemUnavailableException.HBASE, quorum + ":" + port);
        }
    }

    /**
     * Function that tags each HoodieRecord with an existing location, if known.
     */
    class LocationTagFunction
            implements Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> {

        private final HoodieTableMetaClient metaClient;

        LocationTagFunction(HoodieTableMetaClient metaClient) {
            this.metaClient = metaClient;
        }

        @Override
        public Iterator<HoodieRecord<T>> call(Integer partitionNum,
                                           Iterator<HoodieRecord<T>> hoodieRecordIterator) {
            // Grab the global HBase connection
            synchronized (HBaseIndex.class) {
                if (hbaseConnection == null) {
                    hbaseConnection = getHBaseConnection();
                }
            }
            List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
            HTable hTable = null;
            try {
                hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
                // Do the tagging.
                while (hoodieRecordIterator.hasNext()) {
                    HoodieRecord rec = hoodieRecordIterator.next();
                    // TODO(vc): This may need to be a multi get.
                    Result result = hTable.get(
                            new Get(Bytes.toBytes(rec.getRecordKey())).setMaxVersions(1)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                    // first, attempt to grab location from HBase
                    if (result.getRow() != null) {
                        String commitTs =
                                Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                        String fileId =
                                Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));

                        HoodieTimeline commitTimeline = metaClient.getActiveCommitTimeline();
                        // if the last commit ts for this row is less than the system commit ts
                        if (commitTimeline.hasInstants() && commitTimeline.containsInstant(commitTs)) {
                            rec.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                        }
                    }
                    taggedRecords.add(rec);
                }
            } catch (IOException e) {
                throw new HoodieIndexException(
                    "Failed to Tag indexed locations because of exception with HBase Client", e);
            }

            finally {
                if (hTable != null) {
                    try {
                        hTable.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }

            }
            return taggedRecords.iterator();
        }
    }

    @Override
    public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
                                             HoodieTableMetaClient metaClient) {
        return recordRDD.mapPartitionsWithIndex(this.new LocationTagFunction(metaClient), true);
    }

    class UpdateLocationTask implements Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> {
        @Override
        public Iterator<WriteStatus> call(Integer partition, Iterator<WriteStatus> statusIterator) {

            List<WriteStatus> writeStatusList = new ArrayList<>();
            // Grab the global HBase connection
            synchronized (HBaseIndex.class) {
                if (hbaseConnection == null) {
                    hbaseConnection = getHBaseConnection();
                }
            }
            HTable hTable = null;
            try {
                hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
                while (statusIterator.hasNext()) {
                    WriteStatus writeStatus = statusIterator.next();
                    List<Put> puts = new ArrayList<>();
                    try {
                        for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
                            if (!writeStatus.isErrored(rec.getKey())) {
                                Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                                HoodieRecordLocation loc = rec.getNewLocation();
                                put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN,
                                    Bytes.toBytes(loc.getCommitTime()));
                                put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN,
                                    Bytes.toBytes(loc.getFileId()));
                                put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN,
                                    Bytes.toBytes(rec.getPartitionPath()));
                                puts.add(put);
                            }
                        }
                        hTable.put(puts);
                        hTable.flushCommits();
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
        }
    }

    @Override
    public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                               HoodieTableMetaClient metaClient) {
        return writeStatusRDD.mapPartitionsWithIndex(new UpdateLocationTask(), true);
    }

    @Override
    public boolean rollbackCommit(String commitTime) {
        // TODO (weiy)
        return true;
    }
}
