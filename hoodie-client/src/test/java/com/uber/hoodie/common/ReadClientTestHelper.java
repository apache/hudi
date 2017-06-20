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

package com.uber.hoodie.common;


import com.google.common.base.Optional;
import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.exception.SchemaCompatabilityException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This serves as a factory for HoodieReadClient & HoodieMergeOnReadClientTestUtil implementations
 */
public class ReadClientTestHelper {

    private final HoodieReadClient readClient;
    private final HoodieMergeOnReadClientTestUtil mergeOnReadClient;
    private final Schema schema;
    private final HoodieTableType tableType;

    public ReadClientTestHelper(JavaSparkContext jsc, String basePath, SQLContext sqlContext, HoodieTableType tableType, Schema schema) {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            this.readClient = new HoodieReadClient(jsc, basePath, sqlContext);
            this.mergeOnReadClient = null;
        } else {
            this.mergeOnReadClient = new HoodieMergeOnReadClientTestUtil(jsc, basePath, sqlContext);
            //initialize HoodieReadClient used for some API's that need not be implemented here again
            this.readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        }
        this.schema = schema;
        this.tableType = tableType;
    }

    /**
     * Given a bunch of hoodie keys, fetches all the individual records
     *
     * @return a List<GenericRecord>
     */
    public List<GenericRecord> read(JavaRDD<HoodieKey> hoodieKeys, int parallelism)
            throws Exception {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.read(hoodieKeys, parallelism).collectAsList().stream().map(row -> createGenericRecordFromRow(this.schema, row))
                    .collect(Collectors.toList());
        }
        else {
            return readClient.read(hoodieKeys, parallelism).collectAsList().stream().map(row -> createGenericRecordFromRow(this.schema, row))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Reads the paths under the a hoodie dataset out as a List<GenericRecord>
     * @return a List<GenericRecord>
     */
    public List<GenericRecord> read(String... paths) {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.read(paths).collectAsList().stream().map(row -> createGenericRecordFromRow(this.schema, row))
                    .collect(Collectors.toList());
        }
        else {
            return mergeOnReadClient.read(paths);
        }
    }

    /**
     * Obtain all new data written into the Hoodie dataset since the given timestamp.
     * @return a List<GenericRecord>
     */
    public List<GenericRecord> readSince(String lastCommitTimestamp) {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.readSince(lastCommitTimestamp).collectAsList().stream().map(row -> createGenericRecordFromRow(this.schema, row))
                    .collect(Collectors.toList());
        }
        else {
            return mergeOnReadClient.readSince(lastCommitTimestamp);
        }
    }

    /**
     *
     * Read a specfic commit
     * @return a List<GenericRecord>
     */
    public List<GenericRecord> readCommit(String commitTime) {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.readCommit(commitTime).collectAsList().stream().map(row -> createGenericRecordFromRow(this.schema, row))
                    .collect(Collectors.toList());
        }
        else {
            return mergeOnReadClient.readCommit(commitTime);
        }
    }


    /**
     * Checks if the given [Keys] exists in the hoodie table and returns [Key,
     * Optional[FullFilePath]] If the optional FullFilePath value is not present, then the key is
     * not found. If the FullFilePath value is present, it is the path component (without scheme) of
     * the URI underlying file
     */
    public JavaPairRDD<HoodieKey, Optional<String>> checkExists(JavaRDD<HoodieKey> hoodieKeys) {
        return readClient.checkExists(hoodieKeys);
    }

    /**
     * Filter out HoodieRecords that already exists in the output folder. This is useful in
     * deduplication.
     *
     * @param hoodieRecords Input RDD of Hoodie records.
     * @return A subset of hoodieRecords RDD, with existing records filtered out.
     */
    public JavaRDD<HoodieRecord> filterExists(JavaRDD<HoodieRecord> hoodieRecords) {
        return readClient.filterExists(hoodieRecords);
    }

    /**
     * Checks if the Hoodie dataset has new data since given timestamp
     */
    public boolean hasNewCommits(String commitTimestamp) {
        return listCommitsSince(commitTimestamp).size() > 0;
    }

    /**
     *
     * @param commitTimestamp
     * @return a List<String>
     */
    public List<String> listCommitsSince(String commitTimestamp) {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.listCommitsSince(commitTimestamp);
        }
        else {
            return mergeOnReadClient.listCommitsSince(commitTimestamp);
        }
    }

    /**
     * Returns the last successful commit (a successful write operation) into a Hoodie table.
     */
    public String latestCommit() {
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            return readClient.latestCommit();
        }
        else {
            return mergeOnReadClient.latestCommit();
        }
    }

    private GenericRecord createGenericRecordFromRow(Schema schema, Row row) {
        GenericRecord newRecord = new GenericData.Record(schema);
        int i = 0;
        for (Schema.Field f : schema.getFields()) {
            newRecord.put(f.name(), row.get(i));
            i++;
        }
        if (!new GenericData().validate(schema, newRecord)) {
            throw new SchemaCompatabilityException(
                    "Unable to validate the rewritten record " + newRecord + " against schema "
                            + schema);
        }
        return newRecord;
    }
}
