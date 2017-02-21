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

package com.uber.hoodie;

import com.google.common.base.Optional;

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCommits;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.index.HoodieBloomIndex;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.Tuple2;

/**
 * Provides first class support for accessing Hoodie tables for data processing via Apache Spark.
 *
 *
 * TODO: Need to move all read operations here, since Hoodie is a single writer and multiple reader
 */
public class HoodieReadClient implements Serializable {

    private static Logger logger = LogManager.getLogger(HoodieReadClient.class);

    private transient final JavaSparkContext jsc;

    private transient final FileSystem fs;
    /**
     * TODO: We need to persist the index type into hoodie.properties and be able to access the index
     * just with a simple basepath pointing to the dataset. Until, then just always assume a
     * BloomIndex
     */
    private transient final HoodieBloomIndex index;
    private HoodieTableMetadata metadata;
    private transient Optional<SQLContext> sqlContextOpt;


    /**
     * @param basePath path to Hoodie dataset
     */
    public HoodieReadClient(JavaSparkContext jsc, String basePath) {
        this.jsc = jsc;
        this.fs = FSUtils.getFs();
        this.metadata = new HoodieTableMetadata(fs, basePath);
        this.index = new HoodieBloomIndex(HoodieWriteConfig.newBuilder().withPath(basePath).build(), jsc);
        this.sqlContextOpt = Optional.absent();
    }

    /**
     *
     * @param jsc
     * @param basePath
     * @param sqlContext
     */
    public HoodieReadClient(JavaSparkContext jsc, String basePath, SQLContext sqlContext) {
        this(jsc, basePath);
        this.sqlContextOpt = Optional.of(sqlContext);
    }

    /**
     * Adds support for accessing Hoodie built tables from SparkSQL, as you normally would.
     *
     * @return SparkConf object to be used to construct the SparkContext by caller
     */
    public static SparkConf addHoodieSupport(SparkConf conf) {
        conf.set("spark.sql.hive.convertMetastoreParquet", "false");
        return conf;
    }

    private void assertSqlContext() {
        if (!sqlContextOpt.isPresent()) {
            throw new IllegalStateException("SQLContext must be set, when performing dataframe operations");
        }
    }

    /**
     * Given a bunch of hoodie keys, fetches all the individual records out as a data frame
     *
     * @return a dataframe
     */
    public Dataset<Row> read(JavaRDD<HoodieKey> hoodieKeys, int parallelism)
            throws Exception {

        assertSqlContext();
        JavaPairRDD<HoodieKey, Optional<String>> keyToFileRDD =
                index.fetchRecordLocation(hoodieKeys, metadata);
        List<String> paths = keyToFileRDD
                .filter(new Function<Tuple2<HoodieKey, Optional<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<HoodieKey, Optional<String>> keyFileTuple) throws Exception {
                        return keyFileTuple._2().isPresent();
                    }
                })
                .map(new Function<Tuple2<HoodieKey, Optional<String>>, String>() {

                    @Override
                    public String call(Tuple2<HoodieKey, Optional<String>> keyFileTuple) throws Exception {
                        return keyFileTuple._2().get();
                    }
                }).collect();

        // record locations might be same for multiple keys, so need a unique list
        Set<String> uniquePaths = new HashSet<>(paths);
        Dataset<Row> originalDF = sqlContextOpt.get().read()
                .parquet(uniquePaths.toArray(new String[uniquePaths.size()]));
        StructType schema = originalDF.schema();
        JavaPairRDD<HoodieKey, Row> keyRowRDD = originalDF.javaRDD()
                .mapToPair(new PairFunction<Row, HoodieKey, Row>() {
                    @Override
                    public Tuple2<HoodieKey, Row> call(Row row) throws Exception {
                        HoodieKey key = new HoodieKey(
                                row.<String>getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD),
                                row.<String>getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
                        return new Tuple2<>(key, row);
                    }
                });

        // Now, we need to further filter out, for only rows that match the supplied hoodie keys
        JavaRDD<Row> rowRDD = keyRowRDD.join(keyToFileRDD, parallelism)
                .map(new Function<Tuple2<HoodieKey, Tuple2<Row, Optional<String>>>, Row>() {
                    @Override
                    public Row call(Tuple2<HoodieKey, Tuple2<Row, Optional<String>>> tuple) throws Exception {
                        return tuple._2()._1();
                    }
                });

        return sqlContextOpt.get().createDataFrame(rowRDD, schema);
    }

    /**
     * Reads the paths under the a hoodie dataset out as a DataFrame
     */
    public Dataset<Row> read(String... paths) {
        assertSqlContext();
        List<String> filteredPaths = new ArrayList<>();
        try {
            for (String path : paths) {
                if (!path.contains(metadata.getBasePath())) {
                    throw new HoodieException("Path " + path
                            + " does not seem to be a part of a Hoodie dataset at base path "
                            + metadata.getBasePath());
                }

                FileStatus[] latestFiles = metadata.getLatestVersions(fs.globStatus(new Path(path)));
                for (FileStatus file : latestFiles) {
                    filteredPaths.add(file.getPath().toString());
                }
            }
            return sqlContextOpt.get().read()
                    .parquet(filteredPaths.toArray(new String[filteredPaths.size()]));
        } catch (Exception e) {
            throw new HoodieException("Error reading hoodie dataset as a dataframe", e);
        }
    }

    /**
     * Obtain all new data written into the Hoodie dataset since the given timestamp.
     *
     * If you made a prior call to {@link HoodieReadClient#latestCommit()}, it gives you all data in
     * the time window (commitTimestamp, latestCommit)
     */
    public Dataset<Row> readSince(String lastCommitTimestamp) {

        List<String> commitsToReturn = metadata.findCommitsAfter(lastCommitTimestamp, Integer.MAX_VALUE);
        //TODO: we can potentially trim this down to only affected partitions, using CommitMetadata
        try {

            // Go over the commit metadata, and obtain the new files that need to be read.
            HashMap<String, String> fileIdToFullPath = new HashMap<>();
            for (String commit: commitsToReturn) {
                // get files from each commit, and replace any previous versions
                fileIdToFullPath.putAll(metadata.getCommitMetadata(commit).getFileIdAndFullPaths());
            }

            return sqlContextOpt.get().read()
                    .parquet(fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]))
                    .filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTimestamp));
        } catch (IOException e) {
            throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTimestamp, e);
        }
    }

    /**
     * Obtain
     */
    public Dataset<Row> readCommit(String commitTime) {
        assertSqlContext();
        HoodieCommits commits = metadata.getAllCommits();
        if (!commits.contains(commitTime)) {
            new HoodieException("No commit exists at " + commitTime);
        }

        try {
            HoodieCommitMetadata commitMetdata = metadata.getCommitMetadata(commitTime);
            Collection<String> paths = commitMetdata.getFileIdAndFullPaths().values();
            return sqlContextOpt.get().read()
                    .parquet(paths.toArray(new String[paths.size()]))
                    .filter(String.format("%s ='%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitTime));
        } catch (Exception e) {
            throw new HoodieException("Error reading commit " + commitTime, e);
        }
    }

    /**
     * Checks if the given [Keys] exists in the hoodie table and returns [Key,
     * Optional[FullFilePath]] If the optional FullFilePath value is not present, then the key is
     * not found. If the FullFilePath value is present, it is the path component (without scheme) of
     * the URI underlying file
     */
    public JavaPairRDD<HoodieKey, Optional<String>> checkExists(
            JavaRDD<HoodieKey> hoodieKeys) {
        return index.fetchRecordLocation(hoodieKeys, metadata);
    }

    /**
     * Filter out HoodieRecords that already exists in the output folder. This is useful in
     * deduplication.
     *
     * @param hoodieRecords Input RDD of Hoodie records.
     * @return A subset of hoodieRecords RDD, with existing records filtered out.
     */
    public JavaRDD<HoodieRecord> filterExists(JavaRDD<HoodieRecord> hoodieRecords) {
        JavaRDD<HoodieRecord> recordsWithLocation = index.tagLocation(hoodieRecords, metadata);
        return recordsWithLocation.filter(new Function<HoodieRecord, Boolean>() {
            @Override
            public Boolean call(HoodieRecord v1) throws Exception {
                return !v1.isCurrentLocationKnown();
            }
        });
    }

    /**
     * Checks if the Hoodie dataset has new data since given timestamp. This can be subsequently
     * used to call {@link HoodieReadClient#readSince(String)} to perform incremental processing.
     */
    public boolean hasNewCommits(String commitTimestamp) {
        return listCommitsSince(commitTimestamp).size() > 0;
    }

    /**
     *
     * @param commitTimestamp
     * @return
     */
    public List<String> listCommitsSince(String commitTimestamp) {
        return metadata.getAllCommits().findCommitsAfter(commitTimestamp, Integer.MAX_VALUE);
    }

    /**
     * Returns the last successful commit (a successful write operation) into a Hoodie table.
     */
    public String latestCommit() {
        return metadata.getAllCommits().lastCommit();
    }
}
