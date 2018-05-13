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
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * Provides an RDD based API for accessing/filtering Hoodie tables, based on keys.
 */
public class HoodieReadClient<T extends HoodieRecordPayload> implements Serializable {

  private static final Logger logger = LogManager.getLogger(HoodieReadClient.class);

  private final transient JavaSparkContext jsc;

  private final transient FileSystem fs;
  /**
   * TODO: We need to persist the index type into hoodie.properties and be able to access the index
   * just with a simple basepath pointing to the dataset. Until, then just always assume a
   * BloomIndex
   */
  private final transient HoodieIndex<T> index;
  private final HoodieTimeline commitTimeline;
  private HoodieTable hoodieTable;
  private transient Optional<SQLContext> sqlContextOpt;

  /**
   * @param basePath path to Hoodie dataset
   */
  public HoodieReadClient(JavaSparkContext jsc, String basePath) {
    this(jsc, HoodieWriteConfig.newBuilder().withPath(basePath)
        // by default we use HoodieBloomIndex
        .withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build());
  }

  /**
   * @param jsc
   * @param basePath
   * @param sqlContext
   */
  public HoodieReadClient(JavaSparkContext jsc, String basePath, SQLContext sqlContext) {
    this(jsc, basePath);
    this.sqlContextOpt = Optional.of(sqlContext);
  }

  /**
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieReadClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) {
    final String basePath = clientConfig.getBasePath();
    this.jsc = jsc;
    this.fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath, true);
    this.hoodieTable = HoodieTable
        .getHoodieTable(metaClient,
            clientConfig, jsc);
    this.commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
    this.index = HoodieIndex.createIndex(clientConfig, jsc);
    this.sqlContextOpt = Optional.absent();
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
      throw new IllegalStateException(
          "SQLContext must be set, when performing dataframe operations");
    }
  }

  /**
   * Given a bunch of hoodie keys, fetches all the individual records out as a data frame
   *
   * @return a dataframe
   */
  public Dataset<Row> read(JavaRDD<HoodieKey> hoodieKeys, int parallelism) throws Exception {

    assertSqlContext();
    JavaPairRDD<HoodieKey, Optional<String>> keyToFileRDD = index
        .fetchRecordLocation(hoodieKeys, jsc, hoodieTable);
    List<String> paths = keyToFileRDD.filter(keyFileTuple -> keyFileTuple._2().isPresent())
        .map(keyFileTuple -> keyFileTuple._2().get()).collect();

    // record locations might be same for multiple keys, so need a unique list
    Set<String> uniquePaths = new HashSet<>(paths);
    Dataset<Row> originalDF = sqlContextOpt.get().read()
        .parquet(uniquePaths.toArray(new String[uniquePaths.size()]));
    StructType schema = originalDF.schema();
    JavaPairRDD<HoodieKey, Row> keyRowRDD = originalDF.javaRDD().mapToPair(row -> {
      HoodieKey key = new HoodieKey(row.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD),
          row.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
      return new Tuple2<>(key, row);
    });

    // Now, we need to further filter out, for only rows that match the supplied hoodie keys
    JavaRDD<Row> rowRDD = keyRowRDD.join(keyToFileRDD, parallelism).map(tuple -> tuple._2()._1());

    return sqlContextOpt.get().createDataFrame(rowRDD, schema);
  }

  /**
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Optional[FullFilePath]]
   * If the optional FullFilePath value is not present, then the key is not found. If the
   * FullFilePath value is present, it is the path component (without scheme) of the URI underlying
   * file
   */
  public JavaPairRDD<HoodieKey, Optional<String>> checkExists(JavaRDD<HoodieKey> hoodieKeys) {
    return index.fetchRecordLocation(hoodieKeys, jsc, hoodieTable);
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in
   * deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    JavaRDD<HoodieRecord<T>> recordsWithLocation = tagLocation(hoodieRecords);
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the
   * row (if it is actually present).  Input RDD should contain no duplicates if needed.
   *
   * @param hoodieRecords Input RDD of Hoodie records
   * @return Tagged RDD of Hoodie records
   */
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> hoodieRecords)
      throws HoodieIndexException {
    return index.tagLocation(hoodieRecords, jsc, hoodieTable);
  }
}
