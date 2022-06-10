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

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Provides an RDD based API for accessing/filtering Hoodie tables, based on keys.
 */
public class HoodieReadClient<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * TODO: We need to persist the index type into hoodie.properties and be able to access the index just with a simple
   * base path pointing to the table. Until, then just always assume a BloomIndex
   */
  private final transient HoodieIndex<?, ?> index;
  private HoodieTable hoodieTable;
  private transient Option<SQLContext> sqlContextOpt;
  private final transient HoodieSparkEngineContext context;
  private final transient Configuration hadoopConf;

  /**
   * @param basePath path to Hoodie table
   */
  public HoodieReadClient(HoodieSparkEngineContext context, String basePath) {
    this(context, HoodieWriteConfig.newBuilder().withPath(basePath)
        // by default we use HoodieBloomIndex
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build());
  }

  /**
   * @param context
   * @param basePath
   * @param sqlContext
   */
  public HoodieReadClient(HoodieSparkEngineContext context, String basePath, SQLContext sqlContext) {
    this(context, basePath);
    this.sqlContextOpt = Option.of(sqlContext);
  }

  /**
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieReadClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig) {
    this.context = context;
    this.hadoopConf = context.getHadoopConf().get();
    final String basePath = clientConfig.getBasePath();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    this.hoodieTable = HoodieSparkTable.create(clientConfig, context, metaClient);
    this.index = SparkHoodieIndexFactory.createIndex(clientConfig);
    this.sqlContextOpt = Option.empty();
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

  private Option<String> convertToDataFilePath(Option<Pair<String, String>> partitionPathFileIDPair) {
    if (partitionPathFileIDPair.isPresent()) {
      HoodieBaseFile dataFile = hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFile(partitionPathFileIDPair.get().getLeft(), partitionPathFileIDPair.get().getRight()).get();
      return Option.of(dataFile.getPath());
    } else {
      return Option.empty();
    }
  }

  /**
   * Given a bunch of hoodie keys, fetches all the individual records out as a data frame.
   *
   * @return a dataframe
   */
  public Dataset<Row> readROView(JavaRDD<HoodieKey> hoodieKeys, int parallelism) {
    assertSqlContext();
    JavaPairRDD<HoodieKey, Option<Pair<String, String>>> lookupResultRDD = checkExists(hoodieKeys);
    JavaPairRDD<HoodieKey, Option<String>> keyToFileRDD =
        lookupResultRDD.mapToPair(r -> new Tuple2<>(r._1, convertToDataFilePath(r._2)));
    List<String> paths = keyToFileRDD.filter(keyFileTuple -> keyFileTuple._2().isPresent())
        .map(keyFileTuple -> keyFileTuple._2().get()).collect();

    // record locations might be same for multiple keys, so need a unique list
    Set<String> uniquePaths = new HashSet<>(paths);
    Dataset<Row> originalDF = null;
    // read files based on the file extension name
    if (paths.size() == 0 || paths.get(0).endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      originalDF = sqlContextOpt.get().read().parquet(uniquePaths.toArray(new String[uniquePaths.size()]));
    } else if (paths.get(0).endsWith(HoodieFileFormat.ORC.getFileExtension())) {
      originalDF = sqlContextOpt.get().read().orc(uniquePaths.toArray(new String[uniquePaths.size()]));
    }
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
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Option[FullFilePath]] If the optional
   * FullFilePath value is not present, then the key is not found. If the FullFilePath value is present, it is the path
   * component (without scheme) of the URI underlying file
   */
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> checkExists(JavaRDD<HoodieKey> hoodieKeys) {
    return HoodieJavaRDD.getJavaRDD(
        index.tagLocation(HoodieJavaRDD.of(hoodieKeys.map(k -> new HoodieAvroRecord<>(k, null))),
            context, hoodieTable))
        .mapToPair(hr -> new Tuple2<>(hr.getKey(), hr.isCurrentLocationKnown()
            ? Option.of(Pair.of(hr.getPartitionPath(), hr.getCurrentLocation().getFileId()))
            : Option.empty())
        );
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    JavaRDD<HoodieRecord<T>> recordsWithLocation = tagLocation(hoodieRecords);
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present). Input RDD should contain no duplicates if needed.
   *
   * @param hoodieRecords Input RDD of Hoodie records
   * @return Tagged RDD of Hoodie records
   */
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> hoodieRecords) throws HoodieIndexException {
    return HoodieJavaRDD.getJavaRDD(
        index.tagLocation(HoodieJavaRDD.of(hoodieRecords), context, hoodieTable));
  }

  /**
   * Return all pending compactions with instant time for clients to decide what to compact next.
   *
   * @return
   */
  public List<Pair<String, HoodieCompactionPlan>> getPendingCompactions() {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(hoodieTable.getMetaClient().getBasePath()).setLoadActiveTimelineOnLoad(true).build();
    return CompactionUtils.getAllPendingCompactionPlans(metaClient).stream()
        .map(
            instantWorkloadPair -> Pair.of(instantWorkloadPair.getKey().getTimestamp(), instantWorkloadPair.getValue()))
        .collect(Collectors.toList());
  }
}
