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

package org.apache.hudi.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utility methods to aid testing inside the HoodieClient module.
 */
public class HoodieClientTestUtils {

  private static final Logger LOG = LogManager.getLogger(HoodieClientTestUtils.class);

  /**
   * Returns a Spark config for this test.
   *
   * The following properties may be set to customize the Spark context:
   *   SPARK_EVLOG_DIR: Local directory where event logs should be saved. This
   *                    allows viewing the logs with spark-history-server.
   *
   * @note When running the tests using maven, use the following syntax to set
   *       a property:
   *          mvn -DSPARK_XXX=yyy ...
   *
   * @param appName A name for the Spark application. Shown in the Spark web UI.
   * @return A Spark config
   */
  public static SparkConf getSparkConfForTest(String appName) {
    SparkConf sparkConf = new SparkConf().setAppName(appName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[8]");

    String evlogDir = System.getProperty("SPARK_EVLOG_DIR");
    if (evlogDir != null) {
      sparkConf.set("spark.eventLog.enabled", "true");
      sparkConf.set("spark.eventLog.dir", evlogDir);
    }

    return HoodieReadClient.addHoodieSupport(sparkConf);
  }

  private static HashMap<String, String> getLatestFileIDsToFullPath(String basePath, HoodieTimeline commitTimeline,
                                                                    List<HoodieInstant> commitsToReturn) throws IOException {
    HashMap<String, String> fileIdToFullPath = new HashMap<>();
    for (HoodieInstant commit : commitsToReturn) {
      HoodieCommitMetadata metadata =
          HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get(), HoodieCommitMetadata.class);
      fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths(basePath));
    }
    return fileIdToFullPath;
  }

  public static Dataset<Row> readCommit(String basePath, SQLContext sqlContext, HoodieTimeline commitTimeline,
                                        String instantTime) {
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime);
    if (!commitTimeline.containsInstant(commitInstant)) {
      throw new HoodieException("No commit exists at " + instantTime);
    }
    try {
      HashMap<String, String> paths =
          getLatestFileIDsToFullPath(basePath, commitTimeline, Arrays.asList(commitInstant));
      LOG.info("Path :" + paths.values());
      return sqlContext.read().parquet(paths.values().toArray(new String[paths.size()]))
          .filter(String.format("%s ='%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime));
    } catch (Exception e) {
      throw new HoodieException("Error reading commit " + instantTime, e);
    }
  }

  /**
   * Obtain all new data written into the Hoodie table since the given timestamp.
   */
  public static Dataset<Row> readSince(String basePath, SQLContext sqlContext,
                                       HoodieTimeline commitTimeline, String lastCommitTime) {
    List<HoodieInstant> commitsToReturn =
        commitTimeline.findInstantsAfter(lastCommitTime, Integer.MAX_VALUE).getInstants().collect(Collectors.toList());
    try {
      // Go over the commit metadata, and obtain the new files that need to be read.
      HashMap<String, String> fileIdToFullPath = getLatestFileIDsToFullPath(basePath, commitTimeline, commitsToReturn);
      String[] paths = fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]);
      Dataset<Row> rows = null;
      if (paths[0].endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        rows = sqlContext.read().parquet(paths);
      }

      return rows.filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTime));
    } catch (IOException e) {
      throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTime, e);
    }
  }

  /**
   * Reads the paths under the a hoodie table out as a DataFrame.
   */
  public static Dataset<Row> read(JavaSparkContext jsc, String basePath, SQLContext sqlContext, FileSystem fs,
                                  String... paths) {
    List<String> filteredPaths = new ArrayList<>();
    try {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
      for (String path : paths) {
        BaseFileOnlyView fileSystemView = new HoodieTableFileSystemView(metaClient,
            metaClient.getCommitsTimeline().filterCompletedInstants(), fs.globStatus(new Path(path)));
        List<HoodieBaseFile> latestFiles = fileSystemView.getLatestBaseFiles().collect(Collectors.toList());
        for (HoodieBaseFile file : latestFiles) {
          filteredPaths.add(file.getPath());
        }
      }
      return sqlContext.read().parquet(filteredPaths.toArray(new String[filteredPaths.size()]));
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }

  /**
   * TODO Incorporate into {@link org.apache.hudi.common.testutils.HoodieTestTable}.
   */
  public static String writeParquetFile(String basePath, String partitionPath, String filename,
                                        List<HoodieRecord> records, Schema schema, BloomFilter filter, boolean createCommitTime) throws IOException {

    if (filter == null) {
      filter = BloomFilterFactory
          .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    }
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, filter);
    String instantTime = FSUtils.getCommitTime(filename);
    HoodieAvroParquetConfig config = new HoodieAvroParquetConfig(writeSupport, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
        HoodieTestUtils.getDefaultHadoopConf(), Double.valueOf(HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO));
    HoodieParquetWriter writer =
        new HoodieParquetWriter(instantTime, new Path(basePath + "/" + partitionPath + "/" + filename), config,
                schema, new SparkTaskContextSupplier());
    int seqId = 1;
    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, instantTime, "" + seqId++);
      HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), filename);
      writer.writeAvro(record.getRecordKey(), avroRecord);
      filter.add(record.getRecordKey());
    }
    writer.close();

    if (createCommitTime) {
      HoodieTestUtils.createMetadataFolder(basePath);
      HoodieTestUtils.createCommitFiles(basePath, instantTime);
    }
    return filename;
  }

  /**
   * TODO Incorporate into {@link org.apache.hudi.common.testutils.HoodieTestTable}.
   */
  public static String writeParquetFile(String basePath, String partitionPath, List<HoodieRecord> records,
                                        Schema schema, BloomFilter filter, boolean createCommitTime) throws IOException, InterruptedException {
    Thread.sleep(1000);
    String instantTime = HoodieTestUtils.makeNewCommitTime();
    String fileId = UUID.randomUUID().toString();
    String filename = FSUtils.makeDataFileName(instantTime, "1-0-1", fileId);
    HoodieTestUtils.createCommitFiles(basePath, instantTime);
    return HoodieClientTestUtils.writeParquetFile(basePath, partitionPath, filename, records, schema, filter,
        createCommitTime);
  }

}
