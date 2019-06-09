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

package com.uber.hoodie.common;

import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.io.storage.HoodieParquetConfig;
import com.uber.hoodie.io.storage.HoodieParquetWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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

/**
 * Utility methods to aid testing inside the HoodieClient module.
 */
public class HoodieClientTestUtils {

  private static final transient Logger log = LogManager.getLogger(HoodieClientTestUtils.class);

  public static List<WriteStatus> collectStatuses(Iterator<List<WriteStatus>> statusListItr) {
    List<WriteStatus> statuses = new ArrayList<>();
    while (statusListItr.hasNext()) {
      statuses.addAll(statusListItr.next());
    }
    return statuses;
  }

  public static Set<String> getRecordKeys(List<HoodieRecord> hoodieRecords) {
    Set<String> keys = new HashSet<>();
    for (HoodieRecord rec : hoodieRecords) {
      keys.add(rec.getRecordKey());
    }
    return keys;
  }

  private static void fakeMetaFile(String basePath, String commitTime, String suffix) throws IOException {
    String parentPath = basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME;
    new File(parentPath).mkdirs();
    new File(parentPath + "/" + commitTime + suffix).createNewFile();
  }


  public static void fakeCommitFile(String basePath, String commitTime) throws IOException {
    fakeMetaFile(basePath, commitTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void fakeInFlightFile(String basePath, String commitTime) throws IOException {
    fakeMetaFile(basePath, commitTime, HoodieTimeline.INFLIGHT_EXTENSION);
  }

  public static void fakeDataFile(String basePath, String partitionPath, String commitTime, String fileId)
      throws Exception {
    fakeDataFile(basePath, partitionPath, commitTime, fileId, 0);
  }

  public static void fakeDataFile(String basePath, String partitionPath, String commitTime, String fileId, long length)
      throws Exception {
    String parentPath = String.format("%s/%s", basePath, partitionPath);
    new File(parentPath).mkdirs();
    String path = String.format("%s/%s", parentPath, FSUtils.makeDataFileName(commitTime, "1-0-1", fileId));
    new File(path).createNewFile();
    new RandomAccessFile(path, "rw").setLength(length);
  }

  public static SparkConf getSparkConfForTest(String appName) {
    SparkConf sparkConf = new SparkConf().setAppName(appName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .setMaster("local[8]");
    return HoodieReadClient.addHoodieSupport(sparkConf);
  }

  public static HashMap<String, String> getLatestFileIDsToFullPath(String basePath, HoodieTimeline commitTimeline,
      List<HoodieInstant> commitsToReturn) throws IOException {
    HashMap<String, String> fileIdToFullPath = new HashMap<>();
    for (HoodieInstant commit : commitsToReturn) {
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get(),
          HoodieCommitMetadata.class);
      fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths(basePath));
    }
    return fileIdToFullPath;
  }

  public static Dataset<Row> readCommit(String basePath, SQLContext sqlContext, HoodieTimeline commitTimeline,
      String commitTime) {
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);
    if (!commitTimeline.containsInstant(commitInstant)) {
      new HoodieException("No commit exists at " + commitTime);
    }
    try {
      HashMap<String, String> paths = getLatestFileIDsToFullPath(basePath, commitTimeline,
          Arrays.asList(commitInstant));
      log.info("Path :" + paths.values());
      return sqlContext.read().parquet(paths.values().toArray(new String[paths.size()]))
          .filter(String.format("%s ='%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitTime));
    } catch (Exception e) {
      throw new HoodieException("Error reading commit " + commitTime, e);
    }
  }

  /**
   * Obtain all new data written into the Hoodie dataset since the given timestamp.
   */
  public static Dataset<Row> readSince(String basePath, SQLContext sqlContext, HoodieTimeline commitTimeline,
      String lastCommitTime) {
    List<HoodieInstant> commitsToReturn = commitTimeline.findInstantsAfter(lastCommitTime, Integer.MAX_VALUE)
        .getInstants().collect(Collectors.toList());
    try {
      // Go over the commit metadata, and obtain the new files that need to be read.
      HashMap<String, String> fileIdToFullPath = getLatestFileIDsToFullPath(basePath, commitTimeline, commitsToReturn);
      return sqlContext.read().parquet(fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]))
          .filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTime));
    } catch (IOException e) {
      throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTime, e);
    }
  }

  /**
   * Reads the paths under the a hoodie dataset out as a DataFrame
   */
  public static Dataset<Row> read(JavaSparkContext jsc, String basePath, SQLContext
      sqlContext,
      FileSystem
          fs, String...
      paths) {
    List<String> filteredPaths = new ArrayList<>();
    try {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
      for (String path : paths) {
        TableFileSystemView.ReadOptimizedView fileSystemView = new HoodieTableFileSystemView(
            metaClient, metaClient.getCommitsTimeline().filterCompletedInstants(), fs.globStatus(new Path(path)));
        List<HoodieDataFile> latestFiles = fileSystemView.getLatestDataFiles().collect(Collectors.toList());
        for (HoodieDataFile file : latestFiles) {
          filteredPaths.add(file.getPath());
        }
      }
      return sqlContext.read().parquet(filteredPaths.toArray(new String[filteredPaths.size()]));
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie dataset as a dataframe", e);
    }
  }

  public static String writeParquetFile(String basePath,
                                        String partitionPath,
                                        String filename,
                                        List<HoodieRecord> records,
                                        Schema schema,
                                        BloomFilter filter,
                                        boolean createCommitTime) throws IOException {

    if (filter == null) {
      filter = new BloomFilter(10000, 0.0000001);
    }
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema,
        filter);
    String commitTime = FSUtils.getCommitTime(filename);
    HoodieParquetConfig config = new HoodieParquetConfig(writeSupport, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
        HoodieTestUtils.getDefaultHadoopConf(),
        Double.valueOf(HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO));
    HoodieParquetWriter writer = new HoodieParquetWriter(
        commitTime,
        new Path(basePath + "/" + partitionPath + "/" + filename),
        config,
        schema);
    int seqId = 1;
    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, commitTime, "" + seqId++);
      HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), filename);
      writer.writeAvro(record.getRecordKey(), avroRecord);
      filter.add(record.getRecordKey());
    }
    writer.close();

    if (createCommitTime) {
      HoodieTestUtils.createMetadataFolder(basePath);
      HoodieTestUtils.createCommitFiles(basePath, commitTime);
    }
    return filename;
  }

  public static String writeParquetFile(String basePath,
                                        String partitionPath,
                                        List<HoodieRecord> records,
                                        Schema schema,
                                        BloomFilter filter,
                                        boolean createCommitTime) throws IOException, InterruptedException {
    Thread.sleep(1000);
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    String fileId = UUID.randomUUID().toString();
    String filename = FSUtils.makeDataFileName(commitTime, "1-0-1", fileId);
    HoodieTestUtils.createCommitFiles(basePath, commitTime);
    return HoodieClientTestUtils
        .writeParquetFile(basePath, partitionPath, filename, records, schema, filter, createCommitTime);
  }
}
