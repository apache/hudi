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
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  public static long countRecordsSince(JavaSparkContext jsc, String basePath, SQLContext sqlContext,
                                       HoodieTimeline commitTimeline, String lastCommitTime) {
    List<HoodieInstant> commitsToReturn =
        commitTimeline.findInstantsAfter(lastCommitTime, Integer.MAX_VALUE).getInstants().collect(Collectors.toList());
    try {
      // Go over the commit metadata, and obtain the new files that need to be read.
      HashMap<String, String> fileIdToFullPath = getLatestFileIDsToFullPath(basePath, commitTimeline, commitsToReturn);
      String[] paths = fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]);
      if (paths[0].endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        return sqlContext.read().parquet(paths)
            .filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTime))
            .count();
      } else if (paths[0].endsWith(HoodieFileFormat.HFILE.getFileExtension())) {
        return readHFile(jsc, paths)
            .filter(gr -> HoodieTimeline.compareTimestamps(lastCommitTime, HoodieActiveTimeline.LESSER_THAN,
                gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()))
            .count();
      }
      throw new HoodieException("Unsupported base file format for file :" + paths[0]);
    } catch (IOException e) {
      throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTime, e);
    }
  }

  public static List<HoodieBaseFile> getLatestBaseFiles(String basePath, FileSystem fs,
                                                String... paths) {
    List<HoodieBaseFile> latestFiles = new ArrayList<>();
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
      for (String path : paths) {
        BaseFileOnlyView fileSystemView = new HoodieTableFileSystemView(metaClient,
            metaClient.getCommitsTimeline().filterCompletedInstants(), fs.globStatus(new Path(path)));
        latestFiles.addAll(fileSystemView.getLatestBaseFiles().collect(Collectors.toList()));
      }
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
    return latestFiles;
  }

  /**
   * Reads the paths under the a hoodie table out as a DataFrame.
   */
  public static Dataset<Row> read(JavaSparkContext jsc, String basePath, SQLContext sqlContext, FileSystem fs,
                                  String... paths) {
    List<String> filteredPaths = new ArrayList<>();
    try {
      List<HoodieBaseFile> latestFiles = getLatestBaseFiles(basePath, fs, paths);
      for (HoodieBaseFile file : latestFiles) {
        filteredPaths.add(file.getPath());
      }
      return sqlContext.read().parquet(filteredPaths.toArray(new String[filteredPaths.size()]));
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }

  public static Stream<GenericRecord> readHFile(JavaSparkContext jsc, String[] paths) {
    // TODO: this should be ported to use HoodieStorageReader
    List<GenericRecord> valuesAsList = new LinkedList<>();

    FileSystem fs = FSUtils.getFs(paths[0], jsc.hadoopConfiguration());
    CacheConfig cacheConfig = new CacheConfig(fs.getConf());
    Schema schema = null;
    for (String path : paths) {
      try {
        HFile.Reader reader = HFile.createReader(fs, new Path(path), cacheConfig, fs.getConf());
        if (schema == null) {
          schema = new Schema.Parser().parse(new String(reader.loadFileInfo().get("schema".getBytes())));
        }
        HFileScanner scanner = reader.getScanner(false, false);
        if (!scanner.seekTo()) {
          // EOF reached
          continue;
        }

        do {
          Cell c = scanner.getKeyValue();
          byte[] value = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
          valuesAsList.add(HoodieAvroUtils.bytesToAvro(value, schema));
        } while (scanner.next());
      } catch (IOException e) {
        throw new HoodieException("Error reading hfile " + path + " as a dataframe", e);
      }
    }
    return valuesAsList.stream();
  }

}
