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

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.GenericRecordValidationTestUtils.readHFile;

/**
 * Utility methods to aid testing inside the HoodieClient module.
 */
public class HoodieClientTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieClientTestUtils.class);

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
        .setMaster("local[8]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.default.parallelism", "4");

    // NOTE: This utility is used in modules where this class might not be present, therefore
    //       to avoid littering output w/ [[ClassNotFoundException]]s we will skip adding it
    //       in case this utility is used in the module not providing it
    if (canLoadClass("org.apache.spark.sql.hudi.HoodieSparkSessionExtension")) {
      sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    }

    if (canLoadClass("org.apache.spark.sql.hudi.catalog.HoodieCatalog") && HoodieSparkUtils.gteqSpark3_2()) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.hudi.catalog.HoodieCatalog");
    }

    String evlogDir = System.getProperty("SPARK_EVLOG_DIR");
    if (evlogDir != null) {
      sparkConf.set("spark.eventLog.enabled", "true");
      sparkConf.set("spark.eventLog.dir", evlogDir);
      sparkConf.set("spark.ui.enabled", "true");
    } else {
      sparkConf.set("spark.ui.enabled", "false");
    }

    return SparkRDDReadClient.addHoodieSupport(sparkConf);
  }
  
  public static void overrideSparkHadoopConfiguration(SparkContext sparkContext) {
    try {
      // Clean the default Hadoop configurations since in our Hudi tests they are not used.
      Field hadoopConfigurationField = sparkContext.getClass().getDeclaredField("_hadoopConfiguration");
      hadoopConfigurationField.setAccessible(true);
      Configuration testHadoopConfig = new Configuration(false);
      hadoopConfigurationField.set(sparkContext, testHadoopConfig);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOG.warn(e.getMessage());
    }
  }

  private static HashMap<String, String> getLatestFileIDsToFullPath(String basePath, HoodieTimeline commitTimeline,
                                                                    List<HoodieInstant> commitsToReturn) throws IOException {
    HashMap<String, String> fileIdToFullPath = new HashMap<>();
    for (HoodieInstant commit : commitsToReturn) {
      HoodieCommitMetadata metadata =
          HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get(), HoodieCommitMetadata.class);
      fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths(new StoragePath(basePath)));
    }
    return fileIdToFullPath;
  }

  public static Dataset<Row> readCommit(String basePath, SQLContext sqlContext, HoodieTimeline commitTimeline,
                                        String instantTime) {
    return readCommit(basePath, sqlContext, commitTimeline, instantTime, true);
  }

  public static Dataset<Row> readCommit(String basePath, SQLContext sqlContext, HoodieTimeline commitTimeline,
                                        String instantTime, boolean filterByCommitTime) {
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime);
    if (!commitTimeline.containsInstant(commitInstant)) {
      throw new HoodieException("No commit exists at " + instantTime);
    }
    try {
      HashMap<String, String> paths =
          getLatestFileIDsToFullPath(basePath, commitTimeline, Arrays.asList(commitInstant));
      LOG.info("Path :" + paths.values());
      Dataset<Row> unFilteredRows = null;
      if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.PARQUET)) {
        unFilteredRows = sqlContext.read().parquet(paths.values().toArray(new String[paths.size()]));
      } else if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.ORC)) {
        unFilteredRows = sqlContext.read().orc(paths.values().toArray(new String[paths.size()]));
      }
      if (unFilteredRows != null) {
        if (filterByCommitTime) {
          return unFilteredRows.filter(String.format("%s ='%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime));
        } else {
          return unFilteredRows;
        }
      } else {
        return sqlContext.emptyDataFrame();
      }
    } catch (Exception e) {
      throw new HoodieException("Error reading commit " + instantTime, e);
    }
  }

  /**
   * Obtain all new data written into the Hoodie table with an optional from timestamp.
   */
  public static long countRecordsOptionallySince(JavaSparkContext jsc, String basePath, SQLContext sqlContext,
                                                 HoodieTimeline commitTimeline, Option<String> lastCommitTimeOpt) {
    List<HoodieInstant> commitsToReturn =
        lastCommitTimeOpt.isPresent() ? commitTimeline.findInstantsAfter(lastCommitTimeOpt.get(), Integer.MAX_VALUE).getInstants() :
            commitTimeline.getInstants();
    try {
      // Go over the commit metadata, and obtain the new files that need to be read.
      HashMap<String, String> fileIdToFullPath = getLatestFileIDsToFullPath(basePath, commitTimeline, commitsToReturn);
      String[] paths = fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]);
      if (paths[0].endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        Dataset<Row> rows = sqlContext.read().parquet(paths);
        if (lastCommitTimeOpt.isPresent()) {
          return rows.filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTimeOpt.get()))
              .count();
        } else {
          return rows.count();
        }
      } else if (paths[0].endsWith(HoodieFileFormat.HFILE.getFileExtension())) {
        Stream<GenericRecord> genericRecordStream = readHFile(jsc.hadoopConfiguration(), paths);
        if (lastCommitTimeOpt.isPresent()) {
          return genericRecordStream.filter(gr -> HoodieTimeline.compareTimestamps(lastCommitTimeOpt.get(), HoodieActiveTimeline.LESSER_THAN,
              gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()))
              .count();
        } else {
          return genericRecordStream.count();
        }
      } else if (paths[0].endsWith(HoodieFileFormat.ORC.getFileExtension())) {
        Dataset<Row> rows = sqlContext.read().orc(paths);
        if (lastCommitTimeOpt.isPresent()) {
          return rows.filter(String.format("%s >'%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, lastCommitTimeOpt.get()))
              .count();
        } else {
          return rows.count();
        }
      }
      throw new HoodieException("Unsupported base file format for file :" + paths[0]);
    } catch (IOException e) {
      throw new HoodieException(
          "Error pulling data incrementally from commitTimestamp :" + lastCommitTimeOpt.get(), e);
    }
  }

  public static List<HoodieBaseFile> getLatestBaseFiles(String basePath,
                                                        HoodieStorage storage,
                                                        String... paths) {
    List<HoodieBaseFile> latestFiles = new ArrayList<>();
    try {
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, basePath);
      for (String path : paths) {
        BaseFileOnlyView fileSystemView = new HoodieTableFileSystemView(
            metaClient,
            metaClient.getCommitsTimeline().filterCompletedInstants(),
            storage.globEntries(new StoragePath(path)));
        latestFiles.addAll(fileSystemView.getLatestBaseFiles().collect(Collectors.toList()));
      }
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
    return latestFiles;
  }

  /**
   * Reads the paths under the hoodie table out as a DataFrame.
   */
  public static Dataset<Row> read(JavaSparkContext jsc, String basePath, SQLContext sqlContext,
                                  HoodieStorage storage,
                                  String... paths) {
    List<String> filteredPaths = new ArrayList<>();
    try {
      List<HoodieBaseFile> latestFiles = getLatestBaseFiles(basePath, storage, paths);
      for (HoodieBaseFile file : latestFiles) {
        filteredPaths.add(file.getPath());
      }
      if (filteredPaths.isEmpty()) {
        return sqlContext.emptyDataFrame();
      }
      String[] filteredPathsToRead = filteredPaths.toArray(new String[filteredPaths.size()]);
      if (filteredPathsToRead[0].endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        return sqlContext.read().parquet(filteredPathsToRead);
      } else if (filteredPathsToRead[0].endsWith(HoodieFileFormat.ORC.getFileExtension())) {
        return sqlContext.read().orc(filteredPathsToRead);
      }
      return sqlContext.emptyDataFrame();
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }

  /**
   * Initializes timeline service based on the write config.
   *
   * @param context             {@link HoodieEngineContext} instance to use.
   * @param basePath            Base path of the table.
   * @param timelineServicePort Port number to use for timeline service.
   * @return started {@link TimelineService} instance.
   */
  public static TimelineService initTimelineService(
      HoodieEngineContext context, String basePath, int timelineServicePort) {
    try {
      HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withRemoteServerPort(timelineServicePort).build())
          .build();
      TimelineService timelineService = new TimelineService(context, new Configuration(),
          TimelineService.Config.builder().enableMarkerRequests(true)
              .serverPort(config.getViewStorageConfig().getRemoteViewServerPort()).build(),
          HoodieStorageUtils.getStorage(HoodieTestUtils.getDefaultStorageConf()),
          FileSystemViewManager.createViewManager(context, config.getViewStorageConfig(), config.getCommonConfig()));
      timelineService.startService();
      LOG.info("Timeline service server port: " + timelineServicePort);
      return timelineService;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Option<HoodieCommitMetadata> getCommitMetadataForLatestInstant(HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    if (timeline.lastInstant().isPresent()) {
      return getCommitMetadataForInstant(metaClient, timeline.lastInstant().get());
    } else {
      return Option.empty();
    }
  }

  /**
   * @param jsc      {@link JavaSparkContext} instance.
   * @param basePath base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance.
   */
  public static HoodieTableMetaClient createMetaClient(JavaSparkContext jsc, String basePath) {
    return HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(jsc.hadoopConfiguration()), basePath);
  }

  /**
   * @param spark    {@link SparkSession} instance.
   * @param basePath base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance.
   */
  public static HoodieTableMetaClient createMetaClient(SparkSession spark, String basePath) {
    return HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(spark.sessionState().newHadoopConf()), basePath);
  }

  private static Option<HoodieCommitMetadata> getCommitMetadataForInstant(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      byte[] data = timeline.getInstantDetails(instant).get();
      return Option.of(HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class));
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }

  private static boolean canLoadClass(String className) {
    try {
      return ReflectionUtils.getClass(className) != null;
    } catch (Exception e) {
      return false;
    }
  }
}
