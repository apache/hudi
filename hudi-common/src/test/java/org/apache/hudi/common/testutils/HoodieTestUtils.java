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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.COMMIT_FORMATTER;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A utility class for testing.
 */
public class HoodieTestUtils {

  public static final String RAW_TRIPS_TEST_NAME = "raw_trips";
  public static final String DEFAULT_WRITE_TOKEN = "1-0-1";
  public static final int DEFAULT_LOG_VERSION = 1;
  public static final String[] DEFAULT_PARTITION_PATHS = {"2016/03/15", "2015/03/16", "2015/03/17"};
  private static Random rand = new Random(46474747);

  public static Configuration getDefaultHadoopConf() {
    return new Configuration();
  }

  public static HoodieTableMetaClient init(String basePath) throws IOException {
    return init(basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType) throws IOException {
    return init(getDefaultHadoopConf(), basePath, tableType);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath) throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.HOODIE_BOOTSTRAP_BASE_PATH, bootstrapBasePath);
    return init(getDefaultHadoopConf(), basePath, tableType, props);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieFileFormat baseFileFormat) throws IOException {
    return init(getDefaultHadoopConf(), basePath, HoodieTableType.COPY_ON_WRITE, baseFileFormat);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath) throws IOException {
    return init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType)
      throws IOException {
    return init(hadoopConf, basePath, tableType, new Properties());
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           String tableName)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
    return init(hadoopConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP_NAME, baseFileFormat.toString());
    return init(hadoopConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           Properties properties)
      throws IOException {
    properties.putIfAbsent(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
    properties.putIfAbsent(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
    properties.putIfAbsent(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
  }

  public static String makeNewCommitTime() {
    return COMMIT_FORMATTER.format(new Date());
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static void createCommitFiles(String basePath, String... instantTimes) throws IOException {
    for (String instantTime : instantTimes) {
      new File(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
              + HoodieTimeline.makeRequestedCommitFileName(instantTime)).createNewFile();
      new File(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
              + HoodieTimeline.makeInflightCommitFileName(instantTime)).createNewFile();
      new File(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCommitFileName(instantTime))
          .createNewFile();
    }
  }

  public static void createMetadataFolder(String basePath) {
    new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME).mkdirs();
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static void createInflightCommitFiles(String basePath, String... instantTimes) throws IOException {

    for (String instantTime : instantTimes) {
      new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
          + HoodieTimeline.makeRequestedCommitFileName(instantTime)).createNewFile();
      new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeInflightCommitFileName(
          instantTime)).createNewFile();
    }
  }

  public static void createPendingCleanFiles(HoodieTableMetaClient metaClient, String... instantTimes) {
    for (String instantTime : instantTimes) {
      Arrays.asList(HoodieTimeline.makeRequestedCleanerFileName(instantTime),
          HoodieTimeline.makeInflightCleanerFileName(instantTime))
          .forEach(f -> {
            FSDataOutputStream os = null;
            try {
              Path commitFile = new Path(Paths
                  .get(metaClient.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME, f).toString());
              os = metaClient.getFs().create(commitFile, true);
              // Write empty clean metadata
              os.write(TimelineMetadataUtils.serializeCleanerPlan(
                  new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "", new HashMap<>(),
                      CleanPlanV2MigrationHandler.VERSION, new HashMap<>())).get());
            } catch (IOException ioe) {
              throw new HoodieIOException(ioe.getMessage(), ioe);
            } finally {
              if (null != os) {
                try {
                  os.close();
                } catch (IOException e) {
                  throw new HoodieIOException(e.getMessage(), e);
                }
              }
            }
          });
    }
  }

  public static void createCorruptedPendingCleanFiles(HoodieTableMetaClient metaClient, String commitTime) {
    Arrays.asList(HoodieTimeline.makeRequestedCleanerFileName(commitTime),
        HoodieTimeline.makeInflightCleanerFileName(commitTime))
        .forEach(f -> {
          FSDataOutputStream os = null;
          try {
            Path commitFile = new Path(Paths
                .get(metaClient.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME, f).toString());
            os = metaClient.getFs().create(commitFile, true);
            // Write empty clean metadata
            os.write(new byte[0]);
          } catch (IOException ioe) {
            throw new HoodieIOException(ioe.getMessage(), ioe);
          } finally {
            if (null != os) {
              try {
                os.close();
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            }
          }
        });
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String createNewDataFile(String basePath, String partitionPath, String instantTime, long length)
      throws IOException {
    String fileID = UUID.randomUUID().toString();
    return createDataFileFixLength(basePath, partitionPath, instantTime, fileID, length);
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String createDataFile(String basePath, String partitionPath, String instantTime, String fileID)
      throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    new File(folderPath).mkdirs();
    new File(folderPath + FSUtils.makeDataFileName(instantTime, DEFAULT_WRITE_TOKEN, fileID)).createNewFile();
    return fileID;
  }

  private static String createDataFileFixLength(String basePath, String partitionPath, String instantTime, String fileID,
      long length) throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    Files.createDirectories(Paths.get(folderPath));
    String filePath = folderPath + FSUtils.makeDataFileName(instantTime, DEFAULT_WRITE_TOKEN, fileID);
    Files.createFile(Paths.get(filePath));
    try (FileChannel output = new FileOutputStream(new File(filePath)).getChannel()) {
      output.write(ByteBuffer.allocate(1), length - 1);
    }
    return fileID;
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String createNewLogFile(FileSystem fs, String basePath, String partitionPath, String instantTime,
      String fileID, Option<Integer> version) throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    boolean makeDir = fs.mkdirs(new Path(folderPath));
    if (!makeDir) {
      throw new IOException("cannot create directory for path " + folderPath);
    }
    boolean createFile = fs.createNewFile(new Path(folderPath + FSUtils.makeLogFileName(fileID, ".log", instantTime,
        version.orElse(DEFAULT_LOG_VERSION), HoodieLogFormat.UNKNOWN_WRITE_TOKEN)));
    if (!createFile) {
      throw new IOException(
          StringUtils.format("cannot create data file for commit %s and fileId %s", instantTime, fileID));
    }
    return fileID;
  }

  public static void createCompactionRequest(HoodieTableMetaClient metaClient, String instant,
      List<Pair<String, FileSlice>> fileSliceList) throws IOException {
    HoodieCompactionPlan plan = CompactionUtils.buildFromFileSlices(fileSliceList, Option.empty(), Option.empty());
    HoodieInstant compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instant);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        TimelineMetadataUtils.serializeCompactionPlan(plan));
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String getDataFilePath(String basePath, String partitionPath, String instantTime, String fileID) {
    return basePath + "/" + partitionPath + "/" + FSUtils.makeDataFileName(instantTime, DEFAULT_WRITE_TOKEN, fileID);
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String getLogFilePath(String basePath, String partitionPath, String instantTime, String fileID,
      Option<Integer> version) {
    return basePath + "/" + partitionPath + "/" + FSUtils.makeLogFileName(fileID, ".log", instantTime,
        version.orElse(DEFAULT_LOG_VERSION), HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
  }

  public static String getCommitFilePath(String basePath, String instantTime) {
    return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + instantTime + HoodieTimeline.COMMIT_EXTENSION;
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String getInflightCommitFilePath(String basePath, String instantTime) {
    return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + instantTime
        + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION;
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static String getRequestedCompactionFilePath(String basePath, String instantTime) {
    return basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + instantTime
        + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION;
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static boolean doesDataFileExist(String basePath, String partitionPath, String instantTime,
      String fileID) {
    return new File(getDataFilePath(basePath, partitionPath, instantTime, fileID)).exists();
  }

  public static boolean doesCommitExist(String basePath, String instantTime) {
    return new File(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + instantTime + HoodieTimeline.COMMIT_EXTENSION)
            .exists();
  }

  /**
   * @deprecated Use {@link HoodieTestTable} instead.
   */
  public static boolean doesInflightExist(String basePath, String instantTime) {
    return new File(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + instantTime + HoodieTimeline.INFLIGHT_EXTENSION)
            .exists();
  }

  public static void createCleanFiles(HoodieTableMetaClient metaClient, String basePath,
      String instantTime, Configuration configuration)
      throws IOException {
    createPendingCleanFiles(metaClient, instantTime);
    Path commitFile = new Path(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCleanerFileName(instantTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    try (FSDataOutputStream os = fs.create(commitFile, true)) {
      HoodieCleanStat cleanStats = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          DEFAULT_PARTITION_PATHS[rand.nextInt(DEFAULT_PARTITION_PATHS.length)], new ArrayList<>(), new ArrayList<>(),
          new ArrayList<>(), instantTime);
      // Create the clean metadata

      HoodieCleanMetadata cleanMetadata =
          CleanerUtils.convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats));
      // Write empty clean metadata
      os.write(TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata).get());
    }
  }

  public static <T extends Serializable> T serializeDeserialize(T object, Class<T> clazz) {
    // Using Kyro as the default serializer in Spark Jobs
    Kryo kryo = new Kryo();
    kryo.register(HoodieTableMetaClient.class, new JavaSerializer());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, object);
    output.close();

    Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
    T deseralizedObject = kryo.readObject(input, clazz);
    input.close();
    return deseralizedObject;
  }

  public static void writeRecordsToLogFiles(FileSystem fs, String basePath, Schema schema,
      List<HoodieRecord> updatedRecords) {
    Map<HoodieRecordLocation, List<HoodieRecord>> groupedUpdated =
        updatedRecords.stream().collect(Collectors.groupingBy(HoodieRecord::getCurrentLocation));

    groupedUpdated.forEach((location, value) -> {
      String partitionPath = value.get(0).getPartitionPath();

      Writer logWriter;
      try {
        logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(basePath, partitionPath))
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
          .overBaseCommit(location.getInstantTime()).withFs(fs).build();

        Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
        header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, location.getInstantTime());
        header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
        logWriter.appendBlock(new HoodieAvroDataBlock(value.stream().map(r -> {
          try {
            GenericRecord val = (GenericRecord) r.getData().getInsertValue(schema).get();
            HoodieAvroUtils.addHoodieKeyToRecord(val, r.getRecordKey(), r.getPartitionPath(), "");
            return (IndexedRecord) val;
          } catch (IOException e) {
            return null;
          }
        }).collect(Collectors.toList()), header));
        logWriter.close();
      } catch (Exception e) {
        fail(e.toString());
      }
    });
  }

  // TODO: should be removed
  public static FileStatus[] listAllDataFilesInPath(FileSystem fs, String basePath) throws IOException {
    return listAllDataFilesInPath(fs, basePath, HoodieFileFormat.PARQUET.getFileExtension());
  }

  public static FileStatus[] listAllDataFilesInPath(FileSystem fs, String basePath, String datafileExtension)
      throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(basePath), true);
    List<FileStatus> returns = new ArrayList<>();
    while (itr.hasNext()) {
      LocatedFileStatus status = itr.next();
      if (status.getPath().getName().contains(datafileExtension) && !status.getPath().getName().contains(".marker")) {
        returns.add(status);
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  public static FileStatus[] listAllLogFilesInPath(FileSystem fs, String basePath)
      throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(basePath), true);
    List<FileStatus> returns = new ArrayList<>();
    while (itr.hasNext()) {
      LocatedFileStatus status = itr.next();
      if (status.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
        returns.add(status);
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  public static FileStatus[] listAllDataFilesAndLogFilesInPath(FileSystem fs, String basePath) throws IOException {
    return Stream.concat(Arrays.stream(listAllDataFilesInPath(fs, basePath)), Arrays.stream(listAllLogFilesInPath(fs, basePath)))
            .toArray(FileStatus[]::new);
  }

  public static List<String> monotonicIncreasingCommitTimestamps(int numTimestamps, int startSecsDelta) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.SECOND, startSecsDelta);
    List<String> commits = new ArrayList<>();
    for (int i = 0; i < numTimestamps; i++) {
      commits.add(COMMIT_FORMATTER.format(cal.getTime()));
      cal.add(Calendar.SECOND, 1);
    }
    return commits;
  }

  public static List<HoodieWriteStat> generateFakeHoodieWriteStat(int limit) {
    List<HoodieWriteStat> writeStatList = new ArrayList<>();
    for (int i = 0; i < limit; i++) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(UUID.randomUUID().toString());
      writeStat.setNumDeletes(0);
      writeStat.setNumUpdateWrites(100);
      writeStat.setNumWrites(100);
      writeStat.setPath("/some/fake/path" + i);
      writeStat.setPartitionPath("/some/fake/partition/path" + i);
      writeStat.setTotalLogFilesCompacted(100L);
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalScanTime(100);
      runtimeStats.setTotalCreateTime(100);
      runtimeStats.setTotalUpsertTime(100);
      writeStat.setRuntimeStats(runtimeStats);
      writeStatList.add(writeStat);
    }
    return writeStatList;
  }
}
