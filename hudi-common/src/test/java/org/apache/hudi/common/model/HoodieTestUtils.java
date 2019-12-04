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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A utility class for testing.
 */
public class HoodieTestUtils {

  public static final String TEST_EXTENSION = ".test";
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

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath) throws IOException {
    return init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
    properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
    return HoodieTableMetaClient.initDatasetAndGetMetaClient(hadoopConf, basePath, properties);
  }

  public static String makeNewCommitTime() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
  }

  public static final void createCommitFiles(String basePath, String... commitTimes) throws IOException {
    for (String commitTime : commitTimes) {
      new File(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCommitFileName(commitTime))
              .createNewFile();
    }
  }

  public static final void createDeltaCommitFiles(String basePath, String... commitTimes) throws IOException {
    for (String commitTime : commitTimes) {
      new File(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeDeltaFileName(commitTime))
              .createNewFile();
    }
  }

  public static final void createMetadataFolder(String basePath) throws IOException {
    new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME).mkdirs();
  }

  public static final void createInflightCommitFiles(String basePath, String... commitTimes) throws IOException {
    for (String commitTime : commitTimes) {
      new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
          + HoodieTimeline.makeInflightCommitFileName(commitTime)).createNewFile();
    }
  }

  public static final void createInflightCleanFiles(String basePath, Configuration configuration, String... commitTimes)
      throws IOException {
    for (String commitTime : commitTimes) {
      Path commitFile = new Path((basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
          + HoodieTimeline.makeInflightCleanerFileName(commitTime)));
      FileSystem fs = FSUtils.getFs(basePath, configuration);
      FSDataOutputStream os = fs.create(commitFile, true);
    }
  }

  public static final void createInflightCleanFiles(String basePath, String... commitTimes) throws IOException {
    createInflightCleanFiles(basePath, HoodieTestUtils.getDefaultHadoopConf(), commitTimes);
  }

  public static final String createNewDataFile(String basePath, String partitionPath, String commitTime)
      throws IOException {
    String fileID = UUID.randomUUID().toString();
    return createDataFile(basePath, partitionPath, commitTime, fileID);
  }

  public static final String createNewMarkerFile(String basePath, String partitionPath, String commitTime)
      throws IOException {
    String fileID = UUID.randomUUID().toString();
    return createMarkerFile(basePath, partitionPath, commitTime, fileID);
  }

  public static final String createDataFile(String basePath, String partitionPath, String commitTime, String fileID)
      throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    new File(folderPath).mkdirs();
    new File(folderPath + FSUtils.makeDataFileName(commitTime, DEFAULT_WRITE_TOKEN, fileID)).createNewFile();
    return fileID;
  }

  public static final String createMarkerFile(String basePath, String partitionPath, String commitTime, String fileID)
      throws IOException {
    String folderPath =
        basePath + "/" + HoodieTableMetaClient.TEMPFOLDER_NAME + "/" + commitTime + "/" + partitionPath + "/";
    new File(folderPath).mkdirs();
    File f = new File(folderPath + FSUtils.makeMarkerFile(commitTime, DEFAULT_WRITE_TOKEN, fileID));
    f.createNewFile();
    return f.getAbsolutePath();
  }

  public static final String createNewLogFile(FileSystem fs, String basePath, String partitionPath, String commitTime,
      String fileID, Option<Integer> version) throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    boolean makeDir = fs.mkdirs(new Path(folderPath));
    if (!makeDir) {
      throw new IOException("cannot create directory for path " + folderPath);
    }
    boolean createFile = fs.createNewFile(new Path(folderPath + FSUtils.makeLogFileName(fileID, ".log", commitTime,
        version.orElse(DEFAULT_LOG_VERSION), HoodieLogFormat.UNKNOWN_WRITE_TOKEN)));
    if (!createFile) {
      throw new IOException(
          StringUtils.format("cannot create data file for commit %s and fileId %s", commitTime, fileID));
    }
    return fileID;
  }

  public static final void createCompactionCommitFiles(FileSystem fs, String basePath, String... commitTimes)
      throws IOException {
    for (String commitTime : commitTimes) {
      boolean createFile = fs.createNewFile(new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
          + HoodieTimeline.makeCommitFileName(commitTime)));
      if (!createFile) {
        throw new IOException("cannot create commit file for commit " + commitTime);
      }
    }
  }

  public static final void createCompactionRequest(HoodieTableMetaClient metaClient, String instant,
      List<Pair<String, FileSlice>> fileSliceList) throws IOException {
    HoodieCompactionPlan plan = CompactionUtils.buildFromFileSlices(fileSliceList, Option.empty(), Option.empty());
    HoodieInstant compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instant);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        AvroUtils.serializeCompactionPlan(plan));
  }

  public static final String getDataFilePath(String basePath, String partitionPath, String commitTime, String fileID) {
    return basePath + "/" + partitionPath + "/" + FSUtils.makeDataFileName(commitTime, DEFAULT_WRITE_TOKEN, fileID);
  }

  public static final String getLogFilePath(String basePath, String partitionPath, String commitTime, String fileID,
      Option<Integer> version) {
    return basePath + "/" + partitionPath + "/" + FSUtils.makeLogFileName(fileID, ".log", commitTime,
        version.orElse(DEFAULT_LOG_VERSION), HoodieLogFormat.UNKNOWN_WRITE_TOKEN);
  }

  public static final String getCommitFilePath(String basePath, String commitTime) {
    return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION;
  }

  public static final String getInflightCommitFilePath(String basePath, String commitTime) {
    return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime
        + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION;
  }

  public static final String getRequestedCompactionFilePath(String basePath, String commitTime) {
    return basePath + "/" + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/" + commitTime
        + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION;
  }

  public static final boolean doesDataFileExist(String basePath, String partitionPath, String commitTime,
      String fileID) {
    return new File(getDataFilePath(basePath, partitionPath, commitTime, fileID)).exists();
  }

  public static final boolean doesLogFileExist(String basePath, String partitionPath, String commitTime, String fileID,
      Option<Integer> version) {
    return new File(getLogFilePath(basePath, partitionPath, commitTime, fileID, version)).exists();
  }

  public static final boolean doesCommitExist(String basePath, String commitTime) {
    return new File(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION)
            .exists();
  }

  public static final boolean doesInflightExist(String basePath, String commitTime) {
    return new File(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + HoodieTimeline.INFLIGHT_EXTENSION)
            .exists();
  }

  public static void createCleanFiles(HoodieTableMetaClient metaClient, String basePath,
      String commitTime, Configuration configuration)
      throws IOException {
    Path commitFile = new Path(
        basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCleanerFileName(commitTime));
    FileSystem fs = FSUtils.getFs(basePath, configuration);
    FSDataOutputStream os = fs.create(commitFile, true);
    try {
      HoodieCleanStat cleanStats = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
          DEFAULT_PARTITION_PATHS[rand.nextInt(DEFAULT_PARTITION_PATHS.length)], new ArrayList<>(), new ArrayList<>(),
          new ArrayList<>(), commitTime);
      // Create the clean metadata

      HoodieCleanMetadata cleanMetadata =
          CleanerUtils.convertCleanMetadata(metaClient, commitTime, Option.of(0L), Arrays.asList(cleanStats));
      // Write empty clean metadata
      os.write(AvroUtils.serializeCleanMetadata(cleanMetadata).get());
    } finally {
      os.close();
    }
  }

  public static void createCleanFiles(HoodieTableMetaClient metaClient,
      String basePath, String commitTime) throws IOException {
    createCleanFiles(metaClient, basePath, commitTime, HoodieTestUtils.getDefaultHadoopConf());
  }

  public static String makeTestFileName(String instant) {
    return instant + TEST_EXTENSION;
  }

  public static String makeCommitFileName(String instant) {
    return instant + ".commit";
  }

  public static void assertStreamEquals(String message, Stream<?> expected, Stream<?> actual) {
    Iterator<?> iter1 = expected.iterator();
    Iterator<?> iter2 = actual.iterator();
    while (iter1.hasNext() && iter2.hasNext()) {
      assertEquals(message, iter1.next(), iter2.next());
    }
    assert !iter1.hasNext() && !iter2.hasNext();
  }

  public static <T extends Serializable> T serializeDeserialize(T object, Class<T> clazz)
      throws IOException, ClassNotFoundException {
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

    groupedUpdated.entrySet().forEach(s -> {
      HoodieRecordLocation location = s.getKey();
      String partitionPath = s.getValue().get(0).getPartitionPath();

      Writer logWriter;
      try {
        logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(basePath, partitionPath))
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
            .overBaseCommit(location.getInstantTime()).withFs(fs).build();

        Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
        header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, location.getInstantTime());
        header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
        logWriter.appendBlock(new HoodieAvroDataBlock(s.getValue().stream().map(r -> {
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

  public static FileStatus[] listAllDataFilesInPath(FileSystem fs, String basePath) throws IOException {
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(basePath), true);
    List<FileStatus> returns = Lists.newArrayList();
    while (itr.hasNext()) {
      LocatedFileStatus status = itr.next();
      if (status.getPath().getName().contains(".parquet")) {
        returns.add(status);
      }
    }
    return returns.toArray(new FileStatus[returns.size()]);
  }

  public static List<String> monotonicIncreasingCommitTimestamps(int numTimestamps, int startSecsDelta) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.SECOND, startSecsDelta);
    List<String> commits = new ArrayList<>();
    for (int i = 0; i < numTimestamps; i++) {
      commits.add(HoodieActiveTimeline.COMMIT_FORMATTER.format(cal.getTime()));
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
