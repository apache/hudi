/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.model.HoodieWriteStat.RuntimeStats;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
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
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;

public class HoodieTestUtils {

  public static final String TEST_EXTENSION = ".test";
  public static final String RAW_TRIPS_TEST_NAME = "raw_trips";
  public static final int DEFAULT_TASK_PARTITIONID = 1;
  public static final String[] DEFAULT_PARTITION_PATHS = {"2016/03/15", "2015/03/16", "2015/03/17"};
  private static Random rand = new Random(46474747);

  public static Configuration getDefaultHadoopConf() {
    return new Configuration();
  }

  public static HoodieTableMetaClient init(String basePath)
      throws IOException {
    return initTableType(getDefaultHadoopConf(), basePath, HoodieTableType.COPY_ON_WRITE);
  }
  
  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath)
      throws IOException {
    return initTableType(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient initTableType(Configuration hadoopConf, String basePath,
      HoodieTableType tableType) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
    properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
    return HoodieTableMetaClient.initializePathAsHoodieDataset(hadoopConf, basePath, properties);
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

  public static final void createInflightCommitFiles(String basePath, String... commitTimes) throws IOException {
    for (String commitTime : commitTimes) {
      new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeInflightCommitFileName(
          commitTime)).createNewFile();
    }
  }

  public static final void createInflightCleanFiles(String basePath, Configuration configuration, String... commitTimes)
      throws IOException {
    for (String commitTime : commitTimes) {
      Path commitFile = new Path((basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline
          .makeInflightCleanerFileName(
              commitTime)));
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

  public static final String createDataFile(String basePath, String partitionPath, String commitTime, String fileID)
      throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    new File(folderPath).mkdirs();
    new File(folderPath + FSUtils.makeDataFileName(commitTime, DEFAULT_TASK_PARTITIONID, fileID)).createNewFile();
    return fileID;
  }

  public static final String createNewLogFile(FileSystem fs, String basePath, String partitionPath, String commitTime,
      String fileID, Optional<Integer> version) throws IOException {
    String folderPath = basePath + "/" + partitionPath + "/";
    boolean makeDir = fs.mkdirs(new Path(folderPath));
    if (!makeDir) {
      throw new IOException("cannot create directory for path " + folderPath);
    }
    boolean createFile = fs.createNewFile(new Path(
        folderPath + FSUtils.makeLogFileName(fileID, ".log", commitTime, version.orElse(DEFAULT_TASK_PARTITIONID))));
    if (!createFile) {
      throw new IOException(
          StringUtils.format("cannot create data file for commit %s and fileId %s", commitTime, fileID));
    }
    return fileID;
  }

  public static final void createCompactionCommitFiles(FileSystem fs, String basePath, String... commitTimes)
      throws IOException {
    for (String commitTime : commitTimes) {
      boolean createFile = fs.createNewFile(new Path(
          basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline
              .makeCommitFileName(commitTime)));
      if (!createFile) {
        throw new IOException("cannot create commit file for commit " + commitTime);
      }
    }
  }

  public static final void createCompactionRequest(HoodieTableMetaClient metaClient, String instant,
      List<Pair<String, FileSlice>> fileSliceList) throws IOException {
    HoodieCompactionPlan plan = CompactionUtils.buildFromFileSlices(fileSliceList, Optional.empty(), Optional.empty());
    HoodieInstant compactionInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instant);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        AvroUtils.serializeCompactionPlan(plan));
  }

  public static final String getDataFilePath(String basePath, String partitionPath, String commitTime, String fileID)
      throws IOException {
    return basePath + "/" + partitionPath + "/" + FSUtils
        .makeDataFileName(commitTime, DEFAULT_TASK_PARTITIONID, fileID);
  }

  public static final String getLogFilePath(String basePath, String partitionPath, String commitTime, String fileID,
      Optional<Integer> version) throws IOException {
    return basePath + "/" + partitionPath + "/" + FSUtils.makeLogFileName(fileID, ".log", commitTime,
        version.orElse(DEFAULT_TASK_PARTITIONID));
  }

  public static final String getCommitFilePath(String basePath, String commitTime) throws IOException {
    return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION;
  }

  public static final boolean doesDataFileExist(String basePath, String partitionPath, String commitTime, String fileID)
      throws IOException {
    return new File(getDataFilePath(basePath, partitionPath, commitTime, fileID)).exists();
  }

  public static final boolean doesLogFileExist(String basePath, String partitionPath, String commitTime, String fileID,
      Optional<Integer> version) throws IOException {
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

  public static String makeInflightTestFileName(String instant) {
    return instant + TEST_EXTENSION + HoodieTimeline.INFLIGHT_EXTENSION;
  }

  public static void createCleanFiles(String basePath, String commitTime, Configuration configuration)
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
      HoodieCleanMetadata cleanMetadata = AvroUtils.convertCleanMetadata(commitTime, Optional.of(0L),
          Arrays.asList(cleanStats));
      // Write empty clean metadata
      os.write(AvroUtils.serializeCleanMetadata(cleanMetadata).get());
    } finally {
      os.close();
    }
  }

  public static void createCleanFiles(String basePath, String commitTime) throws IOException {
    createCleanFiles(basePath, commitTime, HoodieTestUtils.getDefaultHadoopConf());
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
    Map<HoodieRecordLocation, List<HoodieRecord>> groupedUpdated = updatedRecords.stream().collect(
        Collectors.groupingBy(HoodieRecord::getCurrentLocation));

    groupedUpdated.entrySet().forEach(s -> {
      HoodieRecordLocation location = s.getKey();
      String partitionPath = s.getValue().get(0).getPartitionPath();

      Writer logWriter;
      try {
        logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(basePath, partitionPath))
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
            .overBaseCommit(location.getCommitTime()).withFs(fs).build();

        Map<HoodieLogBlock.HeaderMetadataType, String> header = Maps.newHashMap();
        header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, location.getCommitTime());
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
