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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Lists;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HoodieTestUtils {
    public static FileSystem fs = FSUtils.getFs();
    public static final String TEST_EXTENSION = ".test";
    public static final String RAW_TRIPS_TEST_NAME = "raw_trips";
    public static final int DEFAULT_TASK_PARTITIONID = 1;

    public static HoodieTableMetaClient init(String basePath) throws IOException {
        return initTableType(basePath, HoodieTableType.COPY_ON_WRITE);
    }

    public static HoodieTableMetaClient initTableType(String basePath, HoodieTableType tableType) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
        return HoodieTableMetaClient.initializePathAsHoodieDataset(fs, basePath, properties);
    }

    public static HoodieTableMetaClient initOnTemp() throws IOException {
        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        String basePath = folder.getRoot().getAbsolutePath();
        return HoodieTestUtils.init(basePath);
    }

    public static String makeNewCommitTime() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }

    public static final void createCommitFiles(String basePath, String... commitTimes) throws IOException {
        for (String commitTime: commitTimes) {
            boolean createFile = fs.createNewFile(new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + HoodieTimeline.makeCommitFileName(commitTime)));
            if(!createFile) {
                throw new IOException("cannot create commit file for commit " + commitTime);
            }
        }
    }

    public static final void createInflightCommitFiles(String basePath, String... commitTimes) throws IOException {
        for (String commitTime: commitTimes) {
            boolean createFile = fs.createNewFile(new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + HoodieTimeline.makeInflightCommitFileName(commitTime)));
            if(!createFile) {
                throw new IOException("cannot create inflight commit file for commit " + commitTime);
            }
        }
    }

    public static final String createNewDataFile(String basePath, String partitionPath, String commitTime) throws IOException {
        String fileID = UUID.randomUUID().toString();
        return createDataFile(basePath, partitionPath, commitTime, fileID);
    }

    public static final String createDataFile(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        String folderPath = basePath + "/" + partitionPath + "/";
        boolean makeDir = fs.mkdirs(new Path(folderPath));
        if(!makeDir) {
            throw new IOException("cannot create directory for path " + folderPath);
        }
        boolean createFile = fs.createNewFile(new Path(folderPath + FSUtils.makeDataFileName(commitTime, DEFAULT_TASK_PARTITIONID, fileID)));
        if(!createFile) {
            throw new IOException(StringUtils.format("cannot create data file for commit %s and fileId %s", commitTime, fileID));
        }
        return fileID;
    }

    public static final String getDataFilePath(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        return basePath + "/" + partitionPath + "/" + FSUtils.makeDataFileName(commitTime, DEFAULT_TASK_PARTITIONID, fileID);
    }

    public static final String getCommitFilePath(String basePath, String commitTime) throws IOException {
        return basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION;
    }

    public static final boolean doesDataFileExist(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        return fs.exists(new Path(getDataFilePath(basePath, partitionPath, commitTime, fileID)));
    }

    public static final boolean doesCommitExist(String basePath, String commitTime) throws IOException {
        return fs.exists(new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION));
    }

    public static final boolean doesInflightExist(String basePath, String commitTime) throws IOException {
        return fs.exists(new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + commitTime + HoodieTimeline.INFLIGHT_EXTENSION));
    }

    public static String makeInflightTestFileName(String instant) {
        return instant + TEST_EXTENSION + HoodieTimeline.INFLIGHT_EXTENSION;
    }

    public static String makeTestFileName(String instant) {
        return instant + TEST_EXTENSION;
    }

    public static String makeCommitFileName(String instant) {
        return instant + ".commit";
    }

    public static void assertStreamEquals(String message, Stream<?> expected, Stream<?> actual) {
        Iterator<?> iter1 = expected.iterator(), iter2 = actual.iterator();
        while(iter1.hasNext() && iter2.hasNext())
            assertEquals(message, iter1.next(), iter2.next());
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

    public static void writeRecordsToLogFiles(String basePath, Schema schema, List<HoodieRecord> updatedRecords) {
        Map<HoodieRecordLocation, List<HoodieRecord>> groupedUpdated = updatedRecords.stream()
                .collect(Collectors.groupingBy(HoodieRecord::getCurrentLocation));

        groupedUpdated.entrySet().forEach(s -> {
            HoodieRecordLocation location = s.getKey();
            String partitionPath = s.getValue().get(0).getPartitionPath();

            Writer logWriter;
            try {
                logWriter = HoodieLogFormat.newWriterBuilder()
                        .onParentPath(new Path(basePath, partitionPath))
                        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
                        .withFileId(location.getFileId())
                        .overBaseCommit(location.getCommitTime())
                        .withFs(fs).build();

                logWriter.appendBlock(new HoodieAvroDataBlock(s.getValue().stream().map(r -> {
                    try {
                        GenericRecord val = (GenericRecord) r.getData().getInsertValue(schema).get();
                        HoodieAvroUtils.addHoodieKeyToRecord(val,
                                r.getRecordKey(),
                                r.getPartitionPath(),
                                "");
                        return (IndexedRecord) val;
                    } catch (IOException e) {
                        return null;
                    }
                }).collect(Collectors.toList()), schema));
                logWriter.close();
            } catch (Exception e) {
                fail(e.toString());
            }
        });
    }

    public static FileStatus[] listAllDataFilesInPath(FileSystem fs, String basePath)
            throws IOException {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(basePath), true);
        List<FileStatus> returns = Lists.newArrayList();
        while(itr.hasNext()) {
            LocatedFileStatus status = itr.next();
            if(status.getPath().getName().contains(".parquet")) {
                returns.add(status);
            }
        }
        return returns.toArray(new FileStatus[returns.size()]);
    }
}