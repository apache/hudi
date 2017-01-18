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
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;

import org.apache.hadoop.fs.FileSystem;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class HoodieTestUtils {
    public static FileSystem fs = FSUtils.getFs();
    public static final String TEST_EXTENSION = ".test";
    public static final String RAW_TRIPS_TEST_NAME = "raw_trips";

    public static HoodieTableMetaClient init(String basePath) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
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
            new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + HoodieTimeline.makeCommitFileName(commitTime)).createNewFile();
        }
    }

    public static final void createInflightCommitFiles(String basePath, String... commitTimes) throws IOException {
        for (String commitTime: commitTimes) {
            new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + HoodieTimeline.makeInflightCommitFileName(commitTime)).createNewFile();
        }
    }

    public static final String createNewDataFile(String basePath, String partitionPath, String commitTime) throws IOException {
        String fileID = UUID.randomUUID().toString();
        return createDataFile(basePath, partitionPath, commitTime, fileID);
    }

    public static final String createDataFile(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        String folderPath = basePath + "/" + partitionPath + "/";
        new File(folderPath).mkdirs();
        new File(folderPath + FSUtils.makeDataFileName(commitTime, 1, fileID)).createNewFile();
        return fileID;
    }

    public static final boolean doesDataFileExist(String basePath, String partitionPath, String commitTime, String fileID) throws IOException {
        return new File(basePath + "/" + partitionPath + "/" + FSUtils.makeDataFileName(commitTime, 1, fileID)).exists();
    }

    public static final boolean doesCommitExist(String basePath, String commitTime) {
        return new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + commitTime + HoodieTimeline.COMMIT_EXTENSION).exists();
    }

    public static final boolean doesInflightExist(String basePath, String commitTime) {
        return new File(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME+ "/" + commitTime + HoodieTimeline.INFLIGHT_EXTENSION).exists();
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
}
