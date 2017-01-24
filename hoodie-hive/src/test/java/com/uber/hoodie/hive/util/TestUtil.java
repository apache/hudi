/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive.util;

import com.google.common.collect.Sets;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.minicluster.ZookeeperTestService;
import com.uber.hoodie.hive.HoodieHiveConfiguration;
import com.uber.hoodie.hive.client.HoodieHiveClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hive.service.server.HiveServer2;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.runners.model.InitializationError;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

public class TestUtil {
    private static MiniDFSCluster dfsCluster;
    private static ZooKeeperServer zkServer;
    private static HiveServer2 hiveServer;
    public static Configuration configuration;
    public static HoodieHiveConfiguration hDroneConfiguration;
    private static DateTimeFormatter dtfOut;
    public static final String CSV_DELIMITER = "|";
    private static FileSystem fileSystem;
    private static Set<String> createdTablesSet = Sets.newHashSet();

    public static void setUp() throws IOException, InterruptedException {
        if (dfsCluster == null) {
            HdfsTestService service = new HdfsTestService();
            dfsCluster = service.start(true);
            configuration = service.getHadoopConf();
        }
        if (zkServer == null) {
            ZookeeperTestService zkService = new ZookeeperTestService(configuration);
            zkServer = zkService.start();
        }
        if (hiveServer == null) {
            HiveTestService hiveService = new HiveTestService(configuration);
            hiveServer = hiveService.start();
        }
        hDroneConfiguration =
            HoodieHiveConfiguration.newBuilder().hiveJdbcUrl("jdbc:hive2://127.0.0.1:9999/")
                .hivedb("hdrone_test").jdbcUsername("").jdbcPassword("")
                .hadoopConfiguration(hiveServer.getHiveConf()).build();
        dtfOut = DateTimeFormat.forPattern("yyyy/MM/dd");

        HoodieHiveClient client = new HoodieHiveClient(hDroneConfiguration);
        for (String tableName : createdTablesSet) {
            client.updateHiveSQL("drop table if exists " + tableName);
        }
        createdTablesSet.clear();
        client.updateHiveSQL(
            "drop database if exists " + hDroneConfiguration.getDbName());
        client.updateHiveSQL("create database " + hDroneConfiguration.getDbName());

        fileSystem = FileSystem.get(configuration);
    }

    public static void shutdown() {
        if (hiveServer != null) {
            hiveServer.stop();
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public static HoodieDatasetReference createDataset(String tableName, String hdfsPath, int numberOfPartitions,
        String schemaFile) throws IOException, InitializationError {
        Path path = new Path(hdfsPath);
        FileUtils.deleteDirectory(new File(hdfsPath));

        boolean result = fileSystem.mkdirs(path);
        checkResult(result);
        HoodieDatasetReference metadata =
            new HoodieDatasetReference(tableName, path.toString(),
                hDroneConfiguration.getDbName());
        DateTime dateTime = DateTime.now();
        createPartitions(metadata, numberOfPartitions, schemaFile, dateTime, 1);
        createdTablesSet.add(metadata.getDatabaseTableName());
        return metadata;
    }

    private static void createPartitions(HoodieDatasetReference metadata, int numberOfPartitions,
        String schemaFile, DateTime startFrom, int schemaVersion) throws IOException {
        startFrom = startFrom.withTimeAtStartOfDay();

        for (int i = 0; i < numberOfPartitions; i++) {
            Path partPath = new Path(metadata.getBaseDatasetPath() + "/" + dtfOut.print(startFrom));
            fileSystem.makeQualified(partPath);
            fileSystem.mkdirs(partPath);
            createTestData(partPath, schemaFile, schemaVersion);
            startFrom = startFrom.minusDays(1);
        }
    }

    private static void createTestData(Path partPath, String schemaFile, int schemaVersion)
        throws IOException {
        for (int i = 0; i < 5; i++) {
            // Create 5 files
            Path filePath =
                new Path(partPath.toString() + "/" + getParquetFilePath(schemaVersion, i));
            generateParquetData(filePath, schemaFile);
        }
    }

    private static String getParquetFilePath(int version, int iteration) {
        return "test.topic.name@sjc1@SV_" + version + "@" + iteration + ".parquet";
    }

    public static MessageType readSchema(String schemaFile) throws IOException {
        return MessageTypeParser
            .parseMessageType(IOUtils.toString(TestUtil.class.getResourceAsStream(schemaFile)));
    }

    public static void generateParquetData(Path filePath, String schemaFile) throws IOException {
        MessageType schema = readSchema(schemaFile);
        CsvParquetWriter writer = new CsvParquetWriter(filePath, schema);

        BufferedReader br = new BufferedReader(
            new InputStreamReader(TestUtil.class.getResourceAsStream(getDataFile(schemaFile))));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
            }
            writer.close();
        } finally {
            br.close();
        }

        InputStreamReader io = null;
        FSDataOutputStream hdfsPath = null;
        try {
            io = new FileReader(filePath.toString());
            hdfsPath = fileSystem.create(filePath);
            IOUtils.copy(io, hdfsPath);
        } finally {
            if (io != null) {
                io.close();
            }
            if (hdfsPath != null) {
                hdfsPath.close();
            }
        }
    }

    private static String getDataFile(String schemaFile) {
        return schemaFile.replaceAll(".schema", ".csv");
    }

    private static void checkResult(boolean result) throws InitializationError {
        if (!result) {
            throw new InitializationError("Could not initialize");
        }
    }

    public static void evolveDataset(HoodieDatasetReference metadata, int newPartitionCount,
        String newSchema, Long startFrom, int schemaVersion) throws IOException {
        createPartitions(metadata, newPartitionCount, newSchema,
            new DateTime(startFrom).plusDays(newPartitionCount + 1), schemaVersion);
    }
}
