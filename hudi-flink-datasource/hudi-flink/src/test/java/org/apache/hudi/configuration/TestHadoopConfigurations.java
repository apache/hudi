/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link HadoopConfigurations} with the hadoop.conf.dir option.
 */
public class TestHadoopConfigurations {

    @TempDir
    File tempDir;

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes a minimal core-site.xml with the given key/value into the specified directory.
     */
    private void writeCoreSite(File confDir, String key, String value) throws IOException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<configuration>\n"
                + "  <property><name>" + key + "</name><value>" + value + "</value></property>\n"
                + "</configuration>";
        try (FileWriter w = new FileWriter(new File(confDir, "core-site.xml"))) {
            w.write(xml);
        }
    }

    /**
     * Writes a minimal hdfs-site.xml with the given key/value into the specified directory.
     */
    private void writeHdfsSite(File confDir, String key, String value) throws IOException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<configuration>\n"
                + "  <property><name>" + key + "</name><value>" + value + "</value></property>\n"
                + "</configuration>";
        try (FileWriter w = new FileWriter(new File(confDir, "hdfs-site.xml"))) {
            w.write(xml);
        }
    }

    // -------------------------------------------------------------------------
    //  Tests
    // -------------------------------------------------------------------------

    /**
     * When hadoop.conf.dir is set, getHadoopConf() should load core-site.xml
     * from that directory and expose its properties.
     */
    @Test
    void testGetHadoopConfLoadsCoreSiteFromConfDir() throws IOException {
        File hadoopConfDir = new File(tempDir, "hadoop-conf");
        hadoopConfDir.mkdirs();
        writeCoreSite(hadoopConfDir, "fs.defaultFS", "hdfs://remote-cluster:8020");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), hadoopConfDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);

        assertNotNull(hadoopConf);
        assertEquals("hdfs://remote-cluster:8020", hadoopConf.get("fs.defaultFS"),
                "fs.defaultFS should be loaded from core-site.xml in hadoop.conf.dir");
    }

    /**
     * When hadoop.conf.dir is set, getHadoopConf() should also load hdfs-site.xml.
     */
    @Test
    void testGetHadoopConfLoadsHdfsSiteFromConfDir() throws IOException {
        File hadoopConfDir = new File(tempDir, "hadoop-conf");
        hadoopConfDir.mkdirs();
        writeHdfsSite(hadoopConfDir, "dfs.replication", "3");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), hadoopConfDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);

        assertEquals("3", hadoopConf.get("dfs.replication"),
                "dfs.replication should be loaded from hdfs-site.xml in hadoop.conf.dir");
    }

    /**
     * Individual hadoop.* options in FlinkOptions should override properties
     * loaded from hadoop.conf.dir (conf dir is the base, individual options win).
     */
    @Test
    void testIndividualHadoopOptionsOverrideConfDir() throws IOException {
        File hadoopConfDir = new File(tempDir, "hadoop-conf");
        hadoopConfDir.mkdirs();
        // conf dir sets remote cluster
        writeCoreSite(hadoopConfDir, "fs.defaultFS", "hdfs://remote-cluster:8020");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), hadoopConfDir.getAbsolutePath());
        // individual option overrides the conf dir value
        conf.setString("hadoop.fs.defaultFS", "hdfs://override-cluster:9000");

        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);

        assertEquals("hdfs://override-cluster:9000", hadoopConf.get("fs.defaultFS"),
                "Individual hadoop.* options should override values from hadoop.conf.dir");
    }

    /**
     * When hadoop.conf.dir is not set, getHadoopConf() should still work normally
     * (falls back to environment-based discovery).
     */
    @Test
    void testGetHadoopConfWithoutConfDir() {
        Configuration conf = new Configuration();
        // No hadoop.conf.dir set
        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
        assertNotNull(hadoopConf, "Should return a non-null conf even without hadoop.conf.dir");
    }

    /**
     * When hadoop.conf.dir points to a non-existent directory, getHadoopConf()
     * should fall back gracefully without throwing.
     */
    @Test
    void testGetHadoopConfWithNonExistentConfDir() {
        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), "/non/existent/hadoop/conf");

        // Should not throw; falls back to default Hadoop conf
        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
        assertNotNull(hadoopConf, "Should return a non-null conf even when hadoop.conf.dir does not exist");
    }

    /**
     * Multiple XML files (core-site + hdfs-site) in hadoop.conf.dir should all be loaded,
     * and their properties should be merged.
     */
    @Test
    void testGetHadoopConfMergesMultipleSiteFiles() throws IOException {
        File hadoopConfDir = new File(tempDir, "hadoop-conf");
        hadoopConfDir.mkdirs();
        writeCoreSite(hadoopConfDir, "fs.defaultFS", "hdfs://remote-cluster:8020");
        writeHdfsSite(hadoopConfDir, "dfs.replication", "2");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), hadoopConfDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);

        assertEquals("hdfs://remote-cluster:8020", hadoopConf.get("fs.defaultFS"));
        assertEquals("2", hadoopConf.get("dfs.replication"));
    }
}