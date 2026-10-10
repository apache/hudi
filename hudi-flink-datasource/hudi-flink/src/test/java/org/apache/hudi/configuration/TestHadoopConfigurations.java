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

import org.apache.hudi.exception.HoodieValidationException;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HadoopConfigurations#getHadoopConf(Configuration)},
 * covering the {@code hadoop_conf.dir} option used for cross-cluster access.
 */
public class TestHadoopConfigurations {

    @TempDir
    File tempDir;

    // -------------------------------------------------------------------------
    // Helpers

    private File createHadoopConfDir(String... siteFileContents) throws IOException {
        File confDir = new File(tempDir, "hadoop-conf-" + System.nanoTime());
        assertTrue(confDir.mkdirs(), "Failed to create hadoop conf dir");
        for (String content : siteFileContents) {
            // content format: "filename|xml-body"
            String[] parts = content.split("\\|", 2);
            writeSiteFile(new File(confDir, parts[0]), parts[1]);
        }
        return confDir;
    }

    private void writeSiteFile(File file, String content) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
    }

    private static String siteXml(String name, String value) {
        return "<?xml version=\"1.0\"?>\n"
                + "<configuration>\n"
                + "  <property>\n"
                + "    <name>" + name + "</name>\n"
                + "    <value>" + value + "</value>\n"
                + "  </property>\n"
                + "</configuration>\n";
    }

    // -------------------------------------------------------------------------
    // hadoop_conf.dir —— 正常加载路径
    // -------------------------------------------------------------------------

    @Test
    public void testGetHadoopConfLoadsCoreSiteFromConfDir() throws Exception {
        File confDir = createHadoopConfDir(
                "core-site.xml|" + siteXml("fs.defaultFS", "hdfs://remote-cluster:8020"));

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), confDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        assertEquals("hdfs://remote-cluster:8020", hadoopConf.get("fs.defaultFS"),
                "fs.defaultFS should be loaded from the specified hadoop conf dir");
    }

    @Test
    public void testGetHadoopConfLoadsHdfsSiteFromConfDir() throws Exception {
        File confDir = createHadoopConfDir(
                "hdfs-site.xml|" + siteXml("dfs.nameservices", "remote-ns"));

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), confDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        assertEquals("remote-ns", hadoopConf.get("dfs.nameservices"),
                "hdfs-site.xml properties should be loaded from the specified conf dir");
    }

    @Test
    public void testGetHadoopConfMergesMultipleSiteFiles() throws Exception {
        File confDir = createHadoopConfDir(
                "core-site.xml|" + siteXml("fs.defaultFS", "hdfs://remote-cluster:8020"),
                "hdfs-site.xml|" + siteXml("dfs.nameservices", "remote-ns"),
                "yarn-site.xml|" + siteXml("yarn.resourcemanager.address", "remote-rm:8032"));

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), confDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        assertEquals("hdfs://remote-cluster:8020", hadoopConf.get("fs.defaultFS"));
        assertEquals("remote-ns", hadoopConf.get("dfs.nameservices"));
        assertEquals("remote-rm:8032", hadoopConf.get("yarn.resourcemanager.address"));
    }

    // -------------------------------------------------------------------------
    // hadoop.* 透传优先级
    // -------------------------------------------------------------------------

    @Test
    public void testIndividualHadoopOptionsOverrideConfDir() throws Exception {
        File confDir = createHadoopConfDir(
                "core-site.xml|" + siteXml("fs.defaultFS", "hdfs://remote-cluster:8020"));

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), confDir.getAbsolutePath());
        // Individual hadoop.* option should take the highest precedence
        conf.setString("hadoop.fs.defaultFS", "hdfs://override-cluster:8020");

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        assertEquals("hdfs://override-cluster:8020", hadoopConf.get("fs.defaultFS"),
                "hadoop.* prefixed options should override values from hadoop_conf.dir");
    }

    @Test
    public void testHadoopConfDirKeyNotLeakedIntoHadoopConf() throws Exception {
        File confDir = createHadoopConfDir(
                "core-site.xml|" + siteXml("fs.defaultFS", "hdfs://remote-cluster:8020"));

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), confDir.getAbsolutePath());

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        // 'hadoop_conf.dir' uses underscore separator and never matches the 'hadoop.'
        // prefix, so it must not be forwarded as a Hadoop property.
        assertEquals(null, hadoopConf.get("conf.dir"),
                "hadoop_conf.dir should not leak into Hadoop conf as 'conf.dir'");
        assertEquals(null, hadoopConf.get("hadoop_conf.dir"),
                "hadoop_conf.dir is a connector option, not a Hadoop property");
    }

    // -------------------------------------------------------------------------
    // hadoop_conf.dir —— 显式配置无效时 fail-fast
    // -------------------------------------------------------------------------

    @Test
    public void testGetHadoopConfWithNonExistentConfDir() {
        String nonExistentDir = new File(tempDir, "does-not-exist").getAbsolutePath();
        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), nonExistentDir);

        HoodieValidationException e = assertThrows(HoodieValidationException.class,
                () -> HadoopConfigurations.getHadoopConf(conf),
                "Explicitly configured non-existent dir should fail fast");

        assertTrue(e.getMessage().contains(nonExistentDir),
                "Error message should contain the invalid conf dir path");
        assertTrue(e.getMessage().contains(FlinkOptions.HADOOP_CONF_DIR.key()),
                "Error message should mention the option key");
    }

    @Test
    public void testGetHadoopConfWithConfDirBeingAFile() throws Exception {
        // Path exists but is a regular file, not a directory
        File file = new File(tempDir, "not-a-dir");
        assertTrue(file.createNewFile());

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), file.getAbsolutePath());

        HoodieValidationException e = assertThrows(HoodieValidationException.class,
                () -> HadoopConfigurations.getHadoopConf(conf));

        assertTrue(e.getMessage().contains(file.getAbsolutePath()));
    }

    @Test
    public void testGetHadoopConfWithConfDirMissingSiteFiles() {
        // Directory exists but contains no *-site.xml — still a config error.
        // An empty Configuration would silently fall back fs.defaultFS to local FS.
        File emptyDir = new File(tempDir, "empty-conf-dir");
        assertTrue(emptyDir.mkdirs());

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), emptyDir.getAbsolutePath());

        HoodieValidationException e = assertThrows(HoodieValidationException.class,
                () -> HadoopConfigurations.getHadoopConf(conf));

        assertTrue(e.getMessage().contains(emptyDir.getAbsolutePath()));
        assertTrue(e.getMessage().contains("core-site.xml"),
                "Error message should mention expected site files");
    }

    @Test
    public void testGetHadoopConfWithEmptyStringConfDir() {
        // Explicitly set to empty string is also a configuration error
        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.HADOOP_CONF_DIR.key(), "");

        assertThrows(HoodieValidationException.class,
                () -> HadoopConfigurations.getHadoopConf(conf));
    }

    // Unconfigured options — environment discovery fallback
    @Test
    public void testGetHadoopConfWithoutConfDirFallsBackToEnvironment() {
        // Without the option, getHadoopConf delegates to FlinkClientUtil
        // environment discovery — must not throw, and hadoop.* overrides
        // should still be applied on top.
        Configuration conf = new Configuration();
        conf.setString("hadoop.test.marker", "marker-value");

        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopConfigurations.getHadoopConf(conf);

        assertNotNull(hadoopConf);
        assertEquals("marker-value", hadoopConf.get("test.marker"),
                "hadoop.* prefixed options should be forwarded even without hadoop_conf.dir");
    }
}