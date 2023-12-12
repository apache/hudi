package org.apache.hudi.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHoodieRealtimeInputFormatUtils {
    private Configuration hadoopConf;

    @TempDir
    public java.nio.file.Path basePath;

    @BeforeEach
    public void setUp() {
        hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Test
    public void testAddProjectionField() {
        hadoopConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
        HoodieRealtimeInputFormatUtils.addProjectionField(hadoopConf, hadoopConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "").split("/"));
    }
}
