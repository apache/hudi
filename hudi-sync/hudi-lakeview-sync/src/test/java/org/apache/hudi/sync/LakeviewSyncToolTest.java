package org.apache.hudi.sync;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.ParserConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LakeviewSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() {
    FileSystem fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  void getParserConfig() throws Exception {
    List<ParserConfig> expectedParserConfigs = new ArrayList<>();
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-1")
        .databases(Arrays.asList(Database.builder()
                .name("database-1")
                .basePaths(Arrays.asList("s3://user-bucket/lake-1/database-1/table-1",
                    "s3://user-bucket/lake-1/database-1/table-2"))
                .build(),
            Database.builder()
                .name("database-2")
                .basePaths(Arrays.asList("s3://user-bucket/lake-1/database-2/table-1",
                    "s3://user-bucket/lake-1/database-2/table-2"))
                .build()))
        .build());
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-2")
        .databases(Collections.singletonList(Database.builder()
            .name("database-1")
            .basePaths(Arrays.asList("s3://user-bucket/lake-2/database-1/table-1",
                "s3://user-bucket/lake-2/database-1/table-2"))
            .build()))
        .build());

    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/lakeview-sync-s3.properties"));
    TypedProperties typedProperties = new TypedProperties(properties);
    try (LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(typedProperties, hadoopConf)) {
      Config config = lakeviewSyncTool.getConfig();
      assertNotNull(config);
      assertEquals(new HashSet<>(expectedParserConfigs),
          new HashSet<>(config.getMetadataExtractorConfig().getParserConfig()));
    }
  }
}