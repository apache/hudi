/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSyncUtilHelpers {
  private static final String BASE_PATH = "/tmp/test";
  private static final String BASE_FORMAT = "PARQUET";

  private Configuration hadoopConf;
  private FileSystem fileSystem;

  @BeforeEach
  public void setUp() throws IOException {
    fileSystem = FSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  public void testCreateValidSyncClass() {
    HoodieSyncTool metaSyncTool = SyncUtilHelpers.instantiateMetaSyncTool(
        ValidMetaSyncClass.class.getName(),
        new TypedProperties(),
        hadoopConf,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(metaSyncTool instanceof ValidMetaSyncClass);
  }

  /**
   * Ensure it still works for the deprecated constructor of {@link HoodieSyncTool}
   * as we implemented the fallback.
   */
  @Test
  public void testCreateDeprecatedSyncClass() {
    Properties properties = new Properties();
    HoodieSyncTool deprecatedMetaSyncClass = SyncUtilHelpers.instantiateMetaSyncTool(
        DeprecatedMetaSyncClass.class.getName(),
        new TypedProperties(properties),
        hadoopConf,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(deprecatedMetaSyncClass instanceof DeprecatedMetaSyncClass);
  }

  @Test
  public void testCreateInvalidSyncClass() {
    Exception exception = assertThrows(HoodieException.class, () -> {
      SyncUtilHelpers.instantiateMetaSyncTool(
          InvalidSyncClass.class.getName(),
          new TypedProperties(),
          hadoopConf,
          fileSystem,
          BASE_PATH,
          BASE_FORMAT
      );
    });

    String expectedMessage = "Could not load meta sync class " + InvalidSyncClass.class.getName();
    assertTrue(exception.getMessage().contains(expectedMessage));

  }

  public static class ValidMetaSyncClass extends HoodieSyncTool {
    public ValidMetaSyncClass(TypedProperties props, Configuration conf, FileSystem fs) {
      super(props, conf, fs);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }

    @Override
    public void close() throws Exception {
    }
  }

  public static class DeprecatedMetaSyncClass extends HoodieSyncTool {
    public DeprecatedMetaSyncClass(Properties props, FileSystem fileSystem) {
      super(props, fileSystem);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }

    @Override
    public void close() throws Exception {
    }
  }

  public static class InvalidSyncClass {
    public InvalidSyncClass(Properties props) {
    }
  }
}
