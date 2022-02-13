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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.AbstractSyncTool;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSyncUtilHelpers {
  private static final String BASE_PATH = "/tmp/test";
  private static final String BASE_FORMAT = "PARQUET";

  private static Configuration configuration;
  private static FileSystem fileSystem;

  @BeforeEach
  public static void setUp() throws IOException {
    configuration = new Configuration();
    fileSystem = FileSystem.get(configuration);
  }

  @Test
  public void testCreateValidSyncClass() {
    AbstractSyncTool validMetaSyncClass = SyncUtilHelpers.createMetaSyncClass(
        ValidMetaSyncClass.class.getName(),
        new TypedProperties(),
        configuration,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(validMetaSyncClass instanceof ValidMetaSyncClass);
  }

  @Test
  public void testCreateDeprecatedSyncClass() {
    // Ensure it still works for the deprecated constructor of {@link AbstractSyncTool}
    // as we implemented the fallback.
    Properties properties = new Properties();
    AbstractSyncTool deprecatedMetaSyncClass = SyncUtilHelpers.createMetaSyncClass(
        DeprecatedMetaSyncClass.class.getName(),
        new TypedProperties(properties),
        configuration,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(deprecatedMetaSyncClass instanceof DeprecatedMetaSyncClass);
  }

  @Test
  public void testCreateInvalidSyncClass() {
    Exception exception = assertThrows(HoodieException.class, () -> {
      SyncUtilHelpers.createMetaSyncClass(
          InvalidSyncClass.class.getName(),
          new TypedProperties(),
          configuration,
          fileSystem,
          BASE_PATH,
          BASE_FORMAT
      );
    });

    String expectedMessage = "Could not load meta sync class " + InvalidSyncClass.class.getName();
    assertTrue(exception.getMessage().contains(expectedMessage));

  }

  private static class ValidMetaSyncClass extends AbstractSyncTool {
    public ValidMetaSyncClass(TypedProperties props, Configuration conf, FileSystem fs) {
      super(props, conf, fs);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  private static class DeprecatedMetaSyncClass extends AbstractSyncTool {
    public DeprecatedMetaSyncClass(Properties props, FileSystem fileSystem) {
      super(props, fileSystem);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  private static class InvalidSyncClass {
    public InvalidSyncClass(Properties props) {
    }
  }
}
