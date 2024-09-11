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
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestSyncUtilHelpers {
  private static final String BASE_PATH = "/tmp/test";
  private static final String BASE_FORMAT = "PARQUET";

  private Configuration hadoopConf;
  private FileSystem fileSystem;

  @BeforeEach
  public void setUp() throws IOException {
    fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @ParameterizedTest
  @ValueSource(classes = {DummySyncTool1.class, DummySyncTool2.class})
  public void testCreateValidSyncClass(Class<?> clazz) {
    HoodieSyncTool syncTool = SyncUtilHelpers.instantiateMetaSyncTool(
        clazz.getName(),
        new TypedProperties(),
        hadoopConf,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(clazz.isAssignableFrom(syncTool.getClass()));
  }

  /**
   * Ensure it still works for the deprecated constructor of {@link HoodieSyncTool}
   * as we implemented the fallback.
   */
  @ParameterizedTest
  @ValueSource(classes = {DeprecatedSyncTool1.class, DeprecatedSyncTool2.class})
  public void testCreateDeprecatedSyncClass(Class<?> clazz) {
    Properties properties = new Properties();
    HoodieSyncTool syncTool = SyncUtilHelpers.instantiateMetaSyncTool(
        clazz.getName(),
        new TypedProperties(properties),
        hadoopConf,
        fileSystem,
        BASE_PATH,
        BASE_FORMAT
    );
    assertTrue(clazz.isAssignableFrom(syncTool.getClass()));
  }

  @Test
  public void testCreateInvalidSyncClass() {
    Throwable t = assertThrows(HoodieException.class, () -> {
      SyncUtilHelpers.instantiateMetaSyncTool(
          InvalidSyncTool.class.getName(),
          new TypedProperties(),
          hadoopConf,
          fileSystem,
          BASE_PATH,
          BASE_FORMAT
      );
    });

    String expectedMessage = "Could not load meta sync class " + InvalidSyncTool.class.getName()
        + ": no valid constructor found.";
    assertEquals(expectedMessage, t.getMessage());
  }

  @Test
  void testMetaSyncConcurrency() throws Exception {
    String syncToolClassName = DummySyncTool1.class.getName();
    String baseFileFormat = "PARQUET";
    String tableName1 = "table1";
    String tableName2 = "table2";
    String targetBasePath1 = "path/to/target1";
    String targetBasePath2 = "path/to/target2";

    TypedProperties props1 = new TypedProperties();
    props1.setProperty(META_SYNC_TABLE_NAME.key(), tableName1);

    TypedProperties props2 = new TypedProperties();
    props1.setProperty(META_SYNC_TABLE_NAME.key(), tableName2);

    // Simulate processing time here
    HoodieSyncTool syncToolMock = mock(HoodieSyncTool.class);
    doAnswer(invocation -> {
      Thread.sleep(1000);
      return null;
    }).when(syncToolMock).syncHoodieTable();

    AtomicBoolean targetBasePath1Running = new AtomicBoolean(false);
    AtomicBoolean targetBasePath2Running = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(4);

    // Submitting tasks with targetBasePath1
    executor.submit(() -> {
      targetBasePath1Running.set(true);
      SyncUtilHelpers.runHoodieMetaSync(syncToolClassName, props1, hadoopConf, fileSystem, targetBasePath1, baseFileFormat);
      targetBasePath1Running.set(false);
    });
    executor.submit(() -> {
      assertEquals(1, SyncUtilHelpers.getNumberOfLocks()); // Only one lock should exist for this base path
      SyncUtilHelpers.runHoodieMetaSync(syncToolClassName, props1, hadoopConf, fileSystem, targetBasePath1, baseFileFormat);
    });

    // Submitting tasks with targetBasePath2
    executor.submit(() -> {
      targetBasePath2Running.set(true);
      SyncUtilHelpers.runHoodieMetaSync(syncToolClassName, props2, hadoopConf, fileSystem, targetBasePath2, baseFileFormat);
      targetBasePath2Running.set(false);
    });
    executor.submit(() -> {
      assertEquals(2, SyncUtilHelpers.getNumberOfLocks()); // Two locks should exist for both base paths
      SyncUtilHelpers.runHoodieMetaSync(syncToolClassName, props2, hadoopConf, fileSystem, targetBasePath2, baseFileFormat);
    });

    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    // Check if there was a time when both paths were running in parallel
    assertTrue(targetBasePath1Running.get() && targetBasePath2Running.get(),
        "The methods did not run concurrently for different base paths");
  }

  public static class DummySyncTool1 extends HoodieSyncTool {
    public DummySyncTool1(Properties props, Configuration hadoopConf) {
      super(props, hadoopConf);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  public static class DummySyncTool2 extends HoodieSyncTool {
    public DummySyncTool2(Properties props) {
      super(props);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  public static class DeprecatedSyncTool1 extends HoodieSyncTool {
    public DeprecatedSyncTool1(TypedProperties props, Configuration hadoopConf, FileSystem fs) {
      super(props, hadoopConf, fs);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  public static class DeprecatedSyncTool2 extends HoodieSyncTool {
    public DeprecatedSyncTool2(Properties props, FileSystem fs) {
      super(props, fs);
    }

    @Override
    public void syncHoodieTable() {
      throw new HoodieException("Method unimplemented as its a test class");
    }
  }

  public static class InvalidSyncTool {
    public InvalidSyncTool(Properties props, FileSystem fs, Configuration hadoopConf) {
    }
  }
}
