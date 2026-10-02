/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.NonLocalSchemeLocalFileSystem;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.BroadcastReleaseTracker;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.TaskPayloadTestUtils.serializedClasses;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TestBootstrapUtils extends HoodieClientTestBase {

  @Test
  public void testAllLeafFoldersWithFiles() throws IOException {
    // All directories including marker dirs.
    List<String> folders = Arrays.asList("2016/04/15", "2016/05/16", "2016/05/17");
    folders.forEach(f -> {
      try {
        metaClient.getStorage().createDirectory(
            new StoragePath(basePath, f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Files inside partitions and marker directories
    List<String> files = Stream.of(
        "2016/04/15/1_1-0-1_20190528120000",
        "2016/04/15/2_1-0-1_20190528120000",
        "2016/05/16/3_1-0-1_20190528120000",
        "2016/05/16/4_1-0-1_20190528120000",
        "2016/04/17/5_1-0-1_20190528120000",
        "2016/04/17/6_1-0-1_20190528120000")
        .map(file -> file + metaClient.getTableConfig().getBaseFileFormat().getFileExtension())
        .collect(Collectors.toList());

    files.forEach(f -> {
      try {
        metaClient.getStorage().create(new StoragePath(basePath, f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    List<Pair<String, List<HoodieFileStatus>>> collected =
        BootstrapUtils.getAllLeafFoldersWithFiles(
            metaClient.getTableConfig().getBaseFileFormat(),
            metaClient.getStorage(),
            basePath, context);
    assertEquals(3, collected.size());
    collected.forEach(k -> assertEquals(2, k.getRight().size()));

    // Simulate reading from un-partitioned dataset
    collected =
        BootstrapUtils.getAllLeafFoldersWithFiles(
            metaClient.getTableConfig().getBaseFileFormat(),
            metaClient.getStorage(),
            basePath + "/" + folders.get(0), context);
    assertEquals(1, collected.size());
    collected.forEach(k -> assertEquals(2, k.getRight().size()));
  }

  /**
   * The source directories are listed in tasks, which must use the Hadoop configuration of the job, here the only
   * one that maps the scheme of the source path to a file system.
   */
  @Test
  void testListingUsesJobConfiguration() throws IOException {
    String scheme = "jobonly";
    StorageConfiguration<?> jobConf = context.getStorageConf().newInstance();
    jobConf.set("fs." + scheme + ".impl", JobOnlyFileSystem.class.getName());
    // no cached file system instance, as in a new executor
    jobConf.set("fs." + scheme + ".impl.disable.cache", "true");
    String sourcePath = scheme + "://bucket" + basePath + "/source";
    HoodieStorage storage = HoodieStorageUtils.getStorage(sourcePath, jobConf);
    List<String> files = Arrays.asList("2016/04/15/1_1-0-1_20190528120000.parquet", "2016/04/16/2_1-0-1_20190528120000.parquet",
        "2017/04/16/3_1-0-1_20190528120000.parquet");
    for (String file : files) {
      storage.create(new StoragePath(sourcePath, file)).close();
    }

    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    List<Pair<String, List<HoodieFileStatus>>> collected =
        BootstrapUtils.getAllLeafFoldersWithFiles(HoodieFileFormat.PARQUET, storage, sourcePath, spyContext);

    assertEquals(Arrays.asList("2016/04/15", "2016/04/16", "2017/04/16"),
        collected.stream().map(Pair::getKey).sorted().collect(Collectors.toList()));
    collected.forEach(partition -> assertEquals(1, partition.getValue().size()));
    ArgumentCaptor<SerializableFunction> captor = ArgumentCaptor.forClass(SerializableFunction.class);
    verify(spyContext).flatMap(anyList(), captor.capture(), anyInt());
    assertTrue(serializedClasses(captor.getValue()).stream().noneMatch(StorageConfiguration.class::isAssignableFrom),
        "The listing tasks share the configuration instead of each carrying a copy");
    assertEquals(1, broadcasts.created());
    broadcasts.assertAllReleased();
  }

  /**
   * The local file system under another scheme. Its file statuses carry default permissions, since the local file
   * system reads them from a {@code file} URI.
   */
  public static class JobOnlyFileSystem extends NonLocalSchemeLocalFileSystem {
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      return Arrays.stream(super.listStatus(path))
          .map(status -> new FileStatus(status.getLen(), status.isDirectory(), status.getReplication(), status.getBlockSize(),
              status.getModificationTime(), status.getPath()))
          .toArray(FileStatus[]::new);
    }
  }
}
