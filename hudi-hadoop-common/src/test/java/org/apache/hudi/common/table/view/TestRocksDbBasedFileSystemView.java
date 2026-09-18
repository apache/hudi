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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.RocksDBDAO;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests RocksDB based file system view {@link RocksDbBasedFileSystemView}.
 */
public class TestRocksDbBasedFileSystemView extends TestHoodieTableFileSystemView {

  @Override
  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    String subdirPath = Files.createTempDirectory(tempDir, null).toAbsolutePath().toString();
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(), metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString());
    return new RocksDbBasedFileSystemView(tableMetadata, metaClient, timeline,
        FileSystemViewStorageConfig.newBuilder().withRocksDBPath(subdirPath).build());
  }

  /**
   * The driver reaches RocksDBDAO through this view, one per table, and the production symptom was
   * those DAOs and their native logger callbacks accumulating for the life of the JVM. Close a view
   * repeatedly and require that nothing it opened stays reachable.
   */
  @Test
  void testRepeatedViewLifecyclesLeaveNothingReachable() throws Exception {
    List<WeakReference<?>> refs = new ArrayList<>();
    for (int cycle = 0; cycle < 5; cycle++) {
      RocksDbBasedFileSystemView view =
          (RocksDbBasedFileSystemView) getFileSystemView(metaClient.getActiveTimeline());
      RocksDBDAO dao = (RocksDBDAO) readField(view, RocksDbBasedFileSystemView.class, "rocksDB");
      refs.add(new WeakReference<>(view));
      refs.add(new WeakReference<>(dao));
      refs.add(new WeakReference<>(nativeLoggerOf(dao)));
      view.close();
    }
    assertAllCollectable(refs);
  }

  /**
   * The logger is the object the JNI global reference actually pins, and asserting on the view or
   * the DAO alone would pass while it leaks -- the logger is a static nested class now, so it no
   * longer captures its DAO. Read reflectively rather than widening production visibility for a
   * test; the accessor is package-private in a different package from this test.
   */
  private static Object nativeLoggerOf(RocksDBDAO dao) throws ReflectiveOperationException {
    return readField(dao, RocksDBDAO.class, "logger");
  }

  private static Object readField(Object target, Class<?> declaringClass, String name)
      throws ReflectiveOperationException {
    Field field = declaringClass.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void assertAllCollectable(List<WeakReference<?>> refs) throws InterruptedException {
    for (int attempt = 0; attempt < 50; attempt++) {
      if (refs.stream().allMatch(ref -> ref.get() == null)) {
        return;
      }
      System.gc();
      Thread.sleep(50);
    }
    long alive = refs.stream().filter(ref -> ref.get() != null).count();
    fail(alive + " of " + refs.size() + " objects opened by the view are still strongly reachable "
        + "after close(); a native reference is still pinning them");
  }
}
