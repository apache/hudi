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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.v1.ArchivedTimelineLoaderV1;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Lists a table's v6 archive files from storage, newest first.
 *
 * <p>Ordering reuses {@link ArchivedTimelineLoaderV1.ArchiveFileVersionComparator} rather than
 * restating the rule, so the two paths cannot drift apart. Sizes come straight off the listing,
 * which is why the byte budget costs no extra storage calls.
 */
public class HoodieArchiveFileCatalog implements ArchiveFileCatalog {

  private static final String ARCHIVE_FILE_GLOB = ".commits_.archive*";

  private final HoodieTableMetaClient metaClient;

  public HoodieArchiveFileCatalog(HoodieTableMetaClient metaClient) {
    this.metaClient = Objects.requireNonNull(metaClient, "metaClient");
  }

  @Override
  public List<ArchiveFile> list() throws IOException {
    List<StoragePathInfo> entries = metaClient.getStorage().globEntries(
        new StoragePath(metaClient.getArchivePath(), ARCHIVE_FILE_GLOB));
    entries.sort(new ArchivedTimelineLoaderV1.ArchiveFileVersionComparator());

    List<ArchiveFile> files = new ArrayList<>(entries.size());
    for (StoragePathInfo entry : entries) {
      files.add(new ArchiveFile(entry.getPath(), entry.getLength()));
    }
    return files;
  }
}
